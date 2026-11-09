-- =============================================================================
-- 02_openalex_arxiv.sql（优化版：先排名后拼详情）
-- =============================================================================
-- 核心优化：
--   - 轻量候选集 tmp_openalex_candidates_light 只含 (arxiv_id, cited_by_count,
--     paper_date, title) 4 个字段，不含 abstract/authors/categories 等大字段
--   - 先按窗口 LATERAL LIMIT 500 得到 Top500 并集 M
--   - 再只为 M 篇 JOIN arxiv_papers 拿大字段
--   - 大字段物化范围从全量 N 降到并集 M（N ≫ M 时收益显著）
--
-- 口径（与原版一致）：
--   - 论文日期统一使用 arxiv_papers.published_date（v1 发布时间）
--   - recent_Nm 为 (as_of - N months, as_of] 的发表时间 cohort
--   - year_ytd 为更新日所在自然年的 YTD
--   - year_YYYY 为完整历史年度，total 为截至更新日的全部论文
--   - cited_by_count 是 OpenAlex 数据版本中的累计引用，不是窗口内新增引用
--
-- 幂等：同一 as_of_date 重跑只替换该版本，其他版本保留
-- =============================================================================

BEGIN;

SET LOCAL jit = off;
SET LOCAL work_mem = '256MB';
SET LOCAL max_parallel_workers_per_gather = 4;
SET LOCAL lock_timeout = '600s';
SET LOCAL client_min_messages = warning;

SELECT pg_advisory_xact_lock(hashtext('openalex_rank_sql')::bigint);

DROP TABLE IF EXISTS
    tmp_openalex_task, tmp_openalex_works_dedup, tmp_openalex_paper_light,
    tmp_openalex_candidates_light, tmp_openalex_windows,
    tmp_openalex_top500, tmp_openalex_top_ids
CASCADE;

-- =============================================================================
-- STEP 1: 任务日期
-- =============================================================================
CREATE TEMP TABLE tmp_openalex_task ON COMMIT DROP AS
SELECT as_of_date
FROM openalex_rank_task
ORDER BY as_of_date DESC
LIMIT 1;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM tmp_openalex_task) THEN
        RAISE EXCEPTION 'openalex_rank_task 中没有可用日期';
    END IF;
END $$;

-- =============================================================================
-- STEP 2: openalex_works 去重（同 arxiv_id 多 work 取引用最高）
-- =============================================================================
-- 保留 works 的窄字段（work_id / 引用 / 年份 / 主题等），供 STEP 9 拼详情用。
-- 注意：openalex_publication_date 与 referenced_works_count 显式转型，
--       若源表格式异常会整批失败（业务假设已在源侧清洗）。
CREATE TEMP TABLE tmp_openalex_works_dedup ON COMMIT DROP AS
WITH ranked AS (
    SELECT
        o.work_id,
        o.arxiv_id,
        o.cited_by_count::bigint        AS cited_by_count,
        o.referenced_works_count::int   AS referenced_works_count,
        o.publication_date::date        AS openalex_publication_date,
        o.publication_year,
        o.is_oa,
        o.oa_status,
        o.type,
        o.language,
        o.authors_count,
        o.primary_topic_name,
        o.primary_topic_field,
        o.primary_topic_domain,
        ROW_NUMBER() OVER (
            PARTITION BY o.arxiv_id
            ORDER BY o.cited_by_count DESC, o.work_id
        ) AS rn
    FROM openalex_works o
    WHERE o.arxiv_id IS NOT NULL
      AND btrim(o.arxiv_id) <> ''
      AND o.cited_by_count > 0
      AND o.is_retracted IS NOT TRUE
      AND o.is_paratext  IS NOT TRUE
)
SELECT
    arxiv_id, work_id, cited_by_count, referenced_works_count,
    openalex_publication_date, publication_year,
    is_oa, oa_status, type, language, authors_count,
    primary_topic_name, primary_topic_field, primary_topic_domain
FROM ranked
WHERE rn = 1;

CREATE UNIQUE INDEX ON tmp_openalex_works_dedup (arxiv_id);
ANALYZE tmp_openalex_works_dedup;

-- =============================================================================
-- STEP 3: arxiv_papers 去重（取 v1），只保留窄字段 + title
-- =============================================================================
-- 大字段（abstract / authors / categories / doi / datestamp）不在此物化，
-- 留给 STEP 9 只为并集 M 物化。
-- 注意：pg_input_is_valid 需要 PostgreSQL 16+。
CREATE TEMP TABLE tmp_openalex_paper_light ON COMMIT DROP AS
WITH raw AS (
    SELECT
        a.arxiv_id,
        a.title,
        a.published_date,
        CASE
            WHEN a.published_date IS NOT NULL
             AND left(a.published_date, 10) ~
                 '^[0-9]{4}-(0[1-9]|1[0-2])-(0[1-9]|[12][0-9]|3[01])$'
             AND pg_input_is_valid(left(a.published_date, 10), 'date')
            THEN left(a.published_date, 10)::date
            ELSE NULL
        END AS paper_date
    FROM arxiv_papers a
    WHERE a.published_date IS NOT NULL
)
SELECT DISTINCT ON (arxiv_id)
    arxiv_id, title, published_date, paper_date
FROM raw
ORDER BY arxiv_id, published_date ASC;   -- v1 = 最早发布时间

CREATE UNIQUE INDEX ON tmp_openalex_paper_light (arxiv_id);
ANALYZE tmp_openalex_paper_light;

-- =============================================================================
-- STEP 4: 轻量候选集（4 个字段）
-- =============================================================================
CREATE TEMP TABLE tmp_openalex_candidates_light ON COMMIT DROP AS
SELECT
    w.arxiv_id,
    w.cited_by_count,
    p.paper_date,
    p.title
FROM tmp_openalex_works_dedup w
JOIN tmp_openalex_paper_light  p USING (arxiv_id)
CROSS JOIN tmp_openalex_task   t
WHERE p.paper_date IS NOT NULL
  AND p.paper_date <= t.as_of_date;

CREATE INDEX ON tmp_openalex_candidates_light
    (paper_date, cited_by_count DESC, arxiv_id);
ANALYZE tmp_openalex_candidates_light;

-- =============================================================================
-- STEP 5: 窗口表（fixed + years + total）
-- =============================================================================
CREATE TEMP TABLE tmp_openalex_windows ON COMMIT DROP AS
WITH meta AS (
    SELECT
        t.as_of_date,
        (SELECT MIN(paper_date) FROM tmp_openalex_candidates_light) AS min_paper_date
    FROM tmp_openalex_task t
),
fixed AS (
    SELECT as_of_date, window_name, start_date, end_date
    FROM meta
    CROSS JOIN LATERAL (VALUES
        ('recent_3m',
         (as_of_date - INTERVAL '3 months'  + INTERVAL '1 day')::date, as_of_date),
        ('recent_6m',
         (as_of_date - INTERVAL '6 months'  + INTERVAL '1 day')::date, as_of_date),
        ('recent_12m',
         (as_of_date - INTERVAL '12 months' + INTERVAL '1 day')::date, as_of_date),
        ('year_ytd',
         make_date(EXTRACT(YEAR FROM as_of_date)::int, 1, 1),         as_of_date)
    ) v(window_name, start_date, end_date)
),
years AS (
    SELECT
        m.as_of_date,
        format('year_%s', y) AS window_name,
        make_date(y, 1, 1)   AS start_date,
        make_date(y, 12, 31) AS end_date
    FROM meta m
    CROSS JOIN LATERAL generate_series(
        EXTRACT(YEAR FROM m.min_paper_date)::int,
        EXTRACT(YEAR FROM m.as_of_date)::int - 1
    ) AS y
    WHERE m.min_paper_date IS NOT NULL
),
total AS (
    SELECT as_of_date, 'total' AS window_name,
           min_paper_date AS start_date, as_of_date AS end_date
    FROM meta
    WHERE min_paper_date IS NOT NULL
)
SELECT * FROM fixed
UNION ALL SELECT * FROM years
UNION ALL SELECT * FROM total;

-- =============================================================================
-- STEP 6: 持久表 DDL（IF NOT EXISTS，多版本保留）
-- =============================================================================
CREATE TABLE IF NOT EXISTS openalex_ranking_top500 (
    as_of_date    DATE NOT NULL,
    window_name   TEXT NOT NULL,
    rank          INT  NOT NULL CHECK (rank BETWEEN 1 AND 500),
    arxiv_id      TEXT NOT NULL,
    title         TEXT,
    cited_by_count BIGINT NOT NULL,
    PRIMARY KEY (as_of_date, window_name, rank)
);

CREATE INDEX IF NOT EXISTS idx_openalex_ranking_arxiv
    ON openalex_ranking_top500 (as_of_date, arxiv_id);
CREATE INDEX IF NOT EXISTS idx_openalex_ranking_window
    ON openalex_ranking_top500 (as_of_date, window_name, rank);

CREATE TABLE IF NOT EXISTS paper_openalex_details (
    as_of_date                 DATE NOT NULL,
    arxiv_id                   TEXT NOT NULL,
    work_id                    TEXT,
    title                      TEXT,
    abstract                   TEXT,
    authors                    TEXT,
    categories                 TEXT,
    published_date             TEXT,
    doi                        TEXT,
    datestamp                  TEXT,
    cited_by_count             BIGINT,
    referenced_works_count     INT,
    openalex_publication_date  DATE,
    publication_year           INT,
    type                       TEXT,
    language                   TEXT,
    is_oa                      BOOLEAN,
    oa_status                  TEXT,
    authors_count              INT,
    primary_topic_name         TEXT,
    primary_topic_field        TEXT,
    primary_topic_domain       TEXT,
    PRIMARY KEY (as_of_date, arxiv_id)
);

CREATE INDEX IF NOT EXISTS idx_paper_openalex_details_arxiv
    ON paper_openalex_details (arxiv_id);

-- =============================================================================
-- STEP 7: 幂等 DELETE（只删当前 as_of_date）
-- =============================================================================
DELETE FROM openalex_ranking_top500
WHERE as_of_date = (SELECT as_of_date FROM tmp_openalex_task);
DELETE FROM paper_openalex_details
WHERE as_of_date = (SELECT as_of_date FROM tmp_openalex_task);

-- =============================================================================
-- STEP 8: 每窗口 Top500（LATERAL + LIMIT 500）
-- =============================================================================
CREATE TEMP TABLE tmp_openalex_top500 ON COMMIT DROP AS
SELECT
    w.as_of_date,
    w.window_name,
    ROW_NUMBER() OVER (
        PARTITION BY w.window_name
        ORDER BY c.cited_by_count DESC, c.arxiv_id
    )::int AS rank,
    c.arxiv_id,
    c.title,
    c.cited_by_count
FROM tmp_openalex_windows w
CROSS JOIN LATERAL (
    SELECT c.arxiv_id, c.title, c.cited_by_count
    FROM tmp_openalex_candidates_light c
    WHERE c.paper_date BETWEEN w.start_date AND w.end_date
    ORDER BY c.cited_by_count DESC, c.arxiv_id
    LIMIT 500
) c;

INSERT INTO openalex_ranking_top500 (
    as_of_date, window_name, rank, arxiv_id, title, cited_by_count
)
SELECT as_of_date, window_name, rank, arxiv_id, title, cited_by_count
FROM tmp_openalex_top500;

-- =============================================================================
-- STEP 9: 只为并集 M 拼详情
-- =============================================================================
CREATE TEMP TABLE tmp_openalex_top_ids ON COMMIT DROP AS
SELECT DISTINCT arxiv_id FROM tmp_openalex_top500;
CREATE UNIQUE INDEX ON tmp_openalex_top_ids (arxiv_id);

INSERT INTO paper_openalex_details (
    as_of_date, arxiv_id, work_id, title, abstract, authors, categories,
    published_date, doi, datestamp, cited_by_count, referenced_works_count,
    openalex_publication_date, publication_year, type, language, is_oa,
    oa_status, authors_count, primary_topic_name, primary_topic_field,
    primary_topic_domain
)
SELECT
    t.as_of_date,
    ti.arxiv_id,
    w.work_id,
    p.title,
    p.abstract,
    p.authors,
    p.categories,
    p.published_date,
    p.doi,
    p.datestamp,
    w.cited_by_count,
    w.referenced_works_count,
    w.openalex_publication_date,
    w.publication_year,
    w.type,
    w.language,
    w.is_oa,
    w.oa_status,
    w.authors_count,
    w.primary_topic_name,
    w.primary_topic_field,
    w.primary_topic_domain
FROM tmp_openalex_top_ids ti
CROSS JOIN tmp_openalex_task t
JOIN tmp_openalex_works_dedup w
  ON w.arxiv_id = ti.arxiv_id
JOIN (
    -- arxiv_papers 去重取 v1，只为并集 M 取大字段
    SELECT DISTINCT ON (a.arxiv_id)
        a.arxiv_id, a.title, a.abstract, a.authors, a.categories,
        a.published_date, a.doi, a.datestamp
    FROM arxiv_papers a
    JOIN tmp_openalex_top_ids ti2 ON ti2.arxiv_id = a.arxiv_id
    ORDER BY a.arxiv_id, a.published_date ASC
) p
  ON p.arxiv_id = ti.arxiv_id;

-- =============================================================================
-- STEP 10: 校验 + 输出
-- =============================================================================
DO $$
DECLARE
    n_rank        bigint;
    n_details     bigint;
    n_window_bad  int;
BEGIN
    SELECT COUNT(*) INTO n_rank
    FROM openalex_ranking_top500
    WHERE as_of_date = (SELECT as_of_date FROM tmp_openalex_task);

    SELECT COUNT(*) INTO n_details
    FROM paper_openalex_details
    WHERE as_of_date = (SELECT as_of_date FROM tmp_openalex_task);

    IF n_rank = 0 OR n_details = 0 THEN
        RAISE EXCEPTION 'OpenAlex 排名生成异常: ranking=%, details=%',
            n_rank, n_details;
    END IF;

    -- 每个窗口 rank 必须从 1 连续到 MAX(rank)
    SELECT COUNT(*) INTO n_window_bad
    FROM (
        SELECT window_name
        FROM openalex_ranking_top500
        WHERE as_of_date = (SELECT as_of_date FROM tmp_openalex_task)
        GROUP BY window_name
        HAVING MAX(rank) <> COUNT(*)
    ) x;

    IF n_window_bad > 0 THEN
        RAISE EXCEPTION 'rank 不连续: % 个窗口 MAX(rank) <> COUNT(*)',
            n_window_bad;
    END IF;
END $$;

SELECT 'openalex_ranking_top500' AS table_name, COUNT(*) AS rows
FROM openalex_ranking_top500
WHERE as_of_date = (SELECT as_of_date FROM tmp_openalex_task)
UNION ALL
SELECT 'paper_openalex_details', COUNT(*)
FROM paper_openalex_details
WHERE as_of_date = (SELECT as_of_date FROM tmp_openalex_task);

COMMIT;