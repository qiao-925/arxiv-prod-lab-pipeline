-- =============================================================================
-- 01_github_arxiv.sql（优化版）
-- =============================================================================
-- 优化点：
--   1. STEP 6 用 UNPIVOT + 单次分区排序替代 100 次独立 INSERT（52s → 15~20s）
--   2. STEP 9 paper_details / repo_details 用 CTE GROUP BY 替代 LATERAL（12s → 6~8s）
--   3. STEP 4 前置字符串前缀过滤，减少正则 + ::date 开销（7s → 5~6s）
--   4. 降噪：脚本内不输出中间结果，仅 STEP 10 输出行数摘要
--
-- 存储语义：覆盖写，保留 1 天（STEP 0 DROP → STEP 5 CREATE → STEP 6/9 填充）
-- 年份列：DO 块动态生成 year_2008 ~ year_(snap_year-1)，跨年不改代码
--
-- 执行：psql "$PGURL" -q -v ON_ERROR_STOP=1 -f 01_github_arxiv.sql
-- =============================================================================

BEGIN;

-- =============================================================================
-- STEP 0: 初始化（静默）
-- =============================================================================
SET LOCAL jit = off;
SET LOCAL work_mem = '512MB';
SET LOCAL max_parallel_workers_per_gather = 4;
SET LOCAL lock_timeout = '600s';
SET LOCAL client_min_messages = warning;

-- 防并发
SELECT pg_advisory_xact_lock(hashtext('01_github_arxiv')::bigint);

-- 清理临时表
DROP TABLE IF EXISTS
    tmp_snap, tmp_repo_full, tmp_paper_global, tmp_paper_year_wide,
    tmp_paper_d14, tmp_repo_metrics, tmp_top_arxiv, tmp_top_repo,
    tmp_timing
CASCADE;

-- 清理持久表（覆盖写，不留历史）
DROP TABLE IF EXISTS
    ranking_top500, top500_arxiv_repo, paper_details, repo_details
CASCADE;

CREATE TEMP TABLE tmp_snap ON COMMIT PRESERVE ROWS AS
SELECT CURRENT_DATE AS snap_date;

-- 时间监控（纯写表，不输出）
CREATE TEMP TABLE tmp_timing (
    seq   bigserial PRIMARY KEY,
    step  text        NOT NULL,
    ts    timestamptz NOT NULL DEFAULT clock_timestamp()
) ON COMMIT PRESERVE ROWS;

INSERT INTO tmp_timing(step) VALUES ('STEP 0 init');

-- =============================================================================
-- STEP 1: repo 级聚合 → tmp_repo_full（动态年份）
-- =============================================================================
DO $do$
DECLARE
    snap  date;
    y     int;
    ymax  int;
    ycols text := '';
BEGIN
    SELECT snap_date INTO snap FROM tmp_snap;
    ymax := EXTRACT(year FROM snap)::int - 1;

    IF ymax >= 2008 THEN
        FOR y IN 2008..ymax LOOP
            ycols := ycols || format($fmt$,
    SUM(CASE WHEN date <= %L::date THEN count END)::bigint AS year_%s$fmt$,
                format('%s-12-31', y), y);
        END LOOP;
    END IF;

    EXECUTE format($fmt$
        CREATE TEMP TABLE tmp_repo_full AS
        WITH ev AS (
            SELECT repo_id, date, count, 'star'::text AS metric FROM events_stars
            UNION ALL SELECT repo_id, date, count, 'fork'  FROM events_forks
            UNION ALL SELECT repo_id, date, count, 'issue' FROM events_issues
            UNION ALL SELECT repo_id, date, count, 'pr'    FROM events_prs
        )
        SELECT
            repo_id, metric,
            SUM(CASE WHEN date >= (SELECT snap_date FROM tmp_snap) - 6   THEN count END)::bigint AS rolling_7d,
            SUM(CASE WHEN date >= (SELECT snap_date FROM tmp_snap) - 29  THEN count END)::bigint AS rolling_30d,
            SUM(CASE WHEN date >= (SELECT snap_date FROM tmp_snap) - 89  THEN count END)::bigint AS rolling_3m,
            SUM(CASE WHEN date >= (SELECT snap_date FROM tmp_snap) - 179 THEN count END)::bigint AS rolling_6m,
            SUM(CASE WHEN date >= date_trunc('year', (SELECT snap_date FROM tmp_snap))::date
                     THEN count END)::bigint AS year_ytd,
            SUM(count)::bigint AS total%s
        FROM ev
        GROUP BY repo_id, metric
    $fmt$, ycols);

    CREATE INDEX ON tmp_repo_full (repo_id);
    ANALYZE tmp_repo_full;
END $do$;

INSERT INTO tmp_timing(step) VALUES ('STEP 1 repo agg');

-- =============================================================================
-- STEP 2: 论文级聚合（滚动 / YTD / total）→ tmp_paper_global
-- =============================================================================
CREATE TEMP TABLE tmp_paper_global AS
SELECT
    e.arxiv_id,
    COALESCE(MAX(CASE WHEN r.metric='star'  THEN r.rolling_7d END), 0)::bigint AS rolling_7d_star,
    COALESCE(MAX(CASE WHEN r.metric='fork'  THEN r.rolling_7d END), 0)::bigint AS rolling_7d_fork,
    COALESCE(MAX(CASE WHEN r.metric='issue' THEN r.rolling_7d END), 0)::bigint AS rolling_7d_issue,
    COALESCE(MAX(CASE WHEN r.metric='pr'    THEN r.rolling_7d END), 0)::bigint AS rolling_7d_pr,
    COALESCE(MAX(CASE WHEN r.metric='star'  THEN r.rolling_30d END), 0)::bigint AS rolling_30d_star,
    COALESCE(MAX(CASE WHEN r.metric='fork'  THEN r.rolling_30d END), 0)::bigint AS rolling_30d_fork,
    COALESCE(MAX(CASE WHEN r.metric='issue' THEN r.rolling_30d END), 0)::bigint AS rolling_30d_issue,
    COALESCE(MAX(CASE WHEN r.metric='pr'    THEN r.rolling_30d END), 0)::bigint AS rolling_30d_pr,
    COALESCE(MAX(CASE WHEN r.metric='star'  THEN r.rolling_3m END), 0)::bigint AS rolling_3m_star,
    COALESCE(MAX(CASE WHEN r.metric='fork'  THEN r.rolling_3m END), 0)::bigint AS rolling_3m_fork,
    COALESCE(MAX(CASE WHEN r.metric='issue' THEN r.rolling_3m END), 0)::bigint AS rolling_3m_issue,
    COALESCE(MAX(CASE WHEN r.metric='pr'    THEN r.rolling_3m END), 0)::bigint AS rolling_3m_pr,
    COALESCE(MAX(CASE WHEN r.metric='star'  THEN r.rolling_6m END), 0)::bigint AS rolling_6m_star,
    COALESCE(MAX(CASE WHEN r.metric='fork'  THEN r.rolling_6m END), 0)::bigint AS rolling_6m_fork,
    COALESCE(MAX(CASE WHEN r.metric='issue' THEN r.rolling_6m END), 0)::bigint AS rolling_6m_issue,
    COALESCE(MAX(CASE WHEN r.metric='pr'    THEN r.rolling_6m END), 0)::bigint AS rolling_6m_pr,
    COALESCE(MAX(CASE WHEN r.metric='star'  THEN r.year_ytd END), 0)::bigint AS year_ytd_star,
    COALESCE(MAX(CASE WHEN r.metric='fork'  THEN r.year_ytd END), 0)::bigint AS year_ytd_fork,
    COALESCE(MAX(CASE WHEN r.metric='issue' THEN r.year_ytd END), 0)::bigint AS year_ytd_issue,
    COALESCE(MAX(CASE WHEN r.metric='pr'    THEN r.year_ytd END), 0)::bigint AS year_ytd_pr,
    COALESCE(MAX(CASE WHEN r.metric='star'  THEN r.total END), 0)::bigint AS total_star,
    COALESCE(MAX(CASE WHEN r.metric='fork'  THEN r.total END), 0)::bigint AS total_fork,
    COALESCE(MAX(CASE WHEN r.metric='issue' THEN r.total END), 0)::bigint AS total_issue,
    COALESCE(MAX(CASE WHEN r.metric='pr'    THEN r.total END), 0)::bigint AS total_pr
FROM edges_repo_arxiv e
JOIN tmp_repo_full r ON e.repo_id = r.repo_id
GROUP BY e.arxiv_id;

ANALYZE tmp_paper_global;

INSERT INTO tmp_timing(step) VALUES ('STEP 2 paper global');

-- =============================================================================
-- STEP 3: 论文级聚合（年度宽表，动态年份）→ tmp_paper_year_wide
-- =============================================================================
DO $do$
DECLARE
    snap  date;
    y     int;
    ymax  int;
    ycols text := '';
BEGIN
    SELECT snap_date INTO snap FROM tmp_snap;
    ymax := EXTRACT(year FROM snap)::int - 1;

    IF ymax >= 2008 THEN
        FOR y IN 2008..ymax LOOP
            ycols := ycols || format($fmt$,
    COALESCE(MAX(CASE WHEN r.metric='star'  THEN r.year_%s END), 0)::bigint AS year_%s_star,
    COALESCE(MAX(CASE WHEN r.metric='fork'  THEN r.year_%s END), 0)::bigint AS year_%s_fork,
    COALESCE(MAX(CASE WHEN r.metric='issue' THEN r.year_%s END), 0)::bigint AS year_%s_issue,
    COALESCE(MAX(CASE WHEN r.metric='pr'    THEN r.year_%s END), 0)::bigint AS year_%s_pr$fmt$,
                y, y, y, y, y, y, y, y);
        END LOOP;
    END IF;

    EXECUTE format($fmt$
        CREATE TEMP TABLE tmp_paper_year_wide AS
        SELECT
            e.arxiv_id%s
        FROM edges_repo_arxiv e
        JOIN tmp_repo_full r ON e.repo_id = r.repo_id
        GROUP BY e.arxiv_id
    $fmt$, ycols);

    ANALYZE tmp_paper_year_wide;
END $do$;

INSERT INTO tmp_timing(step) VALUES ('STEP 3 paper year wide');

-- =============================================================================
-- STEP 4: 论文级聚合（近 14 天发表）→ tmp_paper_d14
-- =============================================================================
-- 优化：加字符串前缀过滤（published_date >= 'YYYY-MM-DD'），
--       先用字符串比较排除大部分行，再做严格正则 + ::date，减少 CPU 开销。
--       若 published_date 格式非 ISO（如 '2024-1-5'），字符串比较可能漏判，
--       但严格正则 + ::date 仍会兜底，不会漏真实数据。
-- =============================================================================
CREATE TEMP TABLE tmp_paper_d14 AS
WITH pe AS MATERIALIZED (
    SELECT a.arxiv_id, e.repo_id
    FROM arxiv_papers a
    JOIN edges_repo_arxiv e ON e.arxiv_id = a.arxiv_id
    WHERE a.published_date IS NOT NULL
      AND a.published_date >= to_char((SELECT snap_date FROM tmp_snap) - 13, 'YYYY-MM-DD')
      AND a.published_date ~ '^\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])'
      AND a.published_date::date >= (SELECT snap_date FROM tmp_snap) - 13
)
SELECT
    pe.arxiv_id,
    COALESCE(MAX(CASE WHEN r.metric='star'  THEN r.total END), 0)::bigint AS star_val,
    COALESCE(MAX(CASE WHEN r.metric='fork'  THEN r.total END), 0)::bigint AS fork_val,
    COALESCE(MAX(CASE WHEN r.metric='issue' THEN r.total END), 0)::bigint AS issue_val,
    COALESCE(MAX(CASE WHEN r.metric='pr'    THEN r.total END), 0)::bigint AS pr_val
FROM pe
LEFT JOIN tmp_repo_full r USING (repo_id)
GROUP BY pe.arxiv_id;

ANALYZE tmp_paper_d14;

INSERT INTO tmp_timing(step) VALUES ('STEP 4 paper d14');

-- =============================================================================
-- STEP 5: 永久表 DDL
-- =============================================================================
CREATE TABLE ranking_top500 (
    snapshot_date DATE NOT NULL,
    window_name   TEXT NOT NULL CHECK (
        window_name ~ '^(rolling_7d|rolling_30d|rolling_3m|rolling_6m|year_ytd|total|d14)$'
        OR window_name ~ '^year_\d{4}$'
    ),
    metric        TEXT NOT NULL CHECK (metric IN ('star','fork','issue','pr')),
    rank          INT  NOT NULL,
    arxiv_id      TEXT NOT NULL,
    title         TEXT,
    metric_value  BIGINT,
    PRIMARY KEY (snapshot_date, window_name, metric, rank)
);
CREATE INDEX idx_ranking_top500_arxiv  ON ranking_top500 (arxiv_id);
CREATE INDEX idx_ranking_top500_lookup ON ranking_top500 (snapshot_date, window_name, metric);

CREATE TABLE top500_arxiv_repo (
    snapshot_date DATE NOT NULL,
    arxiv_id      TEXT NOT NULL,
    repo_id       BIGINT NOT NULL,
    PRIMARY KEY (snapshot_date, arxiv_id, repo_id)
);
CREATE INDEX idx_top500_ar_arxiv ON top500_arxiv_repo (snapshot_date, arxiv_id);
CREATE INDEX idx_top500_ar_repo  ON top500_arxiv_repo (snapshot_date, repo_id);

CREATE TABLE paper_details (
    snapshot_date  DATE NOT NULL,
    arxiv_id       TEXT NOT NULL,
    title          TEXT,
    abstract       TEXT,
    authors        TEXT,
    categories     TEXT,
    published_date TEXT,
    datestamp      TEXT,
    linked_repos   JSONB,
    openalex_data  JSONB,
    repo_count     INT,
    star_max       BIGINT,
    star_sum       BIGINT,
    fork_max       BIGINT,
    fork_sum       BIGINT,
    issue_max      BIGINT,
    issue_sum      BIGINT,
    pr_max         BIGINT,
    pr_sum         BIGINT,
    PRIMARY KEY (snapshot_date, arxiv_id)
);

CREATE TABLE repo_details (
    snapshot_date  DATE NOT NULL,
    repo_id        BIGINT NOT NULL,
    full_name      TEXT,
    owner          TEXT,
    star_total     BIGINT,
    fork_total     BIGINT,
    issue_total    BIGINT,
    pr_total       BIGINT,
    linked_papers  JSONB,
    PRIMARY KEY (snapshot_date, repo_id)
);

CREATE INDEX IF NOT EXISTS idx_edges_repo_arxiv_aid ON edges_repo_arxiv (arxiv_id);
CREATE INDEX IF NOT EXISTS idx_edges_repo_arxiv_rid ON edges_repo_arxiv (repo_id);

INSERT INTO tmp_timing(step) VALUES ('STEP 5 create tables');

-- =============================================================================
-- STEP 6: 填充论文榜（UNPIVOT + 单次分区排序）
-- =============================================================================
-- 优化：100 次独立 INSERT 改为 3 次扫描 + 1 次 UNION ALL + 1 次 ROW_NUMBER。
--   - 展开阶段 WHERE metric_value > 0 立即过滤稀疏零值
--   - PARTITION BY window_name, metric 让 100 个分区各自排序
--   - 最终 WHERE rank <= 500 取 Top500
-- =============================================================================
DO $do$
DECLARE
    snap      date;
    y         int;
    ymax      int;
    year_vals text := '';
    sql       text;
BEGIN
    SELECT snap_date INTO snap FROM tmp_snap;
    ymax := EXTRACT(year FROM snap)::int - 1;

    IF ymax >= 2008 THEN
        FOR y IN 2008..ymax LOOP
            IF year_vals <> '' THEN
                year_vals := year_vals || ',';
            END IF;
            year_vals := year_vals || format($fmt$
                    ('year_%s', 'star',  p.year_%s_star),
                    ('year_%s', 'fork',  p.year_%s_fork),
                    ('year_%s', 'issue', p.year_%s_issue),
                    ('year_%s', 'pr',    p.year_%s_pr)$fmt$,
                    y, y, y, y, y, y, y, y);
        END LOOP;
    ELSE
        -- 占位：ymax < 2008 时避免 VALUES 为空导致语法错；WHERE metric_value > 0 会过滤 NULL
        year_vals := '(NULL, NULL, NULL)';
    END IF;

    sql := format($sql$
        INSERT INTO ranking_top500
            (snapshot_date, window_name, metric, rank, arxiv_id, title, metric_value)
        SELECT
            %L, window_name, metric, rank,
            arxiv_id, a.title, metric_value
        FROM (
            SELECT arxiv_id, window_name, metric, metric_value,
                   ROW_NUMBER() OVER (
                       PARTITION BY window_name, metric
                       ORDER BY metric_value DESC, arxiv_id
                   ) AS rank
            FROM (
                -- 6a: 展开 tmp_paper_global（24 列）
                SELECT p.arxiv_id, v.window_name, v.metric, v.metric_value
                FROM tmp_paper_global p
                CROSS JOIN LATERAL (VALUES
                    ('rolling_7d',  'star',  p.rolling_7d_star),
                    ('rolling_7d',  'fork',  p.rolling_7d_fork),
                    ('rolling_7d',  'issue', p.rolling_7d_issue),
                    ('rolling_7d',  'pr',    p.rolling_7d_pr),
                    ('rolling_30d', 'star',  p.rolling_30d_star),
                    ('rolling_30d', 'fork',  p.rolling_30d_fork),
                    ('rolling_30d', 'issue', p.rolling_30d_issue),
                    ('rolling_30d', 'pr',    p.rolling_30d_pr),
                    ('rolling_3m',  'star',  p.rolling_3m_star),
                    ('rolling_3m',  'fork',  p.rolling_3m_fork),
                    ('rolling_3m',  'issue', p.rolling_3m_issue),
                    ('rolling_3m',  'pr',    p.rolling_3m_pr),
                    ('rolling_6m',  'star',  p.rolling_6m_star),
                    ('rolling_6m',  'fork',  p.rolling_6m_fork),
                    ('rolling_6m',  'issue', p.rolling_6m_issue),
                    ('rolling_6m',  'pr',    p.rolling_6m_pr),
                    ('year_ytd',    'star',  p.year_ytd_star),
                    ('year_ytd',    'fork',  p.year_ytd_fork),
                    ('year_ytd',    'issue', p.year_ytd_issue),
                    ('year_ytd',    'pr',    p.year_ytd_pr),
                    ('total',       'star',  p.total_star),
                    ('total',       'fork',  p.total_fork),
                    ('total',       'issue', p.total_issue),
                    ('total',       'pr',    p.total_pr)
                ) v(window_name, metric, metric_value)
                WHERE v.metric_value > 0

                UNION ALL

                -- 6b: 展开 tmp_paper_d14（4 列）
                SELECT d.arxiv_id, v.window_name, v.metric, v.metric_value
                FROM tmp_paper_d14 d
                CROSS JOIN LATERAL (VALUES
                    ('d14', 'star',  d.star_val),
                    ('d14', 'fork',  d.fork_val),
                    ('d14', 'issue', d.issue_val),
                    ('d14', 'pr',    d.pr_val)
                ) v(window_name, metric, metric_value)
                WHERE v.metric_value > 0

                UNION ALL

                -- 6c: 展开 tmp_paper_year_wide（动态 N×4 列）
                SELECT p.arxiv_id, v.window_name, v.metric, v.metric_value
                FROM tmp_paper_year_wide p
                CROSS JOIN LATERAL (VALUES
                    %s
                ) v(window_name, metric, metric_value)
                WHERE v.metric_value > 0
            ) long
        ) ranked
        JOIN arxiv_papers a USING (arxiv_id)
        WHERE rank <= 500
    $sql$, snap, year_vals);

    EXECUTE sql;
END $do$;

INSERT INTO tmp_timing(step) VALUES ('STEP 6 fill ranking');

-- =============================================================================
-- STEP 7: 中间健康检查（静默，异常才报）
-- =============================================================================
DO $$
DECLARE
    n_repo int; n_pg int; n_py int; n_d14 int; n_rank int;
BEGIN
    SELECT COUNT(*) INTO n_repo FROM tmp_repo_full;
    SELECT COUNT(*) INTO n_pg   FROM tmp_paper_global;
    SELECT COUNT(*) INTO n_py   FROM tmp_paper_year_wide;
    SELECT COUNT(*) INTO n_d14  FROM tmp_paper_d14;
    SELECT COUNT(*) INTO n_rank FROM ranking_top500;
    IF n_repo = 0 OR n_pg = 0 OR n_rank = 0 THEN
        RAISE EXCEPTION '中间表异常: repo_full=%, paper_global=%, paper_year=%, paper_d14=%, ranking=%',
            n_repo, n_pg, n_py, n_d14, n_rank;
    END IF;
END $$;

INSERT INTO tmp_timing(step) VALUES ('STEP 7 mid verify');

-- =============================================================================
-- STEP 9: 填充桥接 + detail（CTE GROUP BY 替代 LATERAL）
-- =============================================================================
CREATE TEMP TABLE tmp_repo_metrics AS
SELECT
    repo_id,
    MAX(CASE WHEN metric='star'  THEN total END)::bigint AS star_total,
    MAX(CASE WHEN metric='fork'  THEN total END)::bigint AS fork_total,
    MAX(CASE WHEN metric='issue' THEN total END)::bigint AS issue_total,
    MAX(CASE WHEN metric='pr'    THEN total END)::bigint AS pr_total
FROM tmp_repo_full
GROUP BY repo_id;
CREATE INDEX ON tmp_repo_metrics (repo_id);
ANALYZE tmp_repo_metrics;

INSERT INTO top500_arxiv_repo (snapshot_date, arxiv_id, repo_id)
SELECT DISTINCT
    (SELECT snap_date FROM tmp_snap),
    e.arxiv_id,
    e.repo_id
FROM edges_repo_arxiv e
JOIN (
    SELECT DISTINCT arxiv_id FROM ranking_top500
    WHERE snapshot_date = (SELECT snap_date FROM tmp_snap)
) t USING (arxiv_id);

CREATE TEMP TABLE tmp_top_arxiv AS
SELECT DISTINCT arxiv_id FROM ranking_top500
WHERE snapshot_date = (SELECT snap_date FROM tmp_snap);
CREATE INDEX ON tmp_top_arxiv (arxiv_id);

CREATE TEMP TABLE tmp_top_repo AS
SELECT DISTINCT repo_id FROM top500_arxiv_repo
WHERE snapshot_date = (SELECT snap_date FROM tmp_snap);
CREATE INDEX ON tmp_top_repo (repo_id);

-- 9.3 paper_details：CTE GROUP BY 一次聚合（替代 LATERAL N 次子查询）
INSERT INTO paper_details (
    snapshot_date, arxiv_id, title, abstract, authors, categories,
    published_date, datestamp, linked_repos, openalex_data,
    repo_count, star_max, star_sum, fork_max, fork_sum,
    issue_max, issue_sum, pr_max, pr_sum
)
WITH agg AS (
    SELECT
        tar.arxiv_id,
        COUNT(*)::int                              AS repo_count,
        MAX(COALESCE(m.star_total, 0))::bigint     AS star_max,
        SUM(COALESCE(m.star_total, 0))::bigint     AS star_sum,
        MAX(COALESCE(m.fork_total, 0))::bigint     AS fork_max,
        SUM(COALESCE(m.fork_total, 0))::bigint     AS fork_sum,
        MAX(COALESCE(m.issue_total, 0))::bigint    AS issue_max,
        SUM(COALESCE(m.issue_total, 0))::bigint    AS issue_sum,
        MAX(COALESCE(m.pr_total, 0))::bigint       AS pr_max,
        SUM(COALESCE(m.pr_total, 0))::bigint       AS pr_sum,
        jsonb_agg(
            jsonb_build_object(
                'repo_id',     tar.repo_id,
                'full_name',   fr.full_name,
                'owner',       fr.owner,
                'star_total',  COALESCE(m.star_total, 0),
                'fork_total',  COALESCE(m.fork_total, 0),
                'issue_total', COALESCE(m.issue_total, 0),
                'pr_total',    COALESCE(m.pr_total, 0)
            )
            ORDER BY COALESCE(m.star_total, 0) DESC, tar.repo_id
        ) AS linked_repos
    FROM top500_arxiv_repo tar
    LEFT JOIN tmp_repo_metrics m ON m.repo_id = tar.repo_id
    LEFT JOIN features_repo fr   ON fr.repo_id = tar.repo_id
    WHERE tar.snapshot_date = (SELECT snap_date FROM tmp_snap)
    GROUP BY tar.arxiv_id
)
SELECT
    (SELECT snap_date FROM tmp_snap),
    t.arxiv_id,
    a.title, a.abstract, a.authors, a.categories, a.published_date, a.datestamp,
    COALESCE(agg.linked_repos, '[]'::jsonb),
    NULL::jsonb,
    COALESCE(agg.repo_count, 0),
    COALESCE(agg.star_max, 0),  COALESCE(agg.star_sum, 0),
    COALESCE(agg.fork_max, 0),  COALESCE(agg.fork_sum, 0),
    COALESCE(agg.issue_max, 0), COALESCE(agg.issue_sum, 0),
    COALESCE(agg.pr_max, 0),    COALESCE(agg.pr_sum, 0)
FROM tmp_top_arxiv t
LEFT JOIN arxiv_papers a USING (arxiv_id)
LEFT JOIN agg USING (arxiv_id);

-- 9.4 repo_details：CTE GROUP BY 一次聚合（替代 LATERAL N 次子查询）
INSERT INTO repo_details (
    snapshot_date, repo_id, full_name, owner,
    star_total, fork_total, issue_total, pr_total, linked_papers
)
WITH lp AS (
    SELECT
        tar.repo_id,
        jsonb_agg(
            jsonb_build_object(
                'arxiv_id', tar.arxiv_id,
                'title',    a.title
            )
            ORDER BY a.title NULLS LAST, tar.arxiv_id
        ) AS linked_papers
    FROM top500_arxiv_repo tar
    LEFT JOIN arxiv_papers a ON a.arxiv_id = tar.arxiv_id
    WHERE tar.snapshot_date = (SELECT snap_date FROM tmp_snap)
    GROUP BY tar.repo_id
)
SELECT
    (SELECT snap_date FROM tmp_snap),
    tr.repo_id,
    fr.full_name,
    fr.owner,
    COALESCE(m.star_total, 0),
    COALESCE(m.fork_total, 0),
    COALESCE(m.issue_total, 0),
    COALESCE(m.pr_total, 0),
    COALESCE(lp.linked_papers, '[]'::jsonb)
FROM tmp_top_repo tr
LEFT JOIN tmp_repo_metrics m ON m.repo_id = tr.repo_id
LEFT JOIN features_repo fr   ON fr.repo_id = tr.repo_id
LEFT JOIN lp                 ON lp.repo_id = tr.repo_id;

INSERT INTO tmp_timing(step) VALUES ('STEP 9 fill details');

-- =============================================================================
-- STEP 10: 最终输出（仅 1 个结果集）
-- =============================================================================
INSERT INTO tmp_timing(step) VALUES ('STEP 10 done');

-- 结果集：四张表行数摘要
SELECT 'ranking_top500'    AS table_name, COUNT(*) AS rows FROM ranking_top500
UNION ALL SELECT 'top500_arxiv_repo', COUNT(*) FROM top500_arxiv_repo
UNION ALL SELECT 'paper_details',     COUNT(*) FROM paper_details
UNION ALL SELECT 'repo_details',      COUNT(*) FROM repo_details;

COMMIT;
