"""scheduler 测试套件

对齐新调度 API：
    JobSpec / JOB_SPECS 配置
    _next_hour / _register_jobs 时间逻辑
    _run_job 互斥与子进程调用
    main ENV 校验

设计原则：只用 Mock，不真起子进程、不真起 APScheduler。
"""
import subprocess
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch
from zoneinfo import ZoneInfo

import pytest

import jobs.scheduler as sched


TZ = ZoneInfo("Asia/Shanghai")


# ═══════════════════════════════════════════════════════════════════════
# 1. 配置：JOB_SPECS 的契约
# ═══════════════════════════════════════════════════════════════════════

def test_job_specs_contains_merged_jobs():
    """确认旧的 discover/readme 已被合并掉，不再出现。"""
    ids = {spec.id for spec in sched.JOB_SPECS}
    assert ids == {"oai_pull", "github_crawl", "github_sync"}
    assert "github_discover" not in ids
    assert "github_readme_extract" not in ids


def test_job_specs_modules_match_files():
    """每个 JobSpec 的 module 必须指向 jobs.*，且 id 与模块名对应。"""
    for spec in sched.JOB_SPECS:
        assert spec.module.startswith("jobs.")
        if spec.id == "github_crawl":
            assert "github_crawl" in spec.module
        if spec.id == "github_sync":
            assert "github_sync" in spec.module


def test_job_specs_argv_for_long_running_jobs():
    """crawl 和 sync 需要 argv=('run',)，因为它们依赖子命令。"""
    by_id = {spec.id: spec for spec in sched.JOB_SPECS}
    assert by_id["github_crawl"].argv == ("run",)
    assert by_id["github_sync"].argv == ("run",)


def test_locks_initialized_for_enabled_jobs():
    """每个启用的 job 都必须有一把锁，防止多线程并发。"""
    expected = {spec.id for spec in sched.JOB_SPECS if spec.enabled}
    assert set(sched._LOCKS.keys()) == expected


# ═══════════════════════════════════════════════════════════════════════
# 2. 时间逻辑：_next_hour 与 _register_jobs 错开
# ═══════════════════════════════════════════════════════════════════════

def test_next_hour_truncates_and_advances():
    dt = datetime(2026, 10, 30, 14, 37, 42, tzinfo=TZ)
    assert sched._next_hour(dt) == datetime(2026, 10, 30, 15, 0, 0, tzinfo=TZ)


def test_next_hour_at_boundary():
    """整点时刻仍应推进到下一个整点。"""
    dt = datetime(2026, 10, 30, 14, 0, 0, tzinfo=TZ)
    assert sched._next_hour(dt) == datetime(2026, 10, 30, 15, 0, 0, tzinfo=TZ)


def test_register_jobs_staggered_startup():
    """启动任务按 STARTUP_STAGGER_SECONDS 逐个错开。"""
    mock_scheduler = MagicMock()
    now = datetime(2026, 10, 30, 14, 0, 0, tzinfo=TZ)
    sched._register_jobs(mock_scheduler, now)

    startup_calls = [
        c for c in mock_scheduler.add_job.call_args_list
        if c.kwargs.get("id", "").endswith("@startup")
    ]
    enabled_count = len([s for s in sched.JOB_SPECS if s.enabled])
    assert len(startup_calls) == enabled_count

    for i, call in enumerate(startup_calls):
        run_date = call.kwargs["run_date"]
        expected = now + timedelta(seconds=i * sched.STARTUP_STAGGER_SECONDS)
        assert run_date == expected


def test_register_jobs_hourly_after_startup():
    """整点轮巡起点必须晚于最后一个启动任务完成。"""
    mock_scheduler = MagicMock()
    now = datetime(2026, 10, 30, 14, 0, 0, tzinfo=TZ)
    sched._register_jobs(mock_scheduler, now)

    specs = [s for s in sched.JOB_SPECS if s.enabled]
    startup_window = (len(specs) - 1) * sched.STARTUP_STAGGER_SECONDS
    expected_first_hour = sched._next_hour(
        now + timedelta(seconds=startup_window + 60)
    )

    hourly_calls = [
        c for c in mock_scheduler.add_job.call_args_list
        if c.kwargs.get("id", "").endswith("@hourly")
    ]
    assert len(hourly_calls) == len(specs)
    for call in hourly_calls:
        trigger = call.kwargs["trigger"]
        assert trigger.start_date == expected_first_hour


# ═══════════════════════════════════════════════════════════════════════
# 3. 执行互斥与子进程调用
# ═══════════════════════════════════════════════════════════════════════

def test_run_job_skips_when_lock_held():
    """锁被占用时直接跳过，不启动子进程。"""
    spec = sched.JobSpec("t1", "test", "jobs.t1")
    sched._LOCKS["t1"] = sched.threading.Lock()
    sched._LOCKS["t1"].acquire()

    # 直接 patch 全局 subprocess.run，避免解析 scheduler.subprocess 时的模块名冲突
    with patch("subprocess.run") as mock_run:
        try:
            sched._run_job(spec, "test")
        finally:
            sched._LOCKS["t1"].release()

    mock_run.assert_not_called()


@patch("subprocess.run")
def test_run_job_success(mock_run):
    """成功执行时返回码 0，锁正常释放。"""
    mock_run.return_value = MagicMock(returncode=0)
    spec = sched.JobSpec("t2", "test", "jobs.t2", argv=("run",))
    sched._LOCKS["t2"] = sched.threading.Lock()

    sched._run_job(spec, "startup")

    mock_run.assert_called_once()
    cmd = mock_run.call_args[0][0]
    assert cmd == [sched.sys.executable, "-m", "jobs.t2", "run"]
    assert mock_run.call_args.kwargs["timeout"] == spec.timeout
    assert not sched._LOCKS["t2"].locked()


@patch("subprocess.run")
def test_run_job_nonzero_exit(mock_run):
    """非零退出码不抛异常（check=False），锁仍释放。"""
    mock_run.return_value = MagicMock(returncode=1)
    spec = sched.JobSpec("t3", "test", "jobs.t3")
    sched._LOCKS["t3"] = sched.threading.Lock()

    sched._run_job(spec, "hourly")
    assert not sched._LOCKS["t3"].locked()


@patch("subprocess.run")
def test_run_job_timeout_releases_lock(mock_run):
    """超时异常被捕获，锁必须释放，下一轮还能继续。"""
    mock_run.side_effect = subprocess.TimeoutExpired(cmd="x", timeout=1)
    spec = sched.JobSpec("t4", "test", "jobs.t4")
    sched._LOCKS["t4"] = sched.threading.Lock()

    sched._run_job(spec, "hourly")

    assert not sched._LOCKS["t4"].locked()
    assert sched._LOCKS["t4"].acquire(blocking=False) is True
    sched._LOCKS["t4"].release()


# ═══════════════════════════════════════════════════════════════════════
# 4. main ENV 校验
# ═══════════════════════════════════════════════════════════════════════

def test_main_rejects_invalid_env(monkeypatch):
    monkeypatch.setenv("ENV", "dev")
    with pytest.raises(RuntimeError, match="ENV 必须是"):
        sched.main()


def test_main_rejects_missing_env(monkeypatch):
    monkeypatch.delenv("ENV", raising=False)
    with pytest.raises(RuntimeError, match="ENV 必须是"):
        sched.main()
