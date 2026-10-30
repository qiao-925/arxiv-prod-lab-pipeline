"""scheduler 集成测试（完全自包含，无 conftest）

使用 tests.integration.dummy_job 作为真实可执行的测试模块：
    python -m tests.integration.dummy_job           → 成功
    python -m tests.integration.dummy_job --fail    → 非零退出
    python -m tests.integration.dummy_job --sleep 5 → 超时
"""
import time

import pytest

import jobs.scheduler as sched


def test_run_job_real_subprocess_success():
    """真实启动子进程，退出码 0，锁释放。"""
    spec = sched.JobSpec(
        "t_ok", "Test OK",
        "tests.integration.dummy_job",
    )
    sched._LOCKS["t_ok"] = sched.threading.Lock()

    sched._run_job(spec, "integration")

    assert not sched._LOCKS["t_ok"].locked()


def test_run_job_real_subprocess_failure():
    """子进程 --fail 非零退出，不抛异常，锁释放。"""
    spec = sched.JobSpec(
        "t_fail", "Test Fail",
        "tests.integration.dummy_job",
        argv=("--fail",),
    )
    sched._LOCKS["t_fail"] = sched.threading.Lock()

    sched._run_job(spec, "integration")

    assert not sched._LOCKS["t_fail"].locked()


def test_run_job_real_timeout():
    """子进程睡眠 5s，timeout=1s 触发 TimeoutExpired，锁释放。"""
    spec = sched.JobSpec(
        "t_timeout", "Test Timeout",
        "tests.integration.dummy_job",
        argv=("--sleep", "5"),
        timeout=1,
    )
    sched._LOCKS["t_timeout"] = sched.threading.Lock()

    start = time.time()
    sched._run_job(spec, "integration")
    elapsed = time.time() - start

    assert elapsed < 3
    assert not sched._LOCKS["t_timeout"].locked()
