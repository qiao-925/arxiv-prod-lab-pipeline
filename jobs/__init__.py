"""jobs - 业务 Job 包（数据接入的完整实现）

模块平铺在本目录：
    config.py            配置（import 顺序敏感，保持独立）
    readers.py           数据读取（HF + Local）
    job_scan_push_kafka.py   入口 + 主流程 + PG 记账
"""
