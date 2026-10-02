"""jobs - 业务 Job 包（数据接入的完整实现）

模块平铺在本目录（不搞深层嵌套）：入口 `ingest_to_kafka.py`，其余为
被它引用的业务模块（config / ingestor / pg_client / hf_reader / local_reader）。
连接端点与 Kafka 客户端来自 common，本包只含业务语义。
"""
