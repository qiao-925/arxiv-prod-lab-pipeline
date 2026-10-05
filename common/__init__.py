"""common - 跨业务包共享的基础设施（连接配置 + Kafka 客户端 + 统一日志）

定位：只放「无业务语义」的通用件——连接端点、资源名派生、环境声明、日志、
Kafka Producer 封装。业务包（utils / jobs 及未来的 Spark/Flink 落地）从这里取
「连哪个环境、哪套资源」，避免各写各的漂移。

    common.config  连接配置（端点 + 资源名派生 + init_env 环境声明）
    common.kafka   Kafka Producer 封装（端点取自 config）
    common.logger  统一日志 setup_logger
"""
