"""统一日志配置（无业务语义，各业务包共享）"""
import logging


def setup_logger(name: str = "arxiv") -> logging.Logger:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    return logging.getLogger(name)
