"""主流程编排"""
import json
import signal
import sys
import time
import uuid
from datetime import datetime

from common.kafka import KafkaClient
from common.logger import setup_logger

from .config import Config
from .pg_client import PGClient

if Config.DATA_SOURCE == "local":
    from .local_reader import LocalReader as Reader
else:
    from .hf_reader import HFReader as Reader


class Ingestor:
    def __init__(self):
        self.log = setup_logger()
        self._running = True
        signal.signal(signal.SIGTERM, self._handle_sigterm)

        errors = Config.validate()
        if errors:
            for e in errors:
                self.log.error(f"配置错误: {e}")
            sys.exit(1)

        self.pg = PGClient(self.log)
        self.kafka = KafkaClient(self.log)
        self.reader = Reader(self.log)

    def _handle_sigterm(self, signum, frame):
        self.log.warning("收到 SIGTERM，准备优雅退出...")
        self._running = False

    def _print_env_banner(self):
        """打印当前环境解析出的资源名，防止误连（如 prod 连了 dev 库）。

        默认非阻塞；设置 CONFIRM_STARTUP=1 时要求人工确认（避免调度器任务卡住）。
        """
        self.log.info("=" * 60)
        self.log.info(f"⚠️  当前环境:    {Config.ENV}")
        self.log.info(f"⚠️  Kafka Topic: {Config.KAFKA_TOPIC}")
        self.log.info(f"⚠️  PG Database: {Config.PG_DB}")
        self.log.info(f"⚠️  MinIO:       {Config.MINIO_BUCKET}/{Config.MINIO_PREFIX}")
        self.log.info(f"⚠️  Job Name:    {Config.JOB_NAME}")
        self.log.info("=" * 60)
        if Config.CONFIRM_STARTUP:
            answer = input("确认继续？(y/N): ").strip().lower()
            if answer not in ("y", "yes"):
                self.log.warning("用户取消，退出")
                sys.exit(0)

    def run(self):
        start_time = time.time()
        self.reader.preflight()
        self._print_env_banner()

        last_offset = self.pg.get_last_committed_offset()
        self.log.info(f"数据源: {Config.DATA_SOURCE}")
        self.log.info(f"断点位置: 从第 {last_offset} 条开始")

        batch_id = str(uuid.uuid4())
        self.pg.create_batch(batch_id, source_offset=last_offset)
        self.log.info(f"批次 ID: {batch_id}")

        sent = 0
        failed = 0
        records = []
        skipped = 0

        try:
            for raw in self.reader.stream():
                if not self._running:
                    self.log.info("优雅退出中...")
                    break

                if skipped < last_offset:
                    skipped += 1
                    if skipped % 1000 == 0 and Config.DATA_SOURCE == "huggingface":
                        self.log.info(f"跳过中... {skipped}/{last_offset}")
                    continue

                paper = self.reader.parse(raw)
                if paper is None:
                    failed += 1
                    continue

                size = len(json.dumps(paper, ensure_ascii=False).encode("utf-8"))
                if size > Config.MAX_MSG_SIZE:
                    failed += 1
                    self.log.warning(
                        f"消息过大，跳过: {paper.get('paper_id')} "
                        f"= {size/1024/1024:.2f}MB"
                    )
                    continue

                paper["_batch_id"] = batch_id
                paper["_ingested_at"] = datetime.utcnow().isoformat()
                paper["_source"] = "unarxive_2024"

                self.kafka.send(Config.KAFKA_TOPIC, paper)
                records.append(paper)
                sent += 1

                if Config.SEND_INTERVAL > 0:
                    time.sleep(Config.SEND_INTERVAL)

                if sent % 500 == 0:
                    elapsed = time.time() - start_time
                    rate = sent / elapsed if elapsed > 0 else 0
                    self.log.info(f"进度: 已发送 {sent} 条 ({rate:.0f} 条/秒)")

                if Config.RUN_LIMIT > 0 and sent >= Config.RUN_LIMIT:
                    self.log.info(f"达到 RUN_LIMIT={Config.RUN_LIMIT}，停止")
                    break

            self.kafka.flush()
            checksum = self.pg.compute_checksum(records)
            self.pg.commit_batch(
                batch_id,
                source_end=last_offset + sent,
                record_count=sent,
                checksum=checksum,
            )

        except Exception as e:
            self.pg.fail_batch(batch_id, str(e))
            self.log.error(f"批次失败: {e}")
            raise

        finally:
            self.kafka.close()
            self.pg.close()

        elapsed = time.time() - start_time
        self.log.info("=" * 60)
        self.log.info(f"完成: 发送 {sent} 条, 失败 {failed} 条, 耗时 {elapsed:.0f}s")
        self.log.info(f"下次运行断点: {last_offset + sent}")
        self.log.info("=" * 60)
