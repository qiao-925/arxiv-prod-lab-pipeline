#!/usr/bin/env bash
# init-resources.sh - 一键初始化逻辑隔离资源（dev + prod）
#
#   Kafka Topics    : arxiv-papers-dev / arxiv-papers-prod
#   PG Databases    : arxiv_dev / arxiv_prod
#   MinIO Buckets   : arxiv-dev / arxiv-prod
#
# 需 kubectl 可用；MinIO 需 mc（或改用 Console 手动创建）。
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

echo "=========================================================================="
echo "初始化逻辑隔离资源（dev + prod）"
echo "=========================================================================="

echo
echo "[1/3] Kafka Topics"
kubectl apply -f k8s/kafka-topic-dev.yaml
kubectl apply -f k8s/kafka-topic-prod.yaml

echo
echo "[2/3] PostgreSQL Databases"
# Job 名不可变，重跑前先删除旧 Job
kubectl delete job pg-init-schema -n database --ignore-not-found
kubectl apply -f k8s/pg-init.yaml
if ! kubectl wait --for=condition=complete job/pg-init-schema -n database --timeout=120s; then
  echo "[ERROR] pg-init-schema 未在超时内完成，日志如下：" >&2
  kubectl logs job/pg-init-schema -n database >&2 || true
  exit 1
fi
kubectl logs job/pg-init-schema -n database

echo
echo "[3/3] MinIO Buckets"
ENV=test bash scripts/init-minio.sh
ENV=prod bash scripts/init-minio.sh

echo
echo "=========================================================================="
echo "完成。资源清单："
echo "  Kafka Topic : arxiv-papers-dev / arxiv-papers-prod"
echo "  PG Database : arxiv_dev / arxiv_prod"
echo "  MinIO Bucket: arxiv-dev / arxiv-prod"
echo "=========================================================================="
