#!/usr/bin/env bash
# init-minio.sh - 创建 MinIO Bucket（逻辑隔离：arxiv-dev / arxiv-prod）
#
# 用法：
#   bash scripts/init-minio.sh              # 按当前 ENV（默认 test → arxiv-dev）
#   ENV=prod bash scripts/init-minio.sh     # → arxiv-prod
#
# bucket 名从顶层 config.py 派生（单一事实来源），与代码保持一致。
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

ENV="${ENV:-test}"
export ENV

BUCKET="$(python3 -c 'import sys; sys.path.insert(0, "."); from config import get_minio_bucket; print(get_minio_bucket())')"

MINIO_ENDPOINT="${MINIO_ENDPOINT:-http://minio.minio:9000}"
MINIO_ACCESS_KEY="${MINIO_ACCESS_KEY:-minioadmin}"
MINIO_SECRET_KEY="${MINIO_SECRET_KEY:-minioadmin123}"

if ! command -v mc >/dev/null 2>&1; then
  cat >&2 <<'EOF'
[ERROR] 未找到 MinIO 客户端 mc。
  - 安装: https://min.io/docs/minio/linux/reference/minio-mc.html
  - 或进入 MinIO Pod 内用 mc 创建
  - 或用 MinIO Console 网页手动创建 bucket
EOF
  exit 1
fi

echo "MinIO 端点: ${MINIO_ENDPOINT}"
echo "当前环境:   ${ENV} → bucket: ${BUCKET}"

mc alias set arxiv "${MINIO_ENDPOINT}" "${MINIO_ACCESS_KEY}" "${MINIO_SECRET_KEY}"
mc mb --ignore-existing "arxiv/${BUCKET}"

echo "✅ bucket 就绪: ${BUCKET}"
