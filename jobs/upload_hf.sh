#!/usr/bin/env bash
# 上传本地 parquet 目录到 HF Storage Bucket 的包装脚本。
#
# 用法：
#   upload_hf.sh <本地输出目录> <hf://buckets/namespace/bucket>
#
# 环境变量（可由调用方传入，脚本内显式 export）：
#   HF_ENDPOINT  默认 https://hf-mirror.com
#   HF_TOKEN     必填
#   HF_DEBUG     默认 1，得到完整错误堆栈
set -euo pipefail

OUT_DIR="${1:?缺少本地输出目录参数}"
BUCKET="${2:?缺少 HF bucket 参数}"

: "${HF_TOKEN:?缺少 HF_TOKEN 环境变量}"

export HF_ENDPOINT="${HF_ENDPOINT:-https://hf-mirror.com}"
export HF_TOKEN="${HF_TOKEN}"
export HF_DEBUG="${HF_DEBUG:-1}"

exec hf sync "$@" -v
