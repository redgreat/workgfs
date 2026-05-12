#!/bin/bash

# 自动定位到项目根目录（script 的上一级）
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${PROJECT_ROOT}" || { echo "进入目录失败，中止执行"; exit 1; }

SERVICE_NAME="postgres"
CONTAINER_NAME="workgfs"
IMAGE_NAME="registry.cn-hangzhou.aliyuncs.com/redgreat/workgfs:latest"

echo "=========================================="
echo "    开始重新部署 workgfs 容器环境"
echo "=========================================="

echo "➤ 1. 停止并移除旧容器: ${CONTAINER_NAME}..."
docker compose stop "${SERVICE_NAME}"
docker compose rm -f "${SERVICE_NAME}"

echo "➤ 2. 删除本地旧镜像: ${IMAGE_NAME} ..."
docker image rm -f "${IMAGE_NAME}" 2>/dev/null || true

echo "➤ 3. 清理所有旧的日志数据..."
# 删除根目录下 ./logs/ 内的所有文件及文件夹
mkdir -p ./logs
rm -rf ./logs/*
echo "日志清理完成。"

echo "➤ 4. 重新拉取最新镜像并后台启动..."
docker compose pull "${SERVICE_NAME}"
docker compose up -d "${SERVICE_NAME}"

echo "等待 2 秒检查容器状态..."
sleep 2

echo "➤ 5. 当前 ${CONTAINER_NAME} 容器运行状态:"
docker ps -a --filter "name=${CONTAINER_NAME}"

echo "=========================================="
echo "    容器已更新并重启完成！"
echo "    手动触发同步命令："
echo "docker compose exec ${SERVICE_NAME} python src/manual_sync.py"
echo "=========================================="
