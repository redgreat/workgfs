docker exec -d workgfspro bash -c "cd /app && python src/manual_sync.py 2025-06-01 2025-07-01 > logs/manual_sync.log 2>&1"

# 查看容器内进程
docker exec workgfspro ps aux | grep python

# 查看日志
docker exec workgfspro tail -f /app/logs/manual_sync.log

# 或者查看 loguru 的日志文件
docker exec workgfspro find /app/logs -name "*.log" -exec tail -f {} \;
