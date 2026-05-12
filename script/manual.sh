docker exec -d workgfs bash -c "cd /app && python src/manual_sync.py > logs/manual_sync.log 2>&1"

# 查看容器内进程
docker exec workgfs ps aux | grep python

# 查看日志
docker exec workgfs tail -f /app/logs/manual_sync.log

# 或者查看 loguru 的日志文件
docker exec workgfs find /app/logs -name "*.log" -exec tail -f {} \;
