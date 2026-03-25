FROM python:3.11-slim

WORKDIR /app

# 安装依赖
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 复制代码
COPY bytehouse_to_bytehouse.py .

# 创建日志目录
RUN mkdir -p /app/logs

# 设置环境变量
ENV LOG_FILE=/app/logs/bytehouse_to_bytehouse.log
ENV PYTHONUNBUFFERED=1

ENTRYPOINT ["python", "bytehouse_to_bytehouse.py"]
CMD ["--mode", "full"]
