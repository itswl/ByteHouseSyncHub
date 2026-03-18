# Elasticsearch to ByteHouse 同步工具

将 Elasticsearch 索引同步到 ByteHouse，支持全量同步和基于时间字段的增量同步。

## 快速开始

```bash
# 1. 复制配置文件
cp .env.example .env

# 2. 编辑配置
vim .env

# 3. Docker Compose 启动
docker-compose up -d

# 4. 查看日志
docker-compose logs -f
```

## 配置说明

```bash
# ========== Elasticsearch 配置 ==========
ES_HOST=http://es-host:9200
ES_USER=admin
ES_PASSWORD=xxx

# ========== ByteHouse 配置 ==========
BYTEHOUSE_HOST=tenant-xxx.bytehouse.volces.com
BYTEHOUSE_PORT=19000
BYTEHOUSE_USER=bytehouse
BYTEHOUSE_PASSWORD=xxx
BYTEHOUSE_SECURE=true

# ========== 同步配置 ==========
TARGET_DATABASE=es_migration         # 目标数据库
INDEX_PATTERN=*                      # 要同步的索引，支持通配符
BATCH_SIZE=1000                      # 批次大小
SCROLL_SIZE=1000                     # ES 滚动查询大小
INCREMENTAL_INTERVAL=60              # 增量同步间隔（秒）
SKIP_FULL_SYNC=false                 # 是否跳过全量同步
STORE_SOURCE=false                   # 是否存储原始 _source

# ========== 告警配置 ==========
FEISHU_WEBHOOK=                      # 飞书 Webhook（可选）
LOG_LEVEL=INFO                       # 日志级别
```

## 命令行用法

```bash
# 全量同步
python es_to_bytehouse.py --mode full

# 单次增量同步
python es_to_bytehouse.py --mode incremental

# 持续同步（全量 + 增量）
python es_to_bytehouse.py --mode continuous --interval 60

# 同步指定索引
python es_to_bytehouse.py --mode full --index "user_info_prod"

# 列出所有索引
python es_to_bytehouse.py --list-only
```

## 同步模式

| 模式 | 说明 |
|------|------|
| `full` | 全量同步 |
| `incremental` | 单次增量同步 |
| `continuous` | 持续同步（先全量后增量）|

## 索引匹配

```bash
# 所有索引
INDEX_PATTERN=*

# 指定索引
INDEX_PATTERN=user_info_prod

# 通配符
INDEX_PATTERN=user_*
INDEX_PATTERN=*_prod
```

## 数据结构

同步后的表结构：

| 字段 | 说明 |
|------|------|
| `_id` | ES 文档 ID |
| `_source` | 原始 JSON（可选，`STORE_SOURCE=true`）|
| `_timestamp` | 同步时间戳 |
| 其他字段 | ES 文档展平后的字段 |

## 本地运行

```bash
# 安装依赖
pip install -r requirements.txt

# 运行
python es_to_bytehouse.py --mode full
```

## Docker 构建

```bash
# 构建镜像
docker build -t es-bytehouse-sync .

# 运行
docker run -d --env-file .env es-bytehouse-sync

# 跳过全量同步
docker run -d --env-file .env -e SKIP_FULL_SYNC=true es-bytehouse-sync
```

## 注意事项

1. **时间字段**：增量同步需要 ES 文档有 `timestamp` 或 `@timestamp` 字段
2. **字段展平**：嵌套字段会被展平，如 `user.name` → `user_name`
3. **字段类型**：所有字段存储为 String 类型
4. **滚动查询**：全量同步使用 ES scroll API，避免超时
