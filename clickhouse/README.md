# ClickHouse to ByteHouse 同步工具

将 ClickHouse 表同步到 ByteHouse，支持全量同步和基于时间字段的增量同步。

## 快速开始

```bash
# 1. 复制配置文件
cp clickhouse_to_bytehouse.env.example clickhouse_to_bytehouse.env

# 2. 编辑配置
vim clickhouse_to_bytehouse.env

# 3. Docker Compose 启动
docker-compose up -d

# 4. 查看日志
docker-compose logs -f
```

## 配置说明

```bash
# ========== 源 ClickHouse 配置 ==========
SOURCE_CH_HOST=192.168.1.100
SOURCE_CH_PORT=9000
SOURCE_CH_USER=default
SOURCE_CH_PASSWORD=xxx
SOURCE_CH_DATABASE=mydb

# ========== 目标 ByteHouse 配置 ==========
TARGET_BH_HOST=tenant-xxx.bytehouse.volces.com
TARGET_BH_PORT=19000
TARGET_BH_USER=bytehouse
TARGET_BH_PASSWORD=xxx
TARGET_BH_DATABASE=ch_sync

# ========== 同步配置 ==========
TABLE_PATTERN=*                     # 要同步的表，支持逗号分隔、通配符
SYNC_BATCH_SIZE=10000               # 批次大小
INCREMENTAL_INTERVAL=60             # 增量同步间隔（秒）

# ========== 告警配置 ==========
FEISHU_WEBHOOK=                     # 飞书 Webhook（可选）
LOG_LEVEL=INFO                      # 日志级别
```

## 命令行用法

```bash
# 全量同步
python clickhouse_to_bytehouse.py --mode full

# 单次增量同步
python clickhouse_to_bytehouse.py --mode incremental

# 持续增量同步
python clickhouse_to_bytehouse.py --mode incremental --continuous --interval 60

# 指定表
python clickhouse_to_bytehouse.py --mode full --table-pattern "spans,logs"

# 指定时间字段和开始日期
python clickhouse_to_bytehouse.py --mode incremental --time-column "created_at" --start-date "2026-01-01"

# 查看同步状态
python clickhouse_to_bytehouse.py --mode status
```

## 同步模式

| 模式 | 说明 |
|------|------|
| `full` | 全量同步，复制源表全部数据 |
| `incremental` | 增量同步，基于时间字段同步新数据 |
| `status` | 查看同步状态 |

## 表匹配语法

```bash
# 单个表
TABLE_PATTERN=spans

# 多个表（逗号分隔）
TABLE_PATTERN=spans,logs,traces

# 通配符
TABLE_PATTERN=*_log

# 混合
TABLE_PATTERN=spans,*_events,user_*
```

## 增量同步参数

| 参数 | 说明 | 默认值 |
|------|------|--------|
| `--time-column` | 时间字段名 | `created_at` |
| `--start-date` | 开始日期 | 无（从上次同步位置继续）|
| `--continuous` | 持续同步 | 否 |
| `--interval` | 同步间隔（秒）| 60 |

## 本地运行

```bash
# 安装依赖
pip install -r requirements.txt

# 运行
python clickhouse_to_bytehouse.py --mode full
```

## Docker 构建

```bash
# 构建镜像
docker build -t clickhouse-bytehouse-sync .

# 运行
docker run -d --env-file clickhouse_to_bytehouse.env clickhouse-bytehouse-sync
```

## 注意事项

1. **表结构**：自动复制源表结构，使用 CnchMergeTree 引擎
2. **时间字段**：增量同步需要源表有时间字段
3. **网络访问**：确保容器能访问源 ClickHouse 和目标 ByteHouse
