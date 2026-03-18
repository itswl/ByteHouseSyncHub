# MongoDB to ByteHouse 同步工具

将 MongoDB 集合同步到 ByteHouse，支持全量同步和基于 `_id` 的增量同步。

## 快速开始

```bash
# 1. 复制配置文件
cp mongodb_to_bytehouse.env.example mongodb_to_bytehouse.env

# 2. 编辑配置
vim mongodb_to_bytehouse.env

# 3. Docker Compose 启动
docker-compose up -d

# 4. 查看日志
docker-compose logs -f
```

## 配置说明

```bash
# ========== MongoDB 配置 ==========
MONGO_URI=mongodb://user:pass@host:port/?authSource=admin
MONGO_DATABASE=mydb

# ========== ByteHouse 配置 ==========
TARGET_BH_HOST=tenant-xxx.bytehouse.volces.com
TARGET_BH_PORT=19000
TARGET_BH_USER=bytehouse
TARGET_BH_PASSWORD=xxx
TARGET_BH_DATABASE=mongo_sync

# ========== 同步配置 ==========
COLLECTION_PATTERN=*                # 要同步的集合，支持逗号分隔、通配符
SYNC_BATCH_SIZE=1000                # 批次大小
INCREMENTAL_INTERVAL=60             # 增量同步间隔（秒）
STORE_SOURCE=false                  # 是否存储原始 _source JSON
ADD_TIMESTAMP=false                 # 是否添加 _timestamp 字段

# ========== 告警配置 ==========
FEISHU_WEBHOOK=                     # 飞书 Webhook（可选）
LOG_LEVEL=INFO                      # 日志级别
```

## 命令行用法

```bash
# auto 模式（推荐）：首次全量，之后持续增量，重启跳过全量
python mongodb_to_bytehouse.py --mode auto

# 全量同步
python mongodb_to_bytehouse.py --mode full

# 增量同步
python mongodb_to_bytehouse.py --mode incremental

# 指定集合
python mongodb_to_bytehouse.py --mode auto --collection-pattern "message_log,user_log"

# 查看同步状态
python mongodb_to_bytehouse.py --mode status
```

## 同步模式

| 模式 | 说明 |
|------|------|
| `auto` | **推荐** - 首次全量，之后持续增量，重启后自动跳过全量 |
| `full` | 全量同步，清空目标表后重新导入 |
| `incremental` | 增量同步，基于 `_id` 同步新数据 |
| `status` | 查看同步状态 |

## 集合匹配语法

```bash
# 单个集合
COLLECTION_PATTERN=message_log

# 多个集合（逗号分隔）
COLLECTION_PATTERN=message_log,user_log,order_log

# 通配符
COLLECTION_PATTERN=*_log

# 混合
COLLECTION_PATTERN=message_log,*_event,user_*
```

## 字段处理

- **_id**：MongoDB 文档 ID，转为 String
- **嵌套对象**：转为 JSON 字符串存储
- **数组**：转为 JSON 字符串存储
- **_source**：可选，存储原始文档 JSON（`STORE_SOURCE=true`）
- **_timestamp**：可选，同步时间戳（`ADD_TIMESTAMP=true`）

## 本地运行

```bash
# 安装依赖
pip install -r requirements.txt

# 运行
python mongodb_to_bytehouse.py --mode auto
```

## Docker 构建

```bash
# 构建镜像
docker build -t mongodb-bytehouse-sync .

# 运行
docker run -d --env-file mongodb_to_bytehouse.env mongodb-bytehouse-sync
```
