# ByteHouse to ByteHouse 同步工具

在 ByteHouse 实例之间同步数据，支持跨库或跨实例数据迁移。

```
-- 同库不同数据库
INSERT INTO target_db.table_name 
SELECT * FROM source_db.table_name;

-- 跨实例（需要配置外部表）
CREATE TABLE source_db.remote_table AS remote('source_host:19000', 'source_db', 'table_name', 'user', 'password');
INSERT INTO target_db.table_name SELECT * FROM source_db.remote_table;
```

## 快速开始

```bash
# 1. 复制配置文件
cp bytehouse_to_bytehouse.env.example bytehouse_to_bytehouse.env

# 2. 编辑配置
vim bytehouse_to_bytehouse.env

# 3. Docker Compose 启动
docker-compose up -d

# 4. 查看日志
docker-compose logs -f
```

## 配置说明

```bash
# ========== 源 ByteHouse 配置 ==========
SOURCE_BH_HOST=source-tenant.bytehouse.volces.com
SOURCE_BH_PORT=19000
SOURCE_BH_USER=bytehouse
SOURCE_BH_PASSWORD=xxx
SOURCE_BH_DATABASE=source_db
SOURCE_BH_SECURE=true

# ========== 目标 ByteHouse 配置 ==========
TARGET_BH_HOST=target-tenant.bytehouse.volces.com
TARGET_BH_PORT=19000
TARGET_BH_USER=bytehouse
TARGET_BH_PASSWORD=xxx
TARGET_BH_DATABASE=target_db
TARGET_BH_SECURE=true

# ========== 同步配置 ==========
TABLE_PATTERN=*                     # 要同步的表，支持逗号分隔、通配符
SYNC_BATCH_SIZE=10000               # 批次大小
INCREMENTAL_INTERVAL=60             # 增量同步间隔（秒）
```

## 命令行用法

```bash
# 全量同步所有表
python bytehouse_to_bytehouse.py --mode full

# 全量同步指定表
python bytehouse_to_bytehouse.py --mode full --table-pattern "user_*,order_*"

# 单次增量同步
python bytehouse_to_bytehouse.py --mode incremental --time-column "created_at"

# 持续增量同步
python bytehouse_to_bytehouse.py --mode incremental --time-column "created_at" --continuous --interval 60

# 查看同步状态
python bytehouse_to_bytehouse.py --mode status
```

## 同步模式

| 模式 | 说明 |
|------|------|
| `full` | 全量同步，删除目标表后重新创建并导入 |
| `incremental` | 增量同步，基于时间字段同步新数据 |
| `status` | 查看同步状态 |

## 表匹配语法

```bash
# 单个表
TABLE_PATTERN=users

# 多个表（逗号分隔）
TABLE_PATTERN=users,orders,products

# 通配符
TABLE_PATTERN=*_log

# 混合
TABLE_PATTERN=users,orders,*_log
```

## 本地运行

```bash
# 安装依赖
pip install -r requirements.txt

# 运行
python bytehouse_to_bytehouse.py --mode full
```

## 使用场景

1. **跨实例迁移**：从测试环境同步到生产环境
2. **数据备份**：定期同步数据到备份库
3. **跨库同步**：同一实例内不同数据库之间同步

## 注意事项

1. **表结构**：全量同步会复制源表结构，引擎自动转为 CnchMergeTree
2. **增量字段**：增量同步需要源表有时间字段（如 `created_at`）
3. **网络访问**：确保容器能访问源和目标 ByteHouse
4. **权限**：需要源库的读权限和目标库的写权限
