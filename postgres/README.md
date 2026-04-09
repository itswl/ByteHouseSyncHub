# PostgreSQL to ByteHouse 同步工具

将 PostgreSQL 数据同步到 ByteHouse，支持全量和增量同步。

## 快速开始

```bash
cd postgres

# 1. 复制配置
cp postgres_to_bytehouse.env.example postgres_to_bytehouse.env

# 2. 编辑配置（填写 ByteHouse 密码）
vim postgres_to_bytehouse.env

# 3. 全量同步
python postgres_to_bytehouse.py --mode full

# 4. 持续增量
python postgres_to_bytehouse.py --mode incremental --continuous

# 5. Docker 部署
docker-compose up -d
```

## 配置说明

```bash
# PostgreSQL
PG_HOST=postgres-d1dfb6af8127-public.rds-pg.volces.com
PG_PORT=5432
PG_USER=root
PG_PASSWORD=ns2024Xqrif848
PG_DATABASE=eve-cn-prod-backend

# ByteHouse
BH_HOST=tenant-xxx.bytehouse.volces.com
BH_PASSWORD=xxx
BH_DATABASE=eve_cn_prod

# 同步
TABLE_PATTERN=*          # 支持通配符
TIME_COLUMN=created_at   # 增量时间字段
```

## 使用场景

- 数据迁移：PG → ByteHouse
- 分析同步：业务库 → 分析库
- 定期同步：增量更新数据

## 字段类型映射

| PostgreSQL | ByteHouse |
|------------|-----------|
| integer/bigint | Int64 |
| real/double | Float64 |
| boolean | Bool |
| timestamp | DateTime |
| json/jsonb | String |
| text/varchar | String |
