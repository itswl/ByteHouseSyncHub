# ByteHouse 数据同步工具集

将多种数据源同步到 ByteHouse（火山引擎版 ClickHouse），支持全量同步和持续增量同步。

## 支持的数据源

| 数据源 | 目录 | 说明 |
|--------|------|------|
| Elasticsearch | [`es/`](es/) | ES 索引同步到 ByteHouse |
| ClickHouse | [`clickhouse/`](clickhouse/) | ClickHouse 表同步到 ByteHouse |
| MongoDB | [`mongodb/`](mongodb/) | MongoDB 集合同步到 ByteHouse |
| ByteHouse | [`bytehouse/`](bytehouse/) | ByteHouse 跨库/跨实例同步 |
| PostgreSQL | [`postgres/`](postgres/) | PostgreSQL 同步到 ByteHouse |

## 功能特性

- **全量同步**：首次运行时迁移全量数据
- **增量同步**：基于时间戳/ID 字段持续同步新增数据
- **auto 模式**：智能判断，首次全量后自动转增量，重启后跳过全量
- **自动字段发现**：自动扫描源数据结构并创建目标表
- **多表/多集合支持**：支持逗号分隔指定多个表或使用通配符
- **飞书告警**：WARNING/ERROR 自动推送飞书通知
- **Docker 部署**：支持 Docker Compose 一键部署

## 目录结构

```
bytehouse/
├── README.md                        # 项目概述
│
├── es/                              # Elasticsearch → ByteHouse
│   ├── README.md
│   ├── es_to_bytehouse.py
│   ├── requirements.txt
│   ├── .env.example
│   ├── Dockerfile
│   ├── docker-compose.yml
│   └── entrypoint.sh
│
├── clickhouse/                      # ClickHouse → ByteHouse
│   ├── README.md
│   ├── clickhouse_to_bytehouse.py
│   ├── requirements.txt
│   ├── clickhouse_to_bytehouse.env.example
│   ├── Dockerfile
│   └── docker-compose.yml
│
├── mongodb/                         # MongoDB → ByteHouse
│   ├── README.md
│   ├── mongodb_to_bytehouse.py
│   ├── requirements.txt
│   ├── mongodb_to_bytehouse.env.example
│   ├── Dockerfile
│   └── docker-compose.yml
│
├── bytehouse/                       # ByteHouse → ByteHouse
│   ├── README.md
│   ├── bytehouse_to_bytehouse.py
│   ├── requirements.txt
│   ├── bytehouse_to_bytehouse.env.example
│   ├── Dockerfile
│   └── docker-compose.yml
│
└── postgres/                        # PostgreSQL → ByteHouse
    ├── README.md
    ├── postgres_to_bytehouse.py
    ├── requirements.txt
    ├── postgres_to_bytehouse.env.example
    ├── Dockerfile
    └── docker-compose.yml
```

## 快速开始

```bash
# 进入对应数据源目录
cd mongodb  # 或 clickhouse / es

# 复制配置文件并填写
cp *.env.example *.env
vim *.env

# Docker Compose 启动
docker-compose up -d

# 查看日志
docker-compose logs -f
```

## 同步模式

| 模式 | 说明 |
|------|------|
| `full` | 全量同步 |
| `incremental` | 增量同步 |
| `auto` | 智能模式：首次全量，之后持续增量 |
| `status` | 查看同步状态 |

## 通用配置

### 表/集合匹配

支持逗号分隔多个模式和通配符：

```bash
# 单个
COLLECTION_PATTERN=message_log

# 多个（逗号分隔）
COLLECTION_PATTERN=message_log,user_log

# 通配符
COLLECTION_PATTERN=*_log

# 混合
COLLECTION_PATTERN=message_log,*_event
```

### 飞书告警

配置 `FEISHU_WEBHOOK` 后，WARNING/ERROR 级别日志自动推送飞书。

## 注意事项

1. **表创建延迟**：ByteHouse 表创建后需等待 5 秒才能插入数据
2. **增量字段**：MongoDB 用 `_id`，ClickHouse/ES 需指定时间字段
3. **字段类型**：所有字段存储为 String，嵌套对象转 JSON 字符串

## License

MIT
