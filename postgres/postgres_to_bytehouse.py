#!/usr/bin/env python3
"""
PostgreSQL to ByteHouse 数据同步工具
支持全量同步和增量同步
"""

import os
import sys
import time
import json
import logging
import requests
import fnmatch
from datetime import datetime
from typing import Dict, List, Tuple, Optional
import psycopg2
from psycopg2.extras import RealDictCursor
from clickhouse_driver import Client

# 加载 .env 文件
try:
    from dotenv import load_dotenv
    load_dotenv('postgres_to_bytehouse.env')
    load_dotenv('.env')
except ImportError:
    pass

# 配置
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
LOG_FILE = os.getenv("LOG_FILE", "")
FEISHU_WEBHOOK = os.getenv("FEISHU_WEBHOOK", "")


class FeishuHandler(logging.Handler):
    """飞书 Webhook 日志处理器"""
    
    def __init__(self, webhook_url: str):
        super().__init__()
        self.webhook_url = webhook_url
        self.last_send_time = {}
        self.send_interval = 60
    
    def emit(self, record):
        if not self.webhook_url:
            return
        try:
            if record.levelno < logging.WARNING:
                return
            
            msg_key = f"{record.levelname}:{record.getMessage()[:100]}"
            current_time = time.time()
            if msg_key in self.last_send_time:
                if current_time - self.last_send_time[msg_key] < self.send_interval:
                    return
            self.last_send_time[msg_key] = current_time
            
            emoji = "⚠️" if record.levelno == logging.WARNING else "❌"
            content = f"**时间**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
            content += f"**级别**: {record.levelname}\n"
            content += f"**消息**: {record.getMessage()}\n"
            
            payload = {
                "msg_type": "interactive",
                "card": {
                    "header": {
                        "title": {"tag": "plain_text", "content": f"{emoji} PG 同步告警"},
                        "template": "orange" if record.levelno == logging.WARNING else "red"
                    },
                    "elements": [{"tag": "markdown", "content": content}]
                }
            }
            requests.post(self.webhook_url, json=payload, timeout=5)
        except:
            pass


# 配置日志
numeric_level = getattr(logging, LOG_LEVEL.upper(), logging.INFO)
logging.basicConfig(level=numeric_level, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

if LOG_FILE:
    try:
        log_dir = os.path.dirname(LOG_FILE)
        if not log_dir or os.path.exists(log_dir):
            file_handler = logging.FileHandler(LOG_FILE, encoding='utf-8')
            file_handler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s'))
            logger.addHandler(file_handler)
    except Exception as e:
        logger.warning(f"无法创建日志文件: {e}")

console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s'))
logger.addHandler(console_handler)

if FEISHU_WEBHOOK:
    feishu_handler = FeishuHandler(FEISHU_WEBHOOK)
    feishu_handler.setLevel(logging.WARNING)
    logger.addHandler(feishu_handler)
    logger.info("✓ 飞书告警已启用")


# PostgreSQL 配置
PG_HOST = os.getenv("PG_HOST", "")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_USER = os.getenv("PG_USER", "root")
PG_PASSWORD = os.getenv("PG_PASSWORD", "")
PG_DATABASE = os.getenv("PG_DATABASE", "eve-cn-prod-backend")

# ByteHouse 配置
BH_HOST = os.getenv("BH_HOST", "")
BH_PORT = int(os.getenv("BH_PORT", "19000"))
BH_USER = os.getenv("BH_USER", "bytehouse")
BH_PASSWORD = os.getenv("BH_PASSWORD", "")
BH_DATABASE = os.getenv("BH_DATABASE", "default")

# 同步配置
BATCH_SIZE = int(os.getenv("SYNC_BATCH_SIZE", "10000"))
INCREMENTAL_INTERVAL = int(os.getenv("INCREMENTAL_INTERVAL", "60"))
TABLE_PATTERN = os.getenv("TABLE_PATTERN", "*")
TIME_COLUMN = os.getenv("TIME_COLUMN", "created_at")


def match_tables(tables: list, pattern: str) -> list:
    """匹配表名，支持逗号分隔的多个模式"""
    if pattern == "*":
        return tables
    
    patterns = [p.strip() for p in pattern.split(",") if p.strip()]
    matched = set()
    for p in patterns:
        for t in tables:
            if fnmatch.fnmatch(t, p) or t == p:
                matched.add(t)
    return list(matched)


class PGSync:
    def __init__(self):
        self.pg_conn = None
        self.bh_client = None

    def connect_pg(self) -> bool:
        """连接 PostgreSQL"""
        try:
            self.pg_conn = psycopg2.connect(
                host=PG_HOST,
                port=PG_PORT,
                user=PG_USER,
                password=PG_PASSWORD,
                database=PG_DATABASE
            )
            logger.info(f"✓ PostgreSQL 连接成功: {PG_HOST}:{PG_PORT}/{PG_DATABASE}")
            return True
        except Exception as e:
            logger.error(f"✗ PostgreSQL 连接失败: {e}")
            return False

    def connect_bh(self) -> bool:
        """连接 ByteHouse"""
        try:
            self.bh_client = Client(
                host=BH_HOST,
                port=BH_PORT,
                user=BH_USER,
                password=BH_PASSWORD,
                secure=True
            )
            self.bh_client.execute("SELECT 1")
            
            try:
                self.bh_client.execute(f"CREATE DATABASE IF NOT EXISTS `{BH_DATABASE}`")
                logger.info(f"✓ 目标数据库 {BH_DATABASE} 准备就绪")
            except Exception as e:
                logger.warning(f"创建目标数据库失败: {e}")
            
            self.bh_client.execute(f"USE `{BH_DATABASE}`")
            
            # 创建同步状态表
            self.bh_client.execute("""
                CREATE TABLE IF NOT EXISTS `_sync_state` (
                    `table_name` String,
                    `last_value` String,
                    `sync_count` Int64,
                    `sync_time` DateTime DEFAULT now()
                ) ENGINE = CnchMergeTree()
                ORDER BY (table_name, sync_time)
            """)
            logger.info(f"✓ 同步状态表 _sync_state 准备就绪")
            logger.info(f"✓ ByteHouse 连接成功: {BH_HOST}:{BH_PORT}/{BH_DATABASE}")
            return True
        except Exception as e:
            logger.error(f"✗ ByteHouse 连接失败: {e}")
            return False

    def get_tables(self) -> List[str]:
        """获取 PostgreSQL 表列表"""
        with self.pg_conn.cursor() as cursor:
            cursor.execute("""
                SELECT table_name FROM information_schema.tables 
                WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
            """)
            return [row[0] for row in cursor.fetchall()]

    def get_table_columns(self, table_name: str) -> List[Tuple[str, str]]:
        """获取表列信息"""
        with self.pg_conn.cursor() as cursor:
            cursor.execute("""
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = %s
                ORDER BY ordinal_position
            """, (table_name,))
            return cursor.fetchall()

    def get_table_count(self, table_name: str) -> int:
        """获取表数据量"""
        with self.pg_conn.cursor() as cursor:
            cursor.execute(f"SELECT count(*) FROM {table_name}")
            return cursor.fetchone()[0]

    def get_sync_state(self, table_name: str) -> Optional[str]:
        """获取同步状态"""
        try:
            result = self.bh_client.execute(
                f"SELECT last_value FROM `_sync_state` WHERE table_name = '{table_name}' ORDER BY sync_time DESC LIMIT 1"
            )
            if result:
                return result[0][0]
        except:
            pass
        return None

    def update_sync_state(self, table_name: str, last_value: str, count: int):
        """更新同步状态"""
        try:
            self.bh_client.execute(
                f"INSERT INTO `_sync_state` (table_name, last_value, sync_count) VALUES",
                [(table_name, last_value, count)]
            )
        except Exception as e:
            logger.warning(f"更新同步状态失败: {e}")

    def convert_pg_type(self, pg_type: str) -> str:
        """转换 PG 类型到 ClickHouse 类型"""
        type_mapping = {
            'integer': 'Int64',
            'bigint': 'Int64',
            'smallint': 'Int32',
            'serial': 'Int64',
            'bigserial': 'Int64',
            'real': 'Float32',
            'double precision': 'Float64',
            'numeric': 'Float64',
            'decimal': 'Float64',
            'boolean': 'Bool',
            'boolean': 'Bool',
            'timestamp': 'DateTime',
            'timestamp with time zone': 'DateTime',
            'timestamp without time zone': 'DateTime',
            'date': 'Date',
            'json': 'String',
            'jsonb': 'String',
            'text': 'String',
            'character varying': 'String',
            'varchar': 'String',
            'char': 'String',
            'uuid': 'String',
            'bytea': 'String',
            'ARRAY': 'String',
        }
        
        for pg_t, ch_t in type_mapping.items():
            if pg_type.lower().startswith(pg_t.lower()):
                return ch_t
        
        return 'String'

    def create_table(self, table_name: str) -> bool:
        """在 ByteHouse 创建表"""
        try:
            columns = self.get_table_columns(table_name)
            
            col_defs = []
            for col_name, col_type, is_nullable in columns:
                ch_type = self.convert_pg_type(col_type)
                nullable = "Nullable" if is_nullable == 'YES' else ""
                if nullable:
                    col_defs.append(f"`{col_name}` Nullable({ch_type})")
                else:
                    col_defs.append(f"`{col_name}` {ch_type}")
            
            cols_str = ",\n".join(col_defs)
            sql = f"""
CREATE TABLE IF NOT EXISTS `{table_name}` (
{cols_str}
) ENGINE = CnchMergeTree()
ORDER BY tuple()
"""
            self.bh_client.execute(f"DROP TABLE IF EXISTS `{table_name}`")
            self.bh_client.execute(sql)
            logger.info(f"✓ 表 {table_name} 创建成功，{len(columns)} 个字段")
            
            time.sleep(5)
            return True
        except Exception as e:
            logger.error(f"创建表 {table_name} 失败: {e}")
            return False

    def sync_table_full(self, table_name: str, time_column: str = TIME_COLUMN) -> int:
        """全量同步表"""
        logger.info(f"开始全量同步: {table_name}")
        
        count = self.get_table_count(table_name)
        logger.info(f"  数据量: {count:,}")
        
        if count == 0:
            return 0
        
        if not self.create_table(table_name):
            return 0
        
        columns = self.get_table_columns(table_name)
        col_names = [c[0] for c in columns]
        
        total = 0
        offset = 0
        start = time.time()
        
        while offset < count:
            with self.pg_conn.cursor() as cursor:
                cursor.execute(f"SELECT * FROM {table_name} LIMIT {BATCH_SIZE} OFFSET {offset}")
                rows = cursor.fetchall()
                
                if not rows:
                    break
                
                # 转换数据
                data = []
                for row in rows:
                    converted = []
                    for val in row:
                        if isinstance(val, datetime):
                            # datetime 保持原样（clickhouse-driver 会自动处理）
                            converted.append(val)
                        elif isinstance(val, dict):
                            converted.append(json.dumps(val, ensure_ascii=False, default=str))
                        elif val is None:
                            converted.append(None)
                        else:
                            converted.append(str(val) if not isinstance(val, (int, float, bool)) else val)
                    data.append(tuple(converted))
                
                cols_str = ", ".join(f"`{c}`" for c in col_names)
                self.bh_client.execute(f"INSERT INTO `{table_name}` ({cols_str}) VALUES", data)
                
                total += len(data)
                offset += BATCH_SIZE
                
                elapsed = time.time() - start
                speed = total / elapsed if elapsed > 0 else 0
                logger.info(f"  进度: {total:,}/{count:,} ({100*total/count:.1f}%) | {speed:.0f}/s")
        
        # 保存最后一条记录的时间值（用于增量同步）
        if time_column and time_column in col_names:
            try:
                with self.pg_conn.cursor() as cursor:
                    cursor.execute(f"SELECT MAX({time_column}) FROM {table_name}")
                    result = cursor.fetchone()
                    if result and result[0]:
                        last_time_value = result[0].strftime('%Y-%m-%d %H:%M:%S') if isinstance(result[0], datetime) else str(result[0])
                        self.update_sync_state(table_name, last_time_value, total)
                    else:
                        self.update_sync_state(table_name, str(total), total)
            except:
                self.update_sync_state(table_name, str(total), total)
        else:
            self.update_sync_state(table_name, str(total), total)
        
        elapsed = time.time() - start
        logger.info(f"✓ {table_name} 全量完成: {total:,} 条，耗时 {elapsed:.1f}s")
        return total

    def sync_table_incremental(self, table_name: str, time_column: str = TIME_COLUMN) -> int:
        """增量同步表"""
        logger.info(f"增量同步: {table_name}")
        
        columns = self.get_table_columns(table_name)
        col_names = [c[0] for c in columns]
        
        if time_column not in col_names:
            logger.warning(f"  表 {table_name} 没有时间列 {time_column}，跳过")
            return 0
        
        last_value = self.get_sync_state(table_name)
        
        if last_value:
            query = f"SELECT * FROM {table_name} WHERE {time_column} > %s ORDER BY {time_column}"
            params = (last_value,)
            logger.info(f"  条件: {time_column} > {last_value}")
        else:
            query = f"SELECT * FROM {table_name} ORDER BY {time_column}"
            params = ()
        
        total = 0
        current_last = last_value
        
        with self.pg_conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query, params)
            rows = cursor.fetchall()
            
            if not rows:
                logger.info(f"  无新增数据")
                return 0
            
            for i in range(0, len(rows), BATCH_SIZE):
                batch = rows[i:i+BATCH_SIZE]
                
                data = []
                for row in batch:
                    converted = []
                    for col in col_names:
                        val = row[col]
                        if isinstance(val, datetime):
                            converted.append(val)  # 保持 datetime 对象
                            if col == time_column:
                                current_last = val.strftime('%Y-%m-%d %H:%M:%S')
                        elif isinstance(val, dict):
                            converted.append(json.dumps(val, ensure_ascii=False, default=str))
                        elif val is None:
                            converted.append(None)
                        else:
                            converted.append(str(val) if not isinstance(val, (int, float, bool)) else val)
                    data.append(tuple(converted))
                
                cols_str = ", ".join(f"`{c}`" for c in col_names)
                self.bh_client.execute(f"INSERT INTO `{table_name}` ({cols_str}) VALUES", data)
                total += len(data)
        
        if current_last:
            self.update_sync_state(table_name, current_last, total)
        
        logger.info(f"  增量完成: {total} 条")
        return total

    def run_full(self, table_pattern: str = "*"):
        """全量同步"""
        logger.info("=" * 60)
        logger.info("PostgreSQL to ByteHouse 全量同步")
        logger.info("=" * 60)
        
        if not self.connect_pg() or not self.connect_bh():
            return
        
        tables = self.get_tables()
        if table_pattern != "*":
            tables = match_tables(tables, table_pattern)
        
        if not tables:
            logger.warning("没有找到表")
            return
        
        logger.info(f"找到 {len(tables)} 个表")
        
        total = 0
        for t in tables:
            total += self.sync_table_full(t, TIME_COLUMN)
        
        logger.info(f"全量同步完成: {total:,} 条")

    def run_incremental(self, table_pattern: str = "*", time_column: str = TIME_COLUMN,
                       continuous: bool = False, interval: int = INCREMENTAL_INTERVAL):
        """增量同步"""
        logger.info("=" * 60)
        logger.info("PostgreSQL to ByteHouse 增量同步")
        logger.info("=" * 60)
        logger.info(f"时间字段: {time_column}")
        logger.info(f"模式: {'持续' if continuous else '单次'}")
        
        if not self.connect_pg() or not self.connect_bh():
            return
        
        tables = self.get_tables()
        if table_pattern != "*":
            tables = match_tables(tables, table_pattern)
        
        round_count = 0
        while True:
            round_count += 1
            logger.info(f"\n[第 {round_count} 轮]")
            
            total = 0
            for t in tables:
                total += self.sync_table_incremental(t, time_column)
            
            logger.info(f"本轮同步: {total} 条")
            
            if not continuous:
                break
            
            logger.info(f"等待 {interval} 秒...")
            time.sleep(interval)


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="PostgreSQL to ByteHouse 同步工具")
    parser.add_argument("--mode", choices=["full", "incremental", "auto"], default="auto")
    parser.add_argument("--table-pattern", default=TABLE_PATTERN)
    parser.add_argument("--time-column", default=TIME_COLUMN)
    parser.add_argument("--continuous", action="store_true")
    parser.add_argument("--interval", type=int, default=INCREMENTAL_INTERVAL)
    
    args = parser.parse_args()
    
    sync = PGSync()
    
    if args.mode == "auto":
        # auto 模式：先检查同步状态，需要全量则全量，然后进入增量
        logger.info("=" * 60)
        logger.info("自动模式：检测同步状态后决定全量或增量")
        logger.info("=" * 60)
        
        if not sync.connect_pg() or not sync.connect_bh():
            return
        
        tables = sync.get_tables()
        if args.table_pattern != "*":
            tables = match_tables(tables, args.table_pattern)
        
        # 检查哪些表需要同步
        need_full = []
        has_synced = []
        for t in tables:
            if sync.get_sync_state(t) is None:
                need_full.append(t)
            else:
                has_synced.append(t)
        
        if need_full:
            logger.info(f"需要全量同步的表: {', '.join(need_full)}")
            for t in need_full:
                sync.sync_table_full(t)
        else:
            logger.info("所有表已有同步记录，跳过全量")
        
        # 进入持续增量
        logger.info("转入持续增量同步模式...")
        sync.run_incremental(
            table_pattern=args.table_pattern,
            time_column=args.time_column,
            continuous=True,
            interval=args.interval
        )
    
    elif args.mode == "full":
        sync.run_full(args.table_pattern)
    elif args.mode == "incremental":
        sync.run_incremental(
            table_pattern=args.table_pattern,
            time_column=args.time_column,
            continuous=args.continuous,
            interval=args.interval
        )


if __name__ == "__main__":
    main()
