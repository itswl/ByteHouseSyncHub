#!/usr/bin/env python3
"""
ByteHouse to ByteHouse 数据同步工具
支持全量同步和增量同步，用于跨库或跨实例数据迁移
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
from clickhouse_driver import Client

# 加载 .env 文件
try:
    from dotenv import load_dotenv
    load_dotenv('bytehouse_to_bytehouse.env')
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
                        "title": {"tag": "plain_text", "content": f"{emoji} ByteHouse 同步告警"},
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


# 源 ByteHouse 配置
SOURCE_HOST = os.getenv("SOURCE_BH_HOST", "")
SOURCE_PORT = int(os.getenv("SOURCE_BH_PORT", "19000"))
SOURCE_USER = os.getenv("SOURCE_BH_USER", "bytehouse")
SOURCE_PASSWORD = os.getenv("SOURCE_BH_PASSWORD", "")
SOURCE_DATABASE = os.getenv("SOURCE_BH_DATABASE", "default")
SOURCE_SECURE = os.getenv("SOURCE_BH_SECURE", "true").lower() in ("true", "1", "yes")

# 目标 ByteHouse 配置
TARGET_HOST = os.getenv("TARGET_BH_HOST", "")
TARGET_PORT = int(os.getenv("TARGET_BH_PORT", "19000"))
TARGET_USER = os.getenv("TARGET_BH_USER", "bytehouse")
TARGET_PASSWORD = os.getenv("TARGET_BH_PASSWORD", "")
TARGET_DATABASE = os.getenv("TARGET_BH_DATABASE", "default")
TARGET_SECURE = os.getenv("TARGET_BH_SECURE", "true").lower() in ("true", "1", "yes")

# 同步配置
BATCH_SIZE = int(os.getenv("SYNC_BATCH_SIZE", "10000"))
INCREMENTAL_INTERVAL = int(os.getenv("INCREMENTAL_INTERVAL", "60"))
TABLE_PATTERN = os.getenv("TABLE_PATTERN", "*")


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


class ByteHouseSync:
    def __init__(self):
        self.source_client = None
        self.target_client = None

    def connect_source(self) -> bool:
        """连接源 ByteHouse"""
        try:
            self.source_client = Client(
                host=SOURCE_HOST,
                port=SOURCE_PORT,
                user=SOURCE_USER,
                password=SOURCE_PASSWORD,
                database=SOURCE_DATABASE,
                secure=SOURCE_SECURE
            )
            self.source_client.execute("SELECT 1")
            logger.info(f"✓ 源 ByteHouse 连接成功: {SOURCE_HOST}:{SOURCE_PORT}/{SOURCE_DATABASE}")
            return True
        except Exception as e:
            logger.error(f"✗ 源 ByteHouse 连接失败: {e}")
            return False

    def connect_target(self) -> bool:
        """连接目标 ByteHouse"""
        try:
            self.target_client = Client(
                host=TARGET_HOST,
                port=TARGET_PORT,
                user=TARGET_USER,
                password=TARGET_PASSWORD,
                secure=TARGET_SECURE
            )
            self.target_client.execute("SELECT 1")
            
            # 创建目标数据库
            try:
                self.target_client.execute(f"CREATE DATABASE IF NOT EXISTS `{TARGET_DATABASE}`")
                logger.info(f"✓ 目标数据库 {TARGET_DATABASE} 准备就绪")
            except Exception as e:
                logger.warning(f"创建目标数据库失败: {e}")
            
            self.target_client.execute(f"USE `{TARGET_DATABASE}`")
            
            # 创建同步状态表
            self.target_client.execute("""
                CREATE TABLE IF NOT EXISTS `_sync_state` (
                    `table_name` String,
                    `last_value` String,
                    `last_sync_time` String,
                    `total_synced` Int64,
                    `updated_at` DateTime DEFAULT now()
                ) ENGINE = CnchMergeTree()
                ORDER BY table_name
            """)
            logger.info(f"✓ 目标 ByteHouse 连接成功: {TARGET_HOST}:{TARGET_PORT}/{TARGET_DATABASE}")
            return True
        except Exception as e:
            logger.error(f"✗ 目标 ByteHouse 连接失败: {e}")
            return False

    def get_source_tables(self) -> List[str]:
        """获取源数据库的表列表"""
        result = self.source_client.execute("SHOW TABLES")
        tables = [row[0] for row in result if not row[0].startswith('_')]
        return tables

    def get_table_schema(self, table_name: str) -> str:
        """获取表的创建语句"""
        result = self.source_client.execute(f"SHOW CREATE TABLE `{table_name}`")
        return result[0][0] if result else ""

    def get_table_columns(self, table_name: str) -> List[Tuple[str, str]]:
        """获取表的列信息"""
        result = self.source_client.execute(f"DESCRIBE TABLE `{table_name}`")
        return [(row[0], row[1]) for row in result]

    def create_target_table(self, table_name: str) -> bool:
        """在目标库创建表"""
        try:
            # 获取源表的创建语句
            create_sql = self.get_table_schema(table_name)
            if not create_sql:
                logger.error(f"无法获取表 {table_name} 的创建语句")
                return False
            
            # 修改引擎为 CnchMergeTree（如果需要）
            if "ENGINE = MergeTree" in create_sql and "CnchMergeTree" not in create_sql:
                create_sql = create_sql.replace("ENGINE = MergeTree", "ENGINE = CnchMergeTree")
            
            # 替换数据库名
            create_sql = create_sql.replace(f"`{SOURCE_DATABASE}`.", f"`{TARGET_DATABASE}`.")
            
            # 执行创建
            self.target_client.execute(f"DROP TABLE IF EXISTS `{table_name}`")
            self.target_client.execute(create_sql)
            logger.info(f"✓ 表 {table_name} 创建成功")
            
            # 等待分布式表就绪
            time.sleep(5)
            return True
        except Exception as e:
            logger.error(f"创建表 {table_name} 失败: {e}")
            return False

    def get_sync_state(self, table_name: str) -> Tuple[Optional[str], Optional[str]]:
        """获取同步状态"""
        try:
            result = self.target_client.execute(
                f"SELECT last_value, last_sync_time FROM `_sync_state` WHERE table_name = '{table_name}' ORDER BY updated_at DESC LIMIT 1"
            )
            if result:
                return result[0][0], result[0][1]
        except:
            pass
        return None, None

    def update_sync_state(self, table_name: str, last_value: str, total: int):
        """更新同步状态"""
        try:
            self.target_client.execute(
                f"INSERT INTO `_sync_state` (table_name, last_value, last_sync_time, total_synced) VALUES",
                [(table_name, last_value, datetime.now().isoformat(), total)]
            )
        except Exception as e:
            logger.warning(f"更新同步状态失败: {e}")

    def sync_table_full(self, table_name: str) -> int:
        """全量同步单个表"""
        logger.info(f"开始全量同步表: {table_name}")
        
        # 获取源表数据量
        count_result = self.source_client.execute(f"SELECT count() FROM `{table_name}`")
        total_count = count_result[0][0] if count_result else 0
        logger.info(f"  源表数据量: {total_count:,}")
        
        if total_count == 0:
            logger.info(f"  表 {table_name} 为空，跳过")
            return 0
        
        # 创建目标表
        if not self.create_target_table(table_name):
            return 0
        
        # 获取列信息
        columns = self.get_table_columns(table_name)
        column_names = [col[0] for col in columns]
        columns_str = ", ".join(f"`{c}`" for c in column_names)
        
        # 分批同步
        total_synced = 0
        offset = 0
        start_time = time.time()
        
        while offset < total_count:
            try:
                # 从源表读取数据
                data = self.source_client.execute(
                    f"SELECT {columns_str} FROM `{table_name}` LIMIT {BATCH_SIZE} OFFSET {offset}"
                )
                
                if not data:
                    break
                
                # 写入目标表
                self.target_client.execute(
                    f"INSERT INTO `{table_name}` ({columns_str}) VALUES",
                    data
                )
                
                total_synced += len(data)
                offset += BATCH_SIZE
                
                elapsed = time.time() - start_time
                speed = total_synced / elapsed if elapsed > 0 else 0
                logger.info(f"  进度: {total_synced:,}/{total_count:,} ({100*total_synced/total_count:.1f}%) | 速度: {speed:.0f}/s")
                
            except Exception as e:
                logger.error(f"  同步批次失败: {e}")
                break
        
        # 更新同步状态
        self.update_sync_state(table_name, str(total_synced), total_synced)
        
        elapsed = time.time() - start_time
        logger.info(f"✓ 表 {table_name} 全量同步完成: {total_synced:,} 条，耗时 {elapsed:.1f}s")
        return total_synced

    def sync_table_incremental(self, table_name: str, time_column: str = "created_at") -> int:
        """增量同步单个表"""
        logger.info(f"开始增量同步表: {table_name}")
        
        # 检查目标表是否存在
        try:
            self.target_client.execute(f"SELECT 1 FROM `{table_name}` LIMIT 1")
        except:
            logger.info(f"  目标表不存在，执行全量同步")
            return self.sync_table_full(table_name)
        
        # 获取上次同步状态
        last_value, _ = self.get_sync_state(table_name)
        
        # 获取列信息
        columns = self.get_table_columns(table_name)
        column_names = [col[0] for col in columns]
        columns_str = ", ".join(f"`{c}`" for c in column_names)
        
        # 检查时间列是否存在
        if time_column not in column_names:
            logger.warning(f"  表 {table_name} 没有时间列 {time_column}，跳过增量同步")
            return 0
        
        # 构建查询条件
        if last_value:
            query = f"SELECT {columns_str} FROM `{table_name}` WHERE `{time_column}` > '{last_value}' ORDER BY `{time_column}`"
            logger.info(f"  增量条件: {time_column} > {last_value}")
        else:
            query = f"SELECT {columns_str} FROM `{table_name}` ORDER BY `{time_column}`"
            logger.info(f"  首次增量，同步所有数据")
        
        # 执行查询
        total_synced = 0
        current_last_value = last_value
        
        try:
            data = self.source_client.execute(query)
            
            if not data:
                logger.info(f"  无新增数据")
                return 0
            
            # 分批写入
            for i in range(0, len(data), BATCH_SIZE):
                batch = data[i:i+BATCH_SIZE]
                self.target_client.execute(
                    f"INSERT INTO `{table_name}` ({columns_str}) VALUES",
                    batch
                )
                total_synced += len(batch)
            
            # 获取最新的时间值
            time_col_idx = column_names.index(time_column)
            current_last_value = str(data[-1][time_col_idx])
            
            # 更新同步状态
            self.update_sync_state(table_name, current_last_value, total_synced)
            
        except Exception as e:
            logger.error(f"  增量同步失败: {e}")
        
        logger.info(f"  增量同步完成: {total_synced} 条")
        return total_synced

    def run_full_sync(self, table_pattern: str = "*"):
        """运行全量同步"""
        logger.info("=" * 60)
        logger.info("ByteHouse to ByteHouse 全量同步")
        logger.info("=" * 60)
        logger.info(f"源: {SOURCE_HOST}:{SOURCE_PORT}/{SOURCE_DATABASE}")
        logger.info(f"目标: {TARGET_HOST}:{TARGET_PORT}/{TARGET_DATABASE}")
        logger.info(f"表模式: {table_pattern}")
        
        if not self.connect_source() or not self.connect_target():
            return
        
        # 获取要同步的表
        tables = self.get_source_tables()
        if table_pattern != "*":
            tables = match_tables(tables, table_pattern)
        
        if not tables:
            logger.warning("没有找到要同步的表")
            return
        
        logger.info(f"找到 {len(tables)} 个表: {', '.join(tables)}")
        
        total = 0
        for table in tables:
            total += self.sync_table_full(table)
        
        logger.info("=" * 60)
        logger.info(f"全量同步完成，共同步 {total:,} 条数据")

    def run_incremental(self, table_pattern: str = "*", time_column: str = "created_at",
                       continuous: bool = False, interval: int = 60):
        """运行增量同步"""
        logger.info("=" * 60)
        logger.info("ByteHouse to ByteHouse 增量同步")
        logger.info("=" * 60)
        logger.info(f"源: {SOURCE_HOST}:{SOURCE_PORT}/{SOURCE_DATABASE}")
        logger.info(f"目标: {TARGET_HOST}:{TARGET_PORT}/{TARGET_DATABASE}")
        logger.info(f"表模式: {table_pattern}")
        logger.info(f"时间字段: {time_column}")
        logger.info(f"模式: {'持续同步' if continuous else '单次同步'}")
        
        if not self.connect_source() or not self.connect_target():
            return
        
        tables = self.get_source_tables()
        if table_pattern != "*":
            tables = match_tables(tables, table_pattern)
        
        if not tables:
            logger.warning("没有找到要同步的表")
            return
        
        logger.info(f"找到 {len(tables)} 个表: {', '.join(tables)}")
        
        round_count = 0
        while True:
            round_count += 1
            logger.info(f"\n[第 {round_count} 轮] 开始增量同步 ({datetime.now().strftime('%Y-%m-%d %H:%M:%S')})")
            
            total = 0
            for table in tables:
                total += self.sync_table_incremental(table, time_column)
            
            logger.info(f"[第 {round_count} 轮] 完成，同步 {total:,} 条")
            
            if not continuous:
                break
            
            logger.info(f"等待 {interval} 秒...")
            time.sleep(interval)

    def query_sync_state(self, table_name: str = ""):
        """查询同步状态"""
        if not self.connect_target():
            return
        
        if table_name:
            query = f"SELECT * FROM `_sync_state` WHERE table_name LIKE '%{table_name}%' ORDER BY updated_at DESC"
        else:
            query = "SELECT * FROM `_sync_state` ORDER BY updated_at DESC LIMIT 20"
        
        result = self.target_client.execute(query)
        
        logger.info("=" * 60)
        logger.info("同步状态")
        logger.info("=" * 60)
        
        for row in result:
            logger.info(f"表: {row[0]}, 最后值: {row[1]}, 同步时间: {row[2]}, 总数: {row[3]:,}")


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="ByteHouse to ByteHouse 数据同步工具")
    parser.add_argument("--mode", choices=["full", "incremental", "status"], default="full",
                       help="同步模式: full(全量), incremental(增量), status(查看状态)")
    parser.add_argument("--table-pattern", default=TABLE_PATTERN,
                       help="表名模式，支持逗号分隔，如 'table1,table2,*_log'")
    parser.add_argument("--time-column", default="created_at",
                       help="增量同步的时间字段")
    parser.add_argument("--continuous", action="store_true",
                       help="持续增量同步")
    parser.add_argument("--interval", type=int, default=INCREMENTAL_INTERVAL,
                       help="增量同步间隔（秒）")
    
    args = parser.parse_args()
    
    sync = ByteHouseSync()
    
    if args.mode == "full":
        sync.run_full_sync(args.table_pattern)
    elif args.mode == "incremental":
        sync.run_incremental(
            table_pattern=args.table_pattern,
            time_column=args.time_column,
            continuous=args.continuous,
            interval=args.interval
        )
    elif args.mode == "status":
        sync.query_sync_state(args.table_pattern if args.table_pattern != "*" else "")


if __name__ == "__main__":
    main()
