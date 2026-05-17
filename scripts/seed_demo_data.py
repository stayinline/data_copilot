"""
Seed demo data into ClickHouse for AI Data Copilot demo cases.

Generates data from Jan 1 two years ago to today, so
demo queries like "2024年全年GMV", "昨天数据", "近7天趋势" always work regardless
of when the script is executed.

Usage:
    python scripts/seed_demo_data.py

Tables created (copilot database):
  - Core: orders, users, products, metrics
  - Ops: flink_jobs, kafka_topics, pipeline_logs, alerts
  - Governance: metadata_tables, table_lineage, user_permissions, schema_change_history

After seeding, the script syncs the actual ClickHouse schema + data back to
sql/demo_cases.sql so the LLM always sees the correct table structure.
"""

import random
import re
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

import clickhouse_connect

# ── Connection (read from config.py) ──────────────────────────────────────
sys.path.insert(0, ".")
from config import CLICKHOUSE_HOST, CLICKHOUSE_USER, CLICKHOUSE_PASSWORD, CLICKHOUSE_DATABASE

# ── Paths ─────────────────────────────────────────────────────────────────
_SQL_FILE = Path(__file__).parent.parent / "sql" / "demo_cases.sql"

# ── Dynamic date range: 2024-01-01 to today ─────────────────────────────
# Ensures queries like "2024年全年" and "近7天趋势" both return data
DEMO_END = date.today()
DEMO_START = date(DEMO_END.year - 2, 1, 1)  # Jan 1, two years ago (e.g., 2024-01-01 when today is 2026-05-14)
DEMO_DAYS = (DEMO_END - DEMO_START).days
RCA_ANOMALY_DATE = DEMO_END  # RCA anomaly injected on the last day (today)

# ── Constants ─────────────────────────────────────────────────────────────
REGIONS = ["华东", "华南", "华北", "华中", "西南"]
CATEGORIES = ["电子产品", "服装", "食品", "家居", "美妆"]
STATUSES = ["paid", "shipped", "completed", "refunded"]
STATUS_WEIGHTS = [0.1, 0.15, 0.65, 0.10]
LEVELS = ["普通", "银卡", "金卡", "黑金"]
LEVEL_WEIGHTS = [0.40, 0.30, 0.20, 0.10]
GENDERS = ["男", "女"]
CITIES = {
    "华东": ["上海", "杭州", "南京", "苏州"],
    "华南": ["广州", "深圳", "福州", "厦门"],
    "华北": ["北京", "天津", "石家庄", "济南"],
    "华中": ["武汉", "长沙", "郑州", "合肥"],
    "西南": ["成都", "重庆", "昆明", "贵阳"],
}

METRIC_NAMES = ["GMV", "DAU", "新客数", "转化率", "客单价"]

# Ops constants
FLINK_JOBS = [
    "order_sync_job",
    "user_profile_job",
    "payment_realtime_job",
    "inventory_sync_job",
    "recommendation_feed_job",
    "log_aggregation_job",
]
FLINK_STATUSES = ["RUNNING", "RUNNING", "RUNNING", "RUNNING", "RESTARTING", "FAILED"]
KAFKA_TOPICS = [
    "orders", "user_events", "payments", "inventory_changes",
    "clickstream", "recommendations", "logs_raw",
]
CONSUMER_GROUPS = [
    "order-consumer-group", "order-analytics-group",
    "user-profile-consumer", "payment-consumer",
    "inventory-consumer", "log-consumer",
]
CONSUMER_STATUS_OPTIONS = ["OK", "OK", "OK", "SLOW", "STUCK"]
PIPELINE_NAMES = [
    "sync_order_to_dw", "sync_user_profile", "sync_payment_data",
    "sync_inventory", "sync_clickstream", "sync_recommendation",
]
ERROR_TYPES = [
    "ConnectionTimeout", "NullPointerException", "OutOfMemoryError",
    "DataFormatException", "ConstraintViolation", "SocketTimeout",
]
ALERT_LEVELS = ["CRITICAL", "WARNING", "INFO"]
ALERT_LEVEL_WEIGHTS = [0.15, 0.55, 0.30]
ALERT_SERVICES = [
    "ClickHouse 集群", "Flink 任务", "Kafka 集群", "数据同步 Pipeline",
]

# Governance constants
TABLE_TYPES = ["ODS", "DWD", "DWS", "ADS"]
STORAGE_ENGINES = ["MergeTree", "Kafka", "MySQL", "JDBC"]
LINEAGE_RELATIONS = ["DEPENDS_ON", "PRODUCES", "CONSUMES"]
PERMISSION_TYPES = ["SELECT", "INSERT", "ALL"]
SCHEMA_CHANGE_TYPES = ["ADD COLUMN", "DROP COLUMN", "MODIFY COLUMN", "RENAME TABLE"]

random.seed(42)

# ── Helpers ───────────────────────────────────────────────────────────────
def random_date(start: date, end: date) -> date:
    delta = (end - start).days
    return start + timedelta(days=random.randint(0, delta))


def random_datetime(start: date, end: date) -> datetime:
    delta_days = (end - start).days
    d = start + timedelta(days=random.randint(0, delta_days))
    return datetime(d.year, d.month, d.day, random.randint(0, 23), random.randint(0, 59), random.randint(0, 59))


def weighted_choice(population, weights):
    return random.choices(population, weights=weights, k=1)[0]


# ── Core data generators ──────────────────────────────────────────────────
def generate_users(n=200):
    """Generate users spread across regions and membership levels."""
    users = []
    user_ids = []

    for i in range(1, n + 1):
        uid = f"U{i:04d}"
        user_ids.append(uid)
        region = random.choice(REGIONS)
        city = random.choice(CITIES[region])
        level = weighted_choice(LEVELS, LEVEL_WEIGHTS)
        gender = random.choice(GENDERS)
        age = random.randint(18, 60)
        reg_start = DEMO_START - timedelta(days=365)
        register = random_date(reg_start, DEMO_END)
        login_start = max(register, DEMO_END - timedelta(days=30))
        last_login = random_date(login_start, DEMO_END)
        users.append((uid, register, age, gender, city, level, last_login))

    return users, user_ids


def generate_products(n=50):
    """Generate products across all categories."""
    product_names = {
        "电子产品": ["iPhone 15", "MacBook Pro", "Sony A7M4", "iPad Air", "AirPods Pro",
                     "华为Mate60", "小米14", "Switch游戏机", "机械键盘", "降噪耳机",
                     "智能手表", "平板电脑", "蓝牙音箱", "移动电源", "智能手环"],
        "服装": ["运动外套", "连衣裙", "牛仔裤", "羽绒服", "T恤",
                 "冲锋衣", "休闲裤", "衬衫", "卫衣", "针织衫",
                 "西装外套", "短裤", "风衣", "POLO衫", "棉服"],
        "食品": ["坚果礼盒", "饼干大礼包", "牛肉干", "进口巧克力", "茶叶礼盒",
                 "水果干组合", "咖啡礼盒", "橄榄油", "蜂蜜", "有机大米",
                 "进口牛奶", "速食面组合", "坚果混合装", "红酒", "白酒"],
        "家居": ["收纳柜", "四件套", "懒人沙发", "台灯", "置物架",
                 "窗帘", "地毯", "衣架", "抱枕", "垃圾桶",
                 "鞋柜", "床头柜", "餐桌", "椅子", "衣柜"],
        "美妆": ["口红套装", "粉底液", "面膜套装", "香水", "洗面奶",
                 "眼影盘", "精华液", "防晒霜", "卸妆水", "眼霜",
                 "唇膏", "BB霜", "腮红", "护手霜", "化妆刷"],
    }

    suppliers = [f"供应商{chr(ord('A') + i)}" for i in range(6)]

    products = []
    for i in range(1, n + 1):
        pid = f"P{i:04d}"
        cat = CATEGORIES[(i - 1) % len(CATEGORIES)]
        name = product_names[cat][(i - 1) // len(CATEGORIES) % len(product_names[cat])]
        price = round(random.uniform(29, 19999), 2)
        stock = random.randint(100, 50000)
        supplier = random.choice(suppliers)
        create = random_date(DEMO_START - timedelta(days=365), DEMO_START - timedelta(days=30))
        products.append((pid, name, cat, price, stock, supplier, create))

    return products


def generate_orders(n=1000, user_ids=None, users_data=None):
    """Generate orders spread across the demo date range (last 90 days).

    RCA anomaly injection:
    - DEMO_END (today): 华东 + 电子产品 orders reduced by ~60%
    - High-value users (黑金卡/金卡) in 华东 have even fewer orders
    """
    products_data = generate_products()
    product_map = {p[0]: (p[1], p[2], p[3]) for p in products_data}
    product_ids = list(product_map.keys())

    # Build user_id -> level mapping for high-value targeting
    user_level_map = {}
    if users_data:
        for uid, _, _, _, city, level, _ in users_data:
            user_level_map[uid] = (city, level)

    orders = []
    for i in range(1, n + 1):
        oid = f"ORD{i:05d}"
        uid = random.choice(user_ids)
        pid = random.choice(product_ids)
        odate = random_date(DEMO_START, DEMO_END)
        status = weighted_choice(STATUSES, STATUS_WEIGHTS)
        qty = random.randint(1, 5)
        base_price = product_map[pid][2]
        amount = round(base_price * qty * random.uniform(0.85, 1.15), 2)
        region = random.choice(REGIONS)
        category = product_map[pid][1]

        # ── RCA anomaly injection ──
        # Skip some 华东 + 电子产品 orders on DEMO_END
        if odate == DEMO_END and region == "华东" and category == "电子产品":
            if random.random() < 0.65:  # 65% of such orders are dropped
                # Regenerate with different date (not DEMO_END)
                odate = random_date(DEMO_START, DEMO_END - timedelta(days=1))

        orders.append((oid, uid, pid, odate, amount, qty, status, region, category))

    # Inject a handful of DEMO_END orders for 华东 to show there IS some activity
    # (not zero, just sharply reduced)
    end_east_electronics_count = sum(
        1 for o in orders if o[3] == DEMO_END and o[7] == "华东" and o[8] == "电子产品"
    )
    # Ensure at least a few exist
    while end_east_electronics_count < 3:
        oid = f"ORD{len(orders) + 1:05d}"
        uid = random.choice(user_ids)
        pid = random.choice(product_ids)
        # Make sure product is electronics
        while product_map[pid][1] != "电子产品":
            pid = random.choice(product_ids)
        base_price = product_map[pid][2]
        qty = random.randint(1, 3)
        amount = round(base_price * qty * random.uniform(0.85, 1.15), 2)
        orders.append((oid, uid, pid, DEMO_END, amount, qty, "completed", "华东", "电子产品"))
        end_east_electronics_count += 1

    return orders


def generate_metrics():
    """Generate daily metrics for the demo date range (last 90 days).

    RCA anomaly injection:
    - DEMO_END (today): 华东 GMV drops ~35% below trend
    - Root cause isolated to: 华东 + 电子产品 category (-55%)
    - Other categories in 华东 remain normal
    - Other regions remain normal
    """
    records = []

    # Overall base values (region-level)
    base_values = {
        "GMV": {"华东": 3500000, "华南": 2200000, "华北": 1900000, "华中": 1300000, "西南": 1000000},
        "DAU": {"华东": 45000, "华南": 32000, "华北": 28000, "华中": 19000, "西南": 16000},
        "新客数": {"华东": 1800, "华南": 1300, "华北": 1000, "华中": 700, "西南": 600},
        "转化率": {"华东": 4.2, "华南": 3.8, "华北": 3.5, "华中": 3.1, "西南": 2.9},
        "客单价": {"华东": 280, "华南": 250, "华北": 230, "华中": 210, "西南": 200},
    }

    # GMV contribution weight per category within each region
    # (electronics dominates in 华东)
    gmv_category_weights = {
        "电子产品": 0.35,
        "服装": 0.25,
        "食品": 0.18,
        "家居": 0.12,
        "美妆": 0.10,
    }

    # Iterate over every day in the fixed date range
    current = DEMO_START
    while current <= DEMO_END:
        days_from_start = (current - DEMO_START).days
        dow = current.weekday()
        weekend_factor = 1.15 if dow >= 5 else 1.0
        is_anomaly_day = (current == DEMO_END)

        for region in REGIONS:
            for metric in METRIC_NAMES:
                base = base_values[metric][region]
                noise = random.uniform(-0.12, 0.12)
                trend = 1 + days_from_start * 0.0005
                value = base * (1 + noise) * trend * weekend_factor

                # ── RCA anomaly injection on DEMO_END for 华东 ──
                if is_anomaly_day and region == "华东":
                    if metric == "GMV":
                        value = value * 0.65  # ~35% drop
                    elif metric == "DAU":
                        value = value * 0.88  # mild DAU impact
                    elif metric == "转化率":
                        value = value * 0.70  # conversion rate drops

                if metric in ("GMV",):
                    value = round(value, 2)
                elif metric in ("DAU", "新客数"):
                    value = int(value)
                else:
                    value = round(value, 2)

                # ── Overall record (all categories) — appended first ──
                records.append((current, metric, value, region, ""))

                # ── Category-level breakdown (only for GMV) ──
                if metric == "GMV":
                    cat_gmv_total = 0
                    overall_idx = len(records) - 1  # index of the record we just appended
                    categories_for_region = CATEGORIES[:]
                    for ci, cat in enumerate(categories_for_region):
                        weight = gmv_category_weights[cat]
                        cat_noise = random.uniform(-0.08, 0.08)
                        cat_value = base * (1 + cat_noise) * trend * weekend_factor * weight

                        # RCA anomaly: 华东 + 电子产品 gets hammered
                        if is_anomaly_day and region == "华东" and cat == "电子产品":
                            cat_value = cat_value * 0.45  # -55% drop

                        # Slightly lower for other 华东 categories (minor ripple)
                        if is_anomaly_day and region == "华东" and cat != "电子产品":
                            cat_value = cat_value * 0.92  # -8% minor impact

                        cat_value = round(cat_value, 2)
                        cat_gmv_total += cat_value
                        records.append((current, metric, cat_value, region, cat))

                    # Replace the overall record with the sum of categories
                    records[overall_idx] = (current, metric, round(cat_gmv_total, 2), region, "合计")

        current += timedelta(days=1)

    return records


# ── Ops data generators (角色三：数据运维工程师) ──────────────────────────
def generate_flink_jobs():
    """Generate Flink job status records for demo cases 3.1 and 3.3."""
    jobs = []

    # Ensure order_sync_job has the exact scenario from demo case 3.1
    job_configs = [
        {
            "job_id": "order_sync_job",
            "status": "RUNNING",
            "start_time": DEMO_END - timedelta(hours=3, minutes=42),
            "checkpoint_delay_s": 120,
            "backpressure": "HIGH",
            "last_checkpoint_status": "FAILED",
            "last_checkpoint_time": DEMO_END - timedelta(minutes=15),
            "subtask_count": 8,
            "throughput": 3200,
        },
        {
            "job_id": "user_profile_job",
            "status": "RUNNING",
            "start_time": DEMO_END - timedelta(days=5),
            "checkpoint_delay_s": 12,
            "backpressure": "LOW",
            "last_checkpoint_status": "SUCCESS",
            "last_checkpoint_time": DEMO_END - timedelta(minutes=2),
            "subtask_count": 4,
            "throughput": 8500,
        },
        {
            "job_id": "payment_realtime_job",
            "status": "RUNNING",
            "start_time": DEMO_END - timedelta(days=12),
            "checkpoint_delay_s": 8,
            "backpressure": "LOW",
            "last_checkpoint_status": "SUCCESS",
            "last_checkpoint_time": DEMO_END - timedelta(minutes=1),
            "subtask_count": 6,
            "throughput": 12000,
        },
        {
            "job_id": "inventory_sync_job",
            "status": "RUNNING",
            "start_time": DEMO_END - timedelta(days=3),
            "checkpoint_delay_s": 25,
            "backpressure": "MEDIUM",
            "last_checkpoint_status": "SUCCESS",
            "last_checkpoint_time": DEMO_END - timedelta(minutes=5),
            "subtask_count": 4,
            "throughput": 5600,
        },
        {
            "job_id": "recommendation_feed_job",
            "status": "RESTARTING",
            "start_time": DEMO_END - timedelta(hours=1),
            "checkpoint_delay_s": 0,
            "backpressure": "NONE",
            "last_checkpoint_status": "FAILED",
            "last_checkpoint_time": DEMO_END - timedelta(hours=1),
            "subtask_count": 4,
            "throughput": 0,
        },
        {
            "job_id": "log_aggregation_job",
            "status": "RUNNING",
            "start_time": DEMO_END - timedelta(days=20),
            "checkpoint_delay_s": 5,
            "backpressure": "LOW",
            "last_checkpoint_status": "SUCCESS",
            "last_checkpoint_time": DEMO_END - timedelta(minutes=3),
            "subtask_count": 12,
            "throughput": 45000,
        },
    ]

    for cfg in job_configs:
        start_str = cfg["start_time"].strftime("%Y-%m-%d %H:%M:%S") if hasattr(cfg["start_time"], "strftime") else str(cfg["start_time"])
        jobs.append((
            cfg["job_id"],
            cfg["status"],
            start_str,
            cfg["checkpoint_delay_s"],
            cfg["backpressure"],
            cfg["last_checkpoint_status"],
            cfg["last_checkpoint_time"].strftime("%Y-%m-%d %H:%M:%S") if hasattr(cfg["last_checkpoint_time"], "strftime") else str(cfg["last_checkpoint_time"]),
            cfg["subtask_count"],
            cfg["throughput"],
        ))

    return jobs


def generate_kafka_topics():
    """Generate Kafka topic/consumer lag data for demo case 3.2."""
    records = []

    # Main scenario: orders topic has high lag on order-consumer-group
    topic_configs = [
        {
            "topic": "orders",
            "partition_count": 12,
            "producer_tps": 12500,
            "consumer_group": "order-consumer-group",
            "total_lag": 120000,
            "consumer_status": "SLOW",
            "consumer_tps": 3200,
            "max_partition": 3,
            "max_partition_lag": 85000,
        },
        {
            "topic": "orders",
            "partition_count": 12,
            "producer_tps": 12500,
            "consumer_group": "order-analytics-group",
            "total_lag": 5200,
            "consumer_status": "OK",
            "consumer_tps": 11800,
            "max_partition": 7,
            "max_partition_lag": 1200,
        },
        {
            "topic": "user_events",
            "partition_count": 8,
            "producer_tps": 8000,
            "consumer_group": "user-profile-consumer",
            "total_lag": 3200,
            "consumer_status": "OK",
            "consumer_tps": 7800,
            "max_partition": 2,
            "max_partition_lag": 900,
        },
        {
            "topic": "payments",
            "partition_count": 6,
            "producer_tps": 5000,
            "consumer_group": "payment-consumer",
            "total_lag": 1500,
            "consumer_status": "OK",
            "consumer_tps": 4900,
            "max_partition": 1,
            "max_partition_lag": 400,
        },
        {
            "topic": "inventory_changes",
            "partition_count": 6,
            "producer_tps": 3000,
            "consumer_group": "inventory-consumer",
            "total_lag": 8500,
            "consumer_status": "SLOW",
            "consumer_tps": 1200,
            "max_partition": 4,
            "max_partition_lag": 6200,
        },
        {
            "topic": "clickstream",
            "partition_count": 16,
            "producer_tps": 50000,
            "consumer_group": "log-consumer",
            "total_lag": 15000,
            "consumer_status": "OK",
            "consumer_tps": 48500,
            "max_partition": 9,
            "max_partition_lag": 2000,
        },
    ]

    for cfg in topic_configs:
        records.append((
            cfg["topic"],
            cfg["partition_count"],
            cfg["producer_tps"],
            cfg["consumer_group"],
            cfg["total_lag"],
            cfg["consumer_status"],
            cfg["consumer_tps"],
            f"partition-{cfg['max_partition']}",
            cfg["max_partition_lag"],
            DEMO_END,
        ))

    return records


def generate_pipeline_logs(days=7):
    """Generate pipeline error logs including a cascading failure scenario."""
    logs = []
    log_id = 1

    # Daily normal errors (days 7 to 2 before DEMO_END)
    for d in range(days, 1, -1):
        dt = DEMO_END - timedelta(days=d)
        for _ in range(random.randint(1, 3)):
            error_type = random.choice(ERROR_TYPES[:3])
            logs.append((
                f"LOG{log_id:05d}",
                random.choice(PIPELINE_NAMES),
                error_type,
                random.choice(["ERROR", "WARN"]),
                random_datetime(dt, dt),
                "Auto-recovered after retry",
            ))
            log_id += 1

    # Day before DEMO_END: normal day, a few minor errors
    dt_before = DEMO_END - timedelta(days=1)
    for _ in range(3):
        logs.append((
            f"LOG{log_id:05d}",
            random.choice(PIPELINE_NAMES),
            random.choice(ERROR_TYPES[:3]),
            "ERROR",
            random_datetime(dt_before, dt_before),
            "Auto-recovered after retry",
        ))
        log_id += 1

    # DEMO_END: cascading failure scenario
    # Phase 1: Kafka broker failure → Pipeline timeouts (14:45-15:30)
    phase1_errors = [
        ("sync_order_to_dw",  "ConnectionTimeout",  "ERROR", "14:45", "Kafka broker kafka-broker-03:9092 unreachable, retry 1/3"),
        ("sync_order_to_dw",  "ConnectionTimeout",  "ERROR", "14:50", "Kafka broker kafka-broker-03:9092 unreachable, retry 2/3"),
        ("sync_order_to_dw",  "ConnectionTimeout",  "ERROR", "15:00", "Kafka broker kafka-broker-03:9092 unreachable, all retries exhausted"),
        ("sync_order_to_dw",  "ConnectionTimeout",  "ERROR", "15:05", "Connection to dw-clickhouse-01 timed out after 30s"),
        ("sync_order_to_dw",  "ConnectionTimeout",  "ERROR", "15:10", "Connection to dw-clickhouse-01 timed out after 30s"),
        ("sync_payment_data", "ConnectionTimeout",  "ERROR", "15:15", "Kafka broker kafka-broker-03:9092 unreachable, topic: payments"),
        ("sync_payment_data", "ConnectionTimeout",  "ERROR", "15:20", "Kafka broker kafka-broker-03:9092 unreachable, all retries exhausted"),
        ("sync_payment_data", "SocketTimeout",      "ERROR", "15:25", "Read timeout from Kafka consumer, consumer_group=payment-consumer"),
        ("sync_user_profile", "ConnectionTimeout",  "ERROR", "15:30", "Kafka broker kafka-broker-03:9092 unreachable, topic: user_events"),
    ]

    # Phase 2: Multiple Pipelines failing, Flink backpressure (15:30-16:15)
    phase2_errors = [
        ("sync_order_to_dw",  "ConnectionTimeout",  "ERROR", "15:30", "Connection to dw-clickhouse-01 timed out after 30s"),
        ("sync_order_to_dw",  "ConnectionTimeout",  "ERROR", "15:40", "Connection to dw-clickhouse-01 timed out after 30s"),
        ("sync_order_to_dw",  "ConnectionTimeout",  "ERROR", "15:50", "Connection to dw-clickhouse-01 timed out after 30s"),
        ("sync_user_profile", "ConnectionTimeout",  "ERROR", "15:40", "Kafka consumer stuck, no data received for 600s"),
        ("sync_user_profile", "NullPointerException", "ERROR", "15:45", "Null reference in UserMapper.mapFields(), no upstream data"),
        ("sync_payment_data", "ConnectionTimeout",  "ERROR", "15:45", "Connection to dw-clickhouse-01 timed out after 30s"),
        ("sync_payment_data", "DataFormatException", "ERROR", "15:50", "Malformed JSON in payments topic, offset=8823456"),
        ("sync_order_to_dw",  "ConnectionTimeout",  "ERROR", "16:00", "Connection to dw-clickhouse-01 timed out after 30s, CPU 95%"),
        ("sync_order_to_dw",  "ConnectionTimeout",  "ERROR", "16:05", "Connection to dw-clickhouse-01 timed out after 30s, CPU 97%"),
        ("sync_order_to_dw",  "ConnectionTimeout",  "ERROR", "16:10", "Connection to dw-clickhouse-01 timed out after 30s, CPU 99%"),
        ("sync_clickstream",  "ConnectionTimeout",  "ERROR", "16:00", "Kafka broker kafka-broker-03:9092 unreachable, topic: clickstream"),
        ("sync_clickstream",  "SocketTimeout",      "ERROR", "16:05", "Consumer lag 50000+ on clickstream, partition-7"),
        ("sync_inventory",    "ConnectionTimeout",  "ERROR", "16:10", "Kafka broker kafka-broker-03:9092 unreachable, topic: inventory_changes"),
        ("sync_inventory",    "SocketTimeout",      "ERROR", "16:15", "Consumer lag 12000+ on inventory_changes, partition-4"),
    ]

    # Phase 3: Recovery (16:30-17:30)
    phase3_errors = [
        ("sync_order_to_dw",  "ConnectionTimeout",  "ERROR", "16:15", "Connection to dw-clickhouse-01 timed out after 30s, CPU 98%"),
        ("sync_order_to_dw",  "ConnectionTimeout",  "ERROR", "16:20", "Connection to dw-clickhouse-01 timed out after 30s"),
        ("sync_order_to_dw",  "ConnectionTimeout",  "ERROR", "16:25", "Partial recovery, dw-clickhouse-01 CPU 85%"),
        ("sync_order_to_dw",  "ConnectionTimeout",  "ERROR", "16:30", "Partial recovery, dw-clickhouse-01 CPU 72%"),
        ("sync_payment_data", "ConnectionTimeout",  "ERROR", "16:30", "Partial recovery, Kafka consumer reconnected"),
        ("sync_user_profile", "ConnectionTimeout",  "ERROR", "16:35", "Partial recovery, Kafka consumer reconnected"),
        ("sync_order_to_dw",  "DataFormatException", "WARN",  "16:45", "Resuming from offset 99234, processing backlog"),
        ("sync_payment_data", "SocketTimeout",      "WARN",  "16:50", "Processing backlog, estimated catch-up time 15min"),
        ("sync_user_profile", "DataFormatException", "WARN",  "16:55", "Resuming from offset 44123, processing backlog"),
        ("sync_order_to_dw",  "ConnectionTimeout",  "WARN",  "17:00", "Backlog cleared, pipeline running normally"),
        ("sync_payment_data", "ConnectionTimeout",  "WARN",  "17:05", "Backlog cleared, pipeline running normally"),
        ("sync_user_profile", "ConnectionTimeout",  "WARN",  "17:10", "Backlog cleared, pipeline running normally"),
        ("sync_clickstream",  "SocketTimeout",      "WARN",  "17:15", "Backlog cleared, pipeline running normally"),
        ("sync_inventory",    "ConnectionTimeout",  "WARN",  "17:20", "Backlog cleared, pipeline running normally"),
    ]

    # Flink job failure logs during the incident
    flink_logs = [
        ("order_sync_job_flink",   "Backpressure",     "ERROR",   "15:45", "Flink order_sync_job backpressure HIGH, subtask-3 blocked"),
        ("order_sync_job_flink",   "CheckpointFailed", "ERROR",   "15:50", "Checkpoint expired before completion, delay=60s"),
        ("order_sync_job_flink",   "CheckpointFailed", "ERROR",   "15:55", "Checkpoint expired before completion, delay=90s"),
        ("order_sync_job_flink",   "CheckpointFailed", "CRITICAL","16:00", "Checkpoint failed after 3 retries, delay=120s"),
        ("order_sync_job_flink",   "Backpressure",     "CRITICAL","16:05", "Flink order_sync_job backpressure HIGH, all subtasks blocked"),
        ("order_sync_job_flink",   "Restarting",       "ERROR",   "16:20", "Flink order_sync_job restarted by restart-strategy"),
        ("order_sync_job_flink",   "CheckpointFailed", "ERROR",   "16:25", "Checkpoint failed, downstream dw-clickhouse-01 unreachable"),
        ("order_sync_job_flink",   "Recovery",         "INFO",    "16:40", "Flink order_sync_job recovering, checkpoint succeeded"),
        ("order_sync_job_flink",   "Recovery",         "INFO",    "16:50", "Flink order_sync_job backpressure LOW, processing backlog"),
        ("order_sync_job_flink",   "Recovery",         "INFO",    "17:05", "Flink order_sync_job backlog cleared, running normally"),
    ]

    for pipeline, error, level, time_str, msg in phase1_errors + phase2_errors + phase3_errors + flink_logs:
        h, m = time_str.split(":")
        logs.append((
            f"LOG{log_id:05d}",
            pipeline,
            error,
            level,
            datetime(DEMO_END.year, DEMO_END.month, DEMO_END.day, int(h), int(m)),
            msg,
        ))
        log_id += 1

    return logs


def generate_alerts(days=7):
    """Generate alert records including a cascading failure scenario."""
    alerts = []
    alert_id = 1

    # Normal alerts from days before DEMO_END (days 7 to 2)
    normal_alerts = [
        ("Kafka 集群",     "Consumer Lag 超过阈值", "INFO",    6),
        ("Flink 任务",     "Checkpoint 超时",       "INFO",    5),
        ("ClickHouse 集群", "CPU 使用率超过 90%",   "WARNING", 4),
        ("数据同步 Pipeline","同步延迟超过阈值",     "INFO",    3),
        ("Kafka 集群",     "TPS 异常下降",          "INFO",    3),
        ("Flink 任务",     "反序列化失败",          "WARNING", 2),
        ("数据同步 Pipeline","同步延迟超过阈值",     "WARNING", 2),
        ("ClickHouse 集群", "磁盘使用率超过 80%",   "WARNING", 1),
    ]

    for service, alert_type, level, days_ago in normal_alerts:
        dt = DEMO_END - timedelta(days=days_ago)
        alerts.append((
            f"ALT{alert_id:05d}",
            service,
            alert_type,
            level,
            random_datetime(dt, dt),
            "resolved",
        ))
        alert_id += 1

    # DEMO_END: cascading failure alert timeline
    cascade_alerts = [
        # Kafka broker failure (14:45)
        ("Kafka 集群",     "Broker 不可用",            "CRITICAL", "14:45", "active"),
        ("Kafka 集群",     "Broker 不可用",            "CRITICAL", "14:50", "active"),
        ("Kafka 集群",     "Consumer Lag 超过阈值",    "WARNING",  "14:55", "active"),
        ("Kafka 集群",     "分区不均衡",               "WARNING",  "15:00", "active"),
        # Pipeline affected (15:00)
        ("数据同步 Pipeline","同步延迟超过阈值",        "WARNING",  "15:05", "active"),
        ("数据同步 Pipeline","同步延迟超过阈值",        "WARNING",  "15:15", "active"),
        ("数据同步 Pipeline","写入失败",               "CRITICAL", "15:25", "active"),
        ("数据同步 Pipeline","任务频繁重启",            "WARNING",  "15:30", "active"),
        # Flink backpressure (15:45)
        ("Flink 任务",     "Backpressure HIGH",        "WARNING",  "15:45", "active"),
        ("Flink 任务",     "Checkpoint 超时",          "WARNING",  "15:50", "active"),
        ("Flink 任务",     "Checkpoint 超时",          "CRITICAL", "16:00", "active"),
        ("Flink 任务",     "Backpressure HIGH",        "CRITICAL", "16:05", "active"),
        ("Flink 任务",     "TaskManager 宕机",         "CRITICAL", "16:20", "active"),
        # ClickHouse CPU spike (16:00)
        ("ClickHouse 集群", "CPU 使用率超过 90%",      "CRITICAL", "16:00", "active"),
        ("ClickHouse 集群", "CPU 使用率超过 90%",      "CRITICAL", "16:05", "active"),
        ("ClickHouse 集群", "查询超时",                "CRITICAL", "16:10", "active"),
        ("ClickHouse 集群", "连接数过多",              "WARNING",  "16:15", "active"),
        ("ClickHouse 集群", "CPU 使用率超过 90%",      "WARNING",  "16:20", "active"),
        ("ClickHouse 集群", "查询超时",                "WARNING",  "16:25", "active"),
        # Recovery (16:30+)
        ("Kafka 集群",     "Broker 不可用",            "CRITICAL", "16:30", "resolved"),
        ("Kafka 集群",     "Consumer Lag 超过阈值",    "WARNING",  "16:35", "resolved"),
        ("数据同步 Pipeline","同步延迟超过阈值",       "WARNING",  "16:40", "resolved"),
        ("数据同步 Pipeline","写入失败",               "CRITICAL", "16:45", "resolved"),
        ("Flink 任务",     "Backpressure HIGH",        "WARNING",  "16:50", "resolved"),
        ("Flink 任务",     "Checkpoint 超时",          "WARNING",  "16:55", "resolved"),
        ("ClickHouse 集群", "CPU 使用率超过 90%",      "WARNING",  "17:00", "resolved"),
        ("数据同步 Pipeline","任务频繁重启",           "WARNING",  "17:05", "resolved"),
        ("Kafka 集群",     "分区不均衡",               "WARNING",  "17:10", "resolved"),
        # Post-incident cleanup
        ("数据同步 Pipeline","同步延迟超过阈值",        "INFO",     "18:00", "resolved"),
        ("Flink 任务",     "Checkpoint 超时",          "INFO",     "19:00", "resolved"),
    ]

    for service, alert_type, level, time_str, status in cascade_alerts:
        h, m = time_str.split(":")
        alerts.append((
            f"ALT{alert_id:05d}",
            service,
            alert_type,
            level,
            datetime(DEMO_END.year, DEMO_END.month, DEMO_END.day, int(h), int(m)),
            status,
        ))
        alert_id += 1

    return alerts


# ── Governance data generators (角色四：数据管理员) ───────────────────────
def generate_metadata_tables():
    """Generate table metadata for demo cases 4.1 and 4.4."""
    tables = [
        # ODS layer
        ("orders", "ODS", "订单原始数据表", "MergeTree", "MySQL binlog 同步", 230000000, "2024-06-05"),
        ("users", "ODS", "用户信息表", "MergeTree", "MySQL binlog 同步", 5000000, "2024-06-04"),
        ("products", "ODS", "商品信息表", "MergeTree", "MySQL binlog 同步", 500000, "2024-06-03"),
        # DWD layer
        ("dwd_order_detail", "DWD", "订单明细宽表", "MergeTree", "ETL: orders JOIN users JOIN products", 220000000, "2024-06-05"),
        ("dwd_order_status_log", "DWD", "订单状态变更日志", "MergeTree", "ETL: orders.status 变更记录", 450000000, "2024-06-05"),
        ("dwd_user_info", "DWD", "用户信息宽表", "MergeTree", "ETL: users + 标签计算", 5000000, "2024-06-04"),
        # DWS layer
        ("dws_order_daily", "DWS", "订单日汇总表", "MergeTree", "ETL: dwd_order_detail + dwd_user_info 聚合", 365000, "2024-06-05"),
        ("dws_user_order_stats", "DWS", "用户订单统计表", "MergeTree", "ETL: dwd_order_detail 按用户聚合", 5000000, "2024-06-04"),
        # ADS layer
        ("ads_exec_dashboard", "ADS", "高管看板数据表", "MergeTree", "ETL: dws_order_daily + dws_user_order_stats", 365, "2024-06-05"),
        ("ads_sales_daily_report", "ADS", "销售日报数据表", "MergeTree", "ETL: dws_order_daily 聚合", 365, "2024-06-05"),
    ]
    return tables


def generate_table_lineage():
    """Generate lineage relationships for demo case 4.2."""
    # (source_table, target_table, relation_type, column_mapping)
    lineages = [
        ("orders", "dwd_order_detail", "DEPENDS_ON", "order_id, user_id, product_id, amount, status"),
        ("users", "dwd_order_detail", "DEPENDS_ON", "user_id → user_info"),
        ("products", "dwd_order_detail", "DEPENDS_ON", "product_id → product_info"),
        ("orders", "dwd_order_status_log", "DEPENDS_ON", "order_id, status (变更追踪)"),
        ("users", "dwd_user_info", "DEPENDS_ON", "user_id, level, city, age"),
        ("dwd_order_detail", "dws_order_daily", "DEPENDS_ON", "order_date, region, category → 聚合"),
        ("dwd_user_info", "dws_order_daily", "DEPENDS_ON", "user_id → buyer_count"),
        ("dwd_order_detail", "dws_user_order_stats", "DEPENDS_ON", "user_id → 订单统计"),
        ("dws_order_daily", "ads_exec_dashboard", "DEPENDS_ON", "全量指标"),
        ("dws_user_order_stats", "ads_exec_dashboard", "DEPENDS_ON", "用户指标"),
        ("dws_order_daily", "ads_sales_daily_report", "DEPENDS_ON", "销售指标聚合"),
        ("orders", "metrics", "DEPENDS_ON", "amount, region, category → 指标计算"),
    ]
    return lineages


def generate_user_permissions():
    """Generate user permission records for demo case 4.3."""
    # (user_id, role, table_name, permission_type, granted_date)
    users_perms = [
        ("zhangsan", "Analyst", "orders", "SELECT", "2024-01-15"),
        ("zhangsan", "Analyst", "users", "SELECT", "2024-01-15"),
        ("zhangsan", "Analyst", "metrics", "SELECT", "2024-03-20"),
        ("lisi", "Engineer", "orders", "SELECT", "2024-02-10"),
        ("lisi", "Engineer", "users", "SELECT", "2024-02-10"),
        ("lisi", "Engineer", "products", "SELECT", "2024-02-10"),
        ("lisi", "Engineer", "metrics", "SELECT", "2024-02-10"),
        ("lisi", "Engineer", "dwd_order_detail", "SELECT", "2024-04-15"),
        ("wangwu", "Admin", "*", "ALL", "2024-01-01"),
        ("zhaoliu", "Analyst", "orders", "SELECT", "2024-03-01"),
        ("zhaoliu", "Analyst", "metrics", "SELECT", "2024-03-01"),
        ("zhaoliu", "Analyst", "ads_exec_dashboard", "SELECT", "2024-05-10"),
    ]
    return users_perms


def generate_schema_change_history():
    """Generate schema change records for demo case 4.4.

    Dates relative to DEMO_END.
    """
    changes = [
        ("orders", "ADD COLUMN", "新增 discount_amount Decimal(18,2) 字段", "李工", DEMO_END - timedelta(days=3)),
        ("orders", "MODIFY COLUMN", "region 字段类型从 String 改为 LowCardinality(String)", "王工", DEMO_END - timedelta(days=8)),
        ("orders", "DROP COLUMN", "移除 remark 字段（已废弃）", "赵姐", DEMO_END - timedelta(days=21)),
        ("users", "ADD COLUMN", "新增 phone_masked String 字段（脱敏手机号）", "李工", DEMO_END - timedelta(days=12)),
        ("products", "MODIFY COLUMN", "price 字段精度从 Decimal(10,2) 改为 Decimal(18,2)", "王工", DEMO_END - timedelta(days=30)),
        ("dwd_order_detail", "ADD COLUMN", "新增 discount_amount 字段同步", "李工", DEMO_END - timedelta(days=3)),
        ("dws_order_daily", "MODIFY COLUMN", "gmv 字段精度从 Decimal(15,2) 改为 Decimal(18,2)", "赵姐", DEMO_END - timedelta(days=25)),
    ]
    return changes


# ── DDL definitions ──────────────────────────────────────────────────────
CORE_DDL = [
    """
    CREATE TABLE IF NOT EXISTS orders
    (
        order_id    String,
        user_id     String,
        product_id  String,
        order_date  Date,
        amount      Decimal(18, 2),
        quantity    Int32,
        status      String,
        region      String,
        category    String
    ) ENGINE = MergeTree()
    ORDER BY (order_date, region, category)
    """,
    """
    CREATE TABLE IF NOT EXISTS users
    (
        user_id       String,
        register_date Date,
        age           Int32,
        gender        String,
        city          String,
        level         String,
        last_login    Date
    ) ENGINE = MergeTree()
    ORDER BY user_id
    """,
    """
    CREATE TABLE IF NOT EXISTS products
    (
        product_id   String,
        product_name String,
        category     String,
        price        Decimal(18, 2),
        stock        Int32,
        supplier     String,
        create_date  Date
    ) ENGINE = MergeTree()
    ORDER BY product_id
    """,
    """
    CREATE TABLE IF NOT EXISTS metrics
    (
        metric_date  Date,
        metric_name  String,
        metric_value Decimal(18, 2),
        region       String,
        category     String
    ) ENGINE = MergeTree()
    ORDER BY (metric_date, metric_name, region)
    """,
]

OPS_DDL = [
    """
    CREATE TABLE IF NOT EXISTS flink_jobs
    (
        job_id                 String,
        status                 String,
        start_time             String,
        checkpoint_delay_s     Int32,
        backpressure           String,
        last_checkpoint_status String,
        last_checkpoint_time   String,
        subtask_count          Int32,
        throughput             Int64
    ) ENGINE = MergeTree()
    ORDER BY job_id
    """,
    """
    CREATE TABLE IF NOT EXISTS kafka_topics
    (
        topic            String,
        partition_count  Int32,
        producer_tps     Int64,
        consumer_group   String,
        total_lag        Int64,
        consumer_status  String,
        consumer_tps     Int64,
        max_partition    String,
        max_partition_lag Int64,
        check_date       Date
    ) ENGINE = MergeTree()
    ORDER BY (topic, consumer_group, check_date)
    """,
    """
    CREATE TABLE IF NOT EXISTS pipeline_logs
    (
        log_id      String,
        pipeline    String,
        error_type  String,
        log_level   String,
        log_time    DateTime,
        message     String
    ) ENGINE = MergeTree()
    ORDER BY (pipeline, log_time)
    """,
    """
    CREATE TABLE IF NOT EXISTS alerts
    (
        alert_id     String,
        service      String,
        alert_type   String,
        level        String,
        alert_time   DateTime,
        status       String
    ) ENGINE = MergeTree()
    ORDER BY (service, alert_time)
    """,
]

GOVERNANCE_DDL = [
    """
    CREATE TABLE IF NOT EXISTS metadata_tables
    (
        table_name     String,
        layer          String,       -- ODS / DWD / DWS / ADS
        description    String,
        storage_engine String,
        data_source    String,
        row_count      Int64,
        last_updated   String
    ) ENGINE = MergeTree()
    ORDER BY table_name
    """,
    """
    CREATE TABLE IF NOT EXISTS table_lineage
    (
        source_table   String,
        target_table   String,
        relation_type  String,       -- DEPENDS_ON / PRODUCES / CONSUMES
        column_mapping String
    ) ENGINE = MergeTree()
    ORDER BY (source_table, target_table)
    """,
    """
    CREATE TABLE IF NOT EXISTS user_permissions
    (
        user_id         String,
        role            String,
        table_name      String,
        permission_type String,
        granted_date    String
    ) ENGINE = MergeTree()
    ORDER BY (user_id, table_name)
    """,
    """
    CREATE TABLE IF NOT EXISTS schema_change_history
    (
        table_name    String,
        change_type   String,       -- ADD COLUMN / DROP COLUMN / MODIFY COLUMN
        description   String,
        operator      String,
        change_date   Date
    ) ENGINE = MergeTree()
    ORDER BY (table_name, change_date)
    """,
]

ALL_DDL = CORE_DDL + OPS_DDL + GOVERNANCE_DDL

# Tables that have demo data (used by schema_loader)
DEMO_TABLES = [
    "orders", "users", "products", "metrics",
    "flink_jobs", "kafka_topics", "pipeline_logs", "alerts",
    "metadata_tables", "table_lineage", "user_permissions", "schema_change_history",
]

# Group tables by category for output sections
CORE_TABLES = ["orders", "users", "products", "metrics"]
OPS_TABLES = ["flink_jobs", "kafka_topics", "pipeline_logs", "alerts"]
GOVERNANCE_TABLES = ["metadata_tables", "table_lineage", "user_permissions", "schema_change_history"]


# ── SQL file sync ─────────────────────────────────────────────────────────
def _format_value(val) -> str:
    """Format a Python value as a SQL literal."""
    if val is None:
        return "NULL"
    if isinstance(val, (int,)):
        return str(val)
    if isinstance(val, (float,)):
        return f"{val:.2f}"
    s = str(val).replace("'", "\\'")
    return f"'{s}'"


def _fetch_ddl(client, table: str) -> str:
    """Get SHOW CREATE TABLE output from ClickHouse, add IF NOT EXISTS."""
    ddl = client.command(f"SHOW CREATE TABLE {table}")
    # Convert literal \\n to real newlines for readability
    ddl = ddl.replace("\\n", "\n")
    ddl = ddl.strip().rstrip(";")
    # Add IF NOT EXISTS after CREATE TABLE
    ddl = re.sub(r"^CREATE TABLE\b", "CREATE TABLE IF NOT EXISTS", ddl, flags=re.IGNORECASE)
    return ddl + ";"


def _fetch_insert_statements(client, table: str, max_rows: int = 200) -> list[str]:
    """Fetch data from ClickHouse and generate INSERT ... VALUES statements."""
    result = client.query(f"SELECT * FROM {table} LIMIT {max_rows}")
    columns = [cd[0] for cd in result.column_names]
    rows = list(result.result_rows)

    if not rows:
        return []

    # Group rows into chunks of 10 for readability
    chunk_size = 10
    statements = []

    for i in range(0, len(rows), chunk_size):
        chunk = rows[i:i + chunk_size]
        value_strs = []
        for row in chunk:
            values = ", ".join(_format_value(v) for v in row)
            value_strs.append(f"({values})")
        stmt = f"INSERT INTO {table} VALUES\n" + ",\n".join(value_strs) + ";"
        statements.append(stmt)

    return statements


def sync_sql_to_file(client):
    """
    Read actual DDL from ClickHouse and INSERT statements from seeded data,
    then write a new sql/demo_cases.sql file.

    Preserves the existing "三、演示案例查询 SQL" section by extracting it
    from the old file before overwriting.
    """
    print("\n=== Syncing schema to sql/demo_cases.sql ===")

    # Preserve the demo query section from the existing file
    demo_queries = ""
    if _SQL_FILE.exists():
        content = _SQL_FILE.read_text(encoding="utf-8")
        match = re.search(r"(/=\s*=+.*?三、演示案例查询 SQL.*?=+\s*/)", content, re.DOTALL)
        if match:
            demo_queries = content[match.start():]
        else:
            # Fallback: find everything after the last INSERT INTO block
            lines = content.split("\n")
            query_start = None
            for idx, line in enumerate(lines):
                if "演示案例查询" in line:
                    query_start = idx
                    break
            if query_start is not None:
                # Go back to find the section comment
                demo_queries = content
            else:
                demo_queries = ""

    # Build new file content
    lines = []
    lines.append("-- " + "=" * 78)
    lines.append("-- AI Data Copilot 演示案例 SQL")
    lines.append("-- 包含：建表语句、演示数据、案例查询 SQL")
    lines.append("-- 自动生成于 seed_demo_data.py — 请勿手动修改 DDL/DML 部分")
    lines.append("-- " + "=" * 78)
    lines.append("")

    # Section headers for each category
    category_headers = [
        ("CORE", "一、数据表结构（DDL）"),
        ("OPS", "运维角色表（数据运维工程师使用）"),
        ("GOVERNANCE", "数据治理角色表（数据管理员使用）"),
    ]

    section_groups = {
        "CORE": CORE_TABLES,
        "OPS": OPS_TABLES,
        "GOVERNANCE": GOVERNANCE_TABLES,
    }

    for cat_key, cat_title in category_headers:
        lines.append(f"-- " + "=" * 78)
        if cat_key == "CORE":
            lines.append("-- " + cat_title)
        else:
            lines.append(f"-- {cat_title}")
        lines.append(f"-- " + "=" * 78)
        lines.append("")

        for table in section_groups[cat_key]:
            # DDL
            ddl = _fetch_ddl(client, table)
            # Add comment header for first table in each section
            if cat_key == "CORE":
                idx = CORE_TABLES.index(table) + 1
                table_comments = {
                    "orders": "订单表", "users": "用户表",
                    "products": "商品表", "metrics": "指标表",
                }
                lines.append(f"-- {idx}. {table_comments.get(table, table)}")
            lines.append(ddl)
            lines.append("")

    lines.append(f"-- " + "=" * 78)
    lines.append("-- 二、演示数据（DML）")
    lines.append(f"-- " + "=" * 78)
    lines.append("")

    for cat_key in ["CORE", "OPS", "GOVERNANCE"]:
        for table in section_groups[cat_key]:
            inserts = _fetch_insert_statements(client, table, max_rows=200)
            if inserts:
                table_labels = {
                    "orders": "订单数据", "users": "用户数据",
                    "products": "商品数据", "metrics": "指标数据",
                    "flink_jobs": "Flink 作业数据",
                    "kafka_topics": "Kafka Topic 运维数据",
                    "pipeline_logs": "Pipeline 日志数据",
                    "alerts": "告警记录数据",
                    "metadata_tables": "表元数据",
                    "table_lineage": "表血缘关系数据",
                    "user_permissions": "用户权限数据",
                    "schema_change_history": "Schema 变更历史数据",
                }
                label = table_labels.get(table, f"{table} 数据")
                lines.append(f"-- {label}")
                for stmt in inserts:
                    lines.append(stmt)
                lines.append("")

    # Append the demo query section
    lines.append(f"-- " + "=" * 78)
    lines.append("-- 三、演示案例查询 SQL")
    lines.append(f"-- " + "=" * 78)
    lines.append("")

    if demo_queries:
        # Extract just the query part (after section header)
        if "三、演示案例查询" in demo_queries:
            parts = demo_queries.split("三、演示案例查询", 1)
            lines.append(f"-- 三、演示案例查询 SQL")
            lines.append(parts[1].rstrip())
        else:
            lines.append(demo_queries.rstrip())

    lines.append("")

    new_content = "\n".join(lines)
    _SQL_FILE.write_text(new_content, encoding="utf-8")
    print(f"  Synced {len(DEMO_TABLES)} tables to {_SQL_FILE}")


# ── Main ──────────────────────────────────────────────────────────────────
def main():
    CH_HTTP_PORT = 8123
    print(f"Connecting to ClickHouse at {CLICKHOUSE_HOST}:{CH_HTTP_PORT} ...")
    client = clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CH_HTTP_PORT,
        user=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DATABASE,
    )

    version = client.command("SELECT version()")
    print(f"Connected. ClickHouse version: {version}")

    # ── 1. Create tables ─────────────────────────────────────────────────
    print("\n=== Creating tables ===")

    for ddl_sql in ALL_DDL:
        table = re.search(r"CREATE TABLE (?:IF NOT EXISTS\s+)?(\w+)", ddl_sql).group(1)
        client.command(f"DROP TABLE IF EXISTS {table}")
        client.command(ddl_sql)
        print(f"  Created table: {table}")

    # ── 2. Generate data ─────────────────────────────────────────────────
    print("\n=== Generating data ===")
    print(f"  Date range: {DEMO_START} to {DEMO_END} ({DEMO_DAYS + 1} days)")
    print(f"  RCA anomaly date: {RCA_ANOMALY_DATE}")

    # Core data
    users, user_ids = generate_users(n=200)
    print(f"  users:       {len(users)} rows")

    products = generate_products(n=50)
    print(f"  products:    {len(products)} rows")

    orders = generate_orders(n=1000, user_ids=user_ids, users_data=users)
    print(f"  orders:      {len(orders)} rows ({DEMO_START} to {DEMO_END}, RCA anomaly on {DEMO_END})")

    metrics = generate_metrics()
    print(f"  metrics:     {len(metrics)} rows ({DEMO_DAYS + 1} days, 5 regions, 5 metrics)")

    # Ops data
    flink_jobs = generate_flink_jobs()
    print(f"  flink_jobs:  {len(flink_jobs)} rows")

    kafka_topics = generate_kafka_topics()
    print(f"  kafka_topics: {len(kafka_topics)} rows")

    pipeline_logs = generate_pipeline_logs(days=7)
    print(f"  pipeline_logs: {len(pipeline_logs)} rows (last 7 days of demo range)")

    alerts = generate_alerts(days=7)
    print(f"  alerts:      {len(alerts)} rows (last 7 days of demo range)")

    # Governance data
    metadata_tables = generate_metadata_tables()
    print(f"  metadata_tables: {len(metadata_tables)} rows")

    table_lineage = generate_table_lineage()
    print(f"  table_lineage: {len(table_lineage)} rows")

    user_permissions = generate_user_permissions()
    print(f"  user_permissions: {len(user_permissions)} rows")

    schema_changes = generate_schema_change_history()
    print(f"  schema_change_history: {len(schema_changes)} rows")

    # ── 3. Insert data ───────────────────────────────────────────────────
    print("\n=== Inserting data ===")

    client.insert("users", users, column_names=[
        "user_id", "register_date", "age", "gender", "city", "level", "last_login"
    ])
    print(f"  Inserted {len(users)} users")

    client.insert("products", products, column_names=[
        "product_id", "product_name", "category", "price", "stock", "supplier", "create_date"
    ])
    print(f"  Inserted {len(products)} products")

    client.insert("orders", orders, column_names=[
        "order_id", "user_id", "product_id", "order_date", "amount", "quantity", "status", "region", "category"
    ])
    print(f"  Inserted {len(orders)} orders")

    client.insert("metrics", metrics, column_names=[
        "metric_date", "metric_name", "metric_value", "region", "category"
    ])
    print(f"  Inserted {len(metrics)} metrics")

    # Ops inserts
    client.insert("flink_jobs", flink_jobs, column_names=[
        "job_id", "status", "start_time", "checkpoint_delay_s",
        "backpressure", "last_checkpoint_status", "last_checkpoint_time",
        "subtask_count", "throughput"
    ])
    print(f"  Inserted {len(flink_jobs)} flink_jobs")

    client.insert("kafka_topics", kafka_topics, column_names=[
        "topic", "partition_count", "producer_tps", "consumer_group",
        "total_lag", "consumer_status", "consumer_tps", "max_partition",
        "max_partition_lag", "check_date"
    ])
    print(f"  Inserted {len(kafka_topics)} kafka_topics")

    client.insert("pipeline_logs", pipeline_logs, column_names=[
        "log_id", "pipeline", "error_type", "log_level", "log_time", "message"
    ])
    print(f"  Inserted {len(pipeline_logs)} pipeline_logs")

    client.insert("alerts", alerts, column_names=[
        "alert_id", "service", "alert_type", "level", "alert_time", "status"
    ])
    print(f"  Inserted {len(alerts)} alerts")

    # Governance inserts
    client.insert("metadata_tables", metadata_tables, column_names=[
        "table_name", "layer", "description", "storage_engine",
        "data_source", "row_count", "last_updated"
    ])
    print(f"  Inserted {len(metadata_tables)} metadata_tables")

    client.insert("table_lineage", table_lineage, column_names=[
        "source_table", "target_table", "relation_type", "column_mapping"
    ])
    print(f"  Inserted {len(table_lineage)} table_lineage")

    client.insert("user_permissions", user_permissions, column_names=[
        "user_id", "role", "table_name", "permission_type", "granted_date"
    ])
    print(f"  Inserted {len(user_permissions)} user_permissions")

    client.insert("schema_change_history", schema_changes, column_names=[
        "table_name", "change_type", "description", "operator", "change_date"
    ])
    print(f"  Inserted {len(schema_changes)} schema_change_history")

    # ── 4. Verify ────────────────────────────────────────────────────────
    print("\n=== Verification ===")
    all_tables = [
        "orders", "users", "products", "metrics",
        "flink_jobs", "kafka_topics", "pipeline_logs", "alerts",
        "metadata_tables", "table_lineage", "user_permissions", "schema_change_history",
    ]
    for table in all_tables:
        count = client.command(f"SELECT count() FROM {table}")
        print(f"  {table}: {count} rows")

    # Demo case sanity checks
    anomaly_date_str = str(DEMO_END)
    before_anomaly_str = str(DEMO_END - timedelta(days=1))

    gmv_result = client.query(
        f"SELECT metric_value FROM metrics WHERE metric_date = '{before_anomaly_str}' AND metric_name = 'GMV' AND region = '华东' AND category = '合计'"
    )
    gmv = gmv_result.result_rows[0][0] if gmv_result.result_rows else None
    print(f"\n  [2.1] Day-before-anomaly GMV (华东): {gmv}")

    job_status = client.command(
        "SELECT status, checkpoint_delay_s, backpressure, last_checkpoint_status FROM flink_jobs WHERE job_id = 'order_sync_job' LIMIT 1"
    )
    print(f"  [3.1] order_sync_job status: {job_status}")

    kafka_lag = client.command(
        "SELECT total_lag, consumer_status FROM kafka_topics WHERE topic = 'orders' AND consumer_group = 'order-consumer-group' LIMIT 1"
    )
    print(f"  [3.2] orders topic lag: {kafka_lag}")

    log_count = client.command(
        f"SELECT count() FROM pipeline_logs WHERE log_time >= '{anomaly_date_str} 14:00:00' AND error_type = 'ConnectionTimeout'"
    )
    print(f"  [3.3] ConnectionTimeout logs on {anomaly_date_str} after 14:00: {log_count}")

    alert_count = client.command(
        f"SELECT count() FROM alerts WHERE alert_time >= toDateTime('{anomaly_date_str}') - INTERVAL 7 DAY"
    )
    print(f"  [3.4] Alerts in last 7 days: {alert_count}")

    lineage_count = client.command(
        "SELECT count() FROM table_lineage WHERE source_table = 'orders' OR target_table = 'orders'"
    )
    print(f"  [4.2] orders lineage relations: {lineage_count}")

    zhangsan_perms = client.command(
        "SELECT count() FROM user_permissions WHERE user_id = 'zhangsan'"
    )
    print(f"  [4.3] zhangsan permissions: {zhangsan_perms}")

    schema_changes_count = client.command(
        f"SELECT count() FROM schema_change_history WHERE table_name = 'orders' AND change_date >= '{DEMO_END - timedelta(days=30)}'"
    )
    print(f"  [4.4] orders schema changes (last 30 days): {schema_changes_count}")

    # ── RCA scenario verification ──
    def scalar(query_sql):
        r = client.query(query_sql)
        return r.result_rows[0][0] if r.result_rows else None

    # Overall GMV: 华东 day-before-anomaly vs anomaly day (uses "合计" category)
    gmv_before = scalar(
        f"SELECT metric_value FROM metrics WHERE metric_date = '{before_anomaly_str}' AND metric_name = 'GMV' AND region = '华东' AND category = '合计'"
    )
    gmv_anomaly = scalar(
        f"SELECT metric_value FROM metrics WHERE metric_date = '{anomaly_date_str}' AND metric_name = 'GMV' AND region = '华东' AND category = '合计'"
    )
    drop_pct = round((1 - float(gmv_anomaly) / float(gmv_before)) * 100, 1) if gmv_before and gmv_anomaly else "N/A"
    print(f"\n  [RCA] 华东 GMV: day-before={gmv_before}, anomaly-day={gmv_anomaly}, drop={drop_pct}%")

    # Category breakdown for 华东 on anomaly day (excluding overall records)
    cat_rows = client.query(
        f"SELECT category, metric_value FROM metrics WHERE metric_date = '{anomaly_date_str}' AND metric_name = 'GMV' AND region = '华东' AND category NOT IN ('', '合计') ORDER BY metric_value DESC"
    ).result_rows
    print(f"  [RCA] 华东 anomaly day by category GMV: {[(r[0], r[1]) for r in cat_rows]}")

    # Other regions should be normal on anomaly day
    other_rows = client.query(
        f"SELECT region, metric_value FROM metrics WHERE metric_date = '{anomaly_date_str}' AND metric_name = 'GMV' AND region != '华东' AND category = '合计' ORDER BY metric_value DESC"
    ).result_rows
    print(f"  [RCA] Other regions anomaly day GMV (should be normal): {[(r[0], r[1]) for r in other_rows]}")

    # Anomaly day 华东 电子产品 orders count vs day before
    today_order_count = scalar(
        f"SELECT count() FROM orders WHERE order_date = '{anomaly_date_str}' AND region = '华东' AND category = '电子产品'"
    )
    yesterday_order_count = scalar(
        f"SELECT count() FROM orders WHERE order_date = '{before_anomaly_str}' AND region = '华东' AND category = '电子产品'"
    )
    print(f"  [RCA] 华东 electronics orders: anomaly-day={today_order_count}, day-before={yesterday_order_count}")

    # ── 5. Sync actual schema back to sql/demo_cases.sql ──────────────────
    sync_sql_to_file(client)

    print("\nDone! All demo data is ready for 5 roles.")


if __name__ == "__main__":
    main()
