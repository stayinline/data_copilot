# LLM - Model A: qwen3.6-flash (active)
# MODEL_TYPE = "qwen"
# CHAT_MODEL = "qwen3.6-flash"
# RERANK_MODEL_NAME = "qwen3.6-plus"
# MODEL_API_KEY = "sk-2a0c4ae6def84744956ac778b9408dbc"
# MODEL_BASE_URL = "https://dashscope.aliyuncs.com/compatible-mode/v1"
# LLM_TIMEOUT = 90

# LLM - Model B: qwen-plus-latest (uncomment to switch, comment out Model A above)
MODEL_TYPE = "qwen"
CHAT_MODEL = "qwen-plus-latest"
RERANK_MODEL_NAME = "qwen3.6-plus"
MODEL_API_KEY = "sk-f21c88c445d246faa1399f2ccd9a0631"
MODEL_BASE_URL = "https://dashscope.aliyuncs.com/compatible-mode/v1"
LLM_TIMEOUT = 90






#向量化模型
#MODEL_TYPE: "qwen"
#RERANK_MODEL_NAME: "gte-rerank-v2"
#EMBEDDING_MODEL_NAME: "text-embedding-v4"
#MODEL_API_KEY: "sk-f21c88c445d246faa1399f2ccd9a0631"
#MODEL_BASE_URL: "https://dashscope.aliyuncs.com/compatible-mode/v1"
#MODEL_TEMPERATURE: 0.01

# JWT



JWT_SECRET = "your-jwt-secret-key-change-in-production"
JWT_ALGORITHM = "HS256"

# PostgreSQL (chat_session + tool_logs)
POSTGRES_DSN = "postgresql+asyncpg://postgres:postgresql_admin@192.168.1.124:5432/copilot"

# ClickHouse (data query engine)
CLICKHOUSE_HOST = "192.168.1.124"
CLICKHOUSE_PORT = 8123
CLICKHOUSE_USER = "default"
CLICKHOUSE_PASSWORD = "65e84be3"
CLICKHOUSE_DATABASE = "copilot"

# Redis
REDIS_DSN = "redis://:redis_password@192.168.1.124:6379/0"


#
# Allowed tables for SQL queries (loaded from sql/demo_cases.sql)
from src.sql.schema_loader import ALLOWED_TABLES

# Rate limiting
RATE_LIMIT_REQUESTS = 30  # max requests per window
RATE_LIMIT_WINDOW = 60    # window in seconds

# Phase 2: Model routing
SUMMARY_MODEL = "qwen-turbo"
EMBEDDING_MODEL = "text-embedding-v4"

# Phase 2: Cache TTL (seconds)
QUERY_CACHE_TTL = 300       # 5 min
RETRIEVAL_CACHE_TTL = 1800  # 30 min

# Phase 2: Context management
SUMMARY_AFTER_ROUNDS = 2
MAX_CONTEXT_ROUNDS = 2

# Phase 2: SQL auto-fix
SQL_AUTO_FIX_MAX_RETRIES = 3

# Phase 2: Planner
PLANNER_MAX_DEPTH = 5
PLANNER_MAX_TOOL_CALLS = 10
PLANNER_MODE = "langgraph"  # "langgraph" | "react"

# Server
APP_HOST = "0.0.0.0"
APP_PORT = 8800
DEBUG = True
