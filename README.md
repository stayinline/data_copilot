# AI Data Copilot

> AI-powered assistant for data engineers — natural language data querying, automated analysis, and multi-step task planning.

[![Python 3.13+](https://img.shields.io/badge/Python-3.13+-blue.svg)](https://www.python.org/)
[![FastAPI](https://img.shields.io/badge/FastAPI-async-009688.svg)](https://fastapi.tiangolo.com/)
[![LangGraph](https://img.shields.io/badge/LangGraph-Agent-6d28d9.svg)](https://langchain-ai.github.io/langgraph/)
[![License: Internal](https://img.shields.io/badge/License-Internal-lightgrey.svg)]()

## Overview

AI Data Copilot enables data engineers to interact with data infrastructure using natural language. Instead of writing SQL or running diagnostic scripts manually, simply ask questions in Chinese (e.g., "昨天的GMV是多少？") and the system automatically understands intent, generates and executes SQL, performs root cause analysis, and returns structured answers — all through a conversational interface.

**Goal:** Improve data development and troubleshooting efficiency by 3x.

## Features

- **Natural Language Data Querying** — Ask data questions in plain language; the system generates and executes ClickHouse SQL automatically
- **Three-Tier Intent Classification** — Regex → Embedding → LLM routing for fast and accurate intent detection
- **Dual Execution Modes** — LangGraph planner (default) or ReAct loop, configurable via settings
- **SQL Auto-Fix** — LLM-driven automatic SQL correction on execution errors (up to 3 retries)
- **Built-in Tools**:
  - `run_sql` — Read-only SQL execution on ClickHouse with result caching
  - `query_metadata` — Schema discovery, DDL lookup, and table statistics
  - `root_cause_analysis` — Automatic metric anomaly drill-down by region and category
  - `pipeline_troubleshoot` — Flink/Kafka/logs/alerts diagnostic checks
  - `pipeline_full_diagnosis` — End-to-end pipeline cascade diagnosis
- **SQL Safety** — Keyword blacklist, AST-based table whitelist validation, automatic LIMIT injection, read-only access
- **SSE Streaming API** — Real-time structured events: `intent`, `thinking`, `tool_call`, `tool_result`, `sql_fix`, `final_answer`, `metrics`, `guidance`, `done`
- **Session Management** — Persistent sessions in PostgreSQL, short-term context in Redis with auto-summarization
- **Prometheus Metrics** — Chat, tool, LLM, cache, and SQL auto-fix metrics at `/metrics`
- **Rate Limiting** — Configurable per-user rate limiting
- **Web UI** — Built-in chat interface served at the root endpoint

## Tech Stack

| Layer | Technology |
|---|---|
| **Language** | Python 3.13 |
| **Web Framework** | FastAPI (async) |
| **ASGI Server** | Uvicorn |
| **LLM** | Alibaba Cloud DashScope (Qwen models) |
| **Agent Framework** | LangGraph / ReAct |
| **SQL Engine** | ClickHouse (`clickhouse-connect`) |
| **Session Store** | PostgreSQL (`asyncpg` + SQLAlchemy) |
| **Cache** | Redis |
| **SQL Parser** | sqlglot |
| **Auth** | PyJWT |
| **Monitoring** | Prometheus |

## Architecture

```
┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│   Web UI     │────▶│  FastAPI GW  │────▶│ Intent Class │
│ (index.html) │◀────│  (SSE/REST)  │     │   (3-tier)   │
└──────────────┘     └──────┬───────┘     └──────┬───────┘
                            │                    │
                     ┌──────▼───────┐     ┌──────▼───────┐
                     │   Planner    │────▶│   Tools      │
                     │ (LangGraph/  │     │ (SQL/Meta/   │
                     │   ReAct)     │     │  RCA/Pipeline)│
                     └──────┬───────┘     └──────┬───────┘
                            │                    │
                     ┌──────▼───────┐     ┌──────▼───────┐
                     │  Summarizer  │     │  ClickHouse  │
                     │  (Context)   │     │  PostgreSQL  │
                     └──────────────┘     │    Redis     │
                                          └──────────────┘
```

## Quick Start

### Prerequisites

- Python 3.13+
- PostgreSQL
- ClickHouse
- Redis

### Installation

```bash
# Clone the repository
git clone https://git.megarobo.info/mega-data-center/data-copilot.git
cd data-copilot

# Create and activate virtual environment
python -m venv .venv
.venv/Scripts/activate    # Windows
# source .venv/bin/activate  # Linux/macOS

# Install dependencies
pip install -r requirements.txt
```

### Configuration

Edit `config.py` to configure:

- **LLM**: Model selection, API key, and base URL (default: Qwen via DashScope)
- **Database**: PostgreSQL DSN (`POSTGRES_DSN`), ClickHouse connection (host, port, user, password, database)
- **Cache**: Redis DSN (`REDIS_DSN`)
- **Server**: Host, port, debug mode (`APP_HOST`, `APP_PORT`, `DEBUG`)
- **Planner**: Execution mode (`PLANNER_MODE`: `langgraph` or `react`)
- **Auth**: JWT secret key for production use

### Run

```bash
python main.py
```

The server starts on `0.0.0.0:8800` by default. Access the web UI at `http://localhost:8800/`.

### Seed Demo Data

```bash
python scripts/seed_demo_data.py
```

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/` | Serve the web UI |
| `POST` | `/api/v1/chat` | Non-streaming chat (returns full answer) |
| `POST` | `/api/v1/chat/stream` | Streaming chat via Server-Sent Events (SSE) |
| `GET` | `/api/v1/sessions` | List user sessions |
| `GET` | `/api/v1/sessions/{id}` | Get session detail |
| `DELETE` | `/api/v1/sessions/{id}` | Delete a session |
| `GET` | `/api/v1/logs` | Get tool execution logs for a session |
| `GET` | `/metrics` | Prometheus metrics |

## SSE Stream Events

The streaming endpoint (`/api/v1/chat/stream`) emits structured events:

| Event | Description |
|-------|-------------|
| `intent` | Classified user intent |
| `thinking` | LLM reasoning in progress |
| `tool_call` | Tool being invoked |
| `tool_result` | Tool execution result |
| `sql_fix` | SQL auto-fix attempt |
| `final_answer` | Final response to user |
| `metrics` | Performance metrics summary |
| `guidance` | Follow-up suggestions |
| `done` | Stream complete |

## Project Structure

```
data-copilot/
├── main.py                        # Entry point (starts uvicorn)
├── config.py                      # Central configuration
├── requirements.txt               # Python dependencies
├── README.md                      # This file
├── src/
│   ├── agent/
│   │   ├── intent.py              # Three-tier intent classifier
│   │   ├── context.py             # ExecutionContext dataclass
│   │   ├── llm_client.py          # LLM chat & embedding API
│   │   ├── planner.py             # LangGraph planner nodes
│   │   ├── planner_run.py         # Planner execution runner
│   │   ├── planner_state.py       # LangGraph TypedDict state
│   │   ├── react.py               # ReAct loop implementation
│   │   ├── sql_fix.py             # SQL auto-fix on error
│   │   └── summarizer.py          # Conversation summarization
│   ├── gateway/
│   │   ├── api.py                 # FastAPI app & REST endpoints
│   │   ├── auth.py                # JWT authentication
│   │   ├── metrics.py             # Prometheus metrics
│   │   └── rate_limiter.py        # Rate limiting
│   ├── tools/
│   │   ├── base.py                # Tool base class & registry
│   │   ├── run_sql.py             # Read-only SQL execution
│   │   ├── query_metadata.py      # Schema & metadata queries
│   │   ├── root_cause_analysis.py # Metric anomaly drill-down
│   │   ├── pipeline_troubleshoot.py # Pipeline diagnostics
│   │   └── pipeline_full_diagnosis.py # End-to-end diagnosis
│   ├── sql/
│   │   ├── schema_loader.py       # DDL parser for table whitelist
│   │   └── validator.py           # SQL safety validation
│   ├── storage/
│   │   ├── db.py                  # PostgreSQL models
│   │   ├── memory.py              # Redis session memory
│   │   └── redis_client.py        # Redis client wrapper
│   ├── cache/
│   │   └── query_cache.py         # SQL result caching
│   └── utils/
│       └── logging.py             # Structured logging
├── sql/
│   └── demo_cases.sql             # DDL + sample data
├── static/
│   ├── index.html                 # Web UI chat interface
│   └── demo.html                  # Demo page
├── scripts/
│   ├── seed_demo_data.py          # Data seeding script
│   └── gen_ppt/                   # PPT generation
└── docs/                          # Design documents
```

## Intent Classification

The system uses a three-tier approach to classify user intent:

1. **Tier 1 — Regex** (fastest): Pattern matching for greetings, SQL troubleshooting, pipeline issues, metadata queries, data queries, SQL generation, SQL optimization, metric diagnostics
2. **Tier 2 — Embedding**: Cosine similarity against pre-defined examples
3. **Tier 3 — LLM** (fallback): Full LLM classification

Three intent types are supported: `TOOL` (needs tool execution), `DIRECT` (direct LLM answer), `TROUBLESHOOT` (multi-tool diagnosis).

## SQL Safety

Multi-layer security ensures safe SQL execution:

1. **Keyword Blacklist**: Blocks DDL/DML statements (INSERT, UPDATE, DELETE, DROP, etc.)
2. **AST Validation**: Uses `sqlglot` to parse SQL and validate against table whitelist
3. **Automatic LIMIT**: Injects LIMIT clause if missing to prevent large result sets
4. **Read-Only Access**: Database user has read-only permissions

## Prometheus Metrics

Available at `/metrics`:

- `chat_requests_total` — Total chat requests counter
- `tool_calls_total` — Tool invocation counts by type
- `llm_latency_seconds` — LLM request latency histogram
- `cache_hits_total` / `cache_misses_total` — Cache hit/miss counters
- `sql_fix_attempts_total` — SQL auto-fix attempt count
