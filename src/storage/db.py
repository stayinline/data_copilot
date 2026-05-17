from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy import Column, String, Integer, BigInteger, Text, JSON, TIMESTAMP
from sqlalchemy import func

from config import POSTGRES_DSN

engine = create_async_engine(POSTGRES_DSN, echo=False)
async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

Base = declarative_base()


class ChatSession(Base):
    __tablename__ = "chat_session"

    session_id = Column(String(64), primary_key=True)
    user_id = Column(String(64), nullable=False)
    intent_type = Column(String(32))
    status = Column(String(16), default="active")
    created_at = Column(TIMESTAMP, server_default=func.current_timestamp())
    updated_at = Column(TIMESTAMP, server_default=func.current_timestamp())
    context_summary = Column(Text)


class ToolLog(Base):
    __tablename__ = "tool_logs"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    session_id = Column(String(64), nullable=False, index=True)
    trace_id = Column(String(64))
    tool_name = Column(String(64), nullable=False, index=True)
    input_params = Column(JSON)
    output_data = Column(JSON)
    status = Column(String(16))
    error_message = Column(Text)
    latency_ms = Column(Integer)
    retry_count = Column(Integer, default=0)
    created_at = Column(TIMESTAMP, server_default=func.current_timestamp())


async def init_db():
    """Create tables if they don't exist."""
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


async def get_db() -> AsyncSession:
    async with async_session() as session:
        yield session
