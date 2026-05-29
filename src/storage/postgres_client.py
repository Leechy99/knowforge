"""
PostgreSQL Client with pgvector support
"""
import inspect
import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import Column, String, Text, Float, DateTime, Index
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from sqlalchemy.orm import declarative_base

Base = declarative_base()


class DocumentRecord(Base):
    __tablename__ = "documents"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    source_type = Column(String(50), nullable=False)
    source_url = Column(Text, nullable=False)
    content_type = Column(String(50), nullable=False)
    metadata_json = Column("metadata", JSONB, nullable=False, default=dict)
    content_text = Column(Text, nullable=False, default="")
    chunks = Column(JSONB, nullable=False, default=list)
    quality_score = Column(Float, nullable=False, default=0.0)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    processed_at = Column(DateTime, nullable=True)

    __table_args__ = (
        Index("idx_source_type", "source_type"),
        Index("idx_content_type", "content_type"),
        Index("idx_quality_score", "quality_score"),
    )


def _get_record_metadata(record: Any) -> dict[str, Any]:
    return getattr(record, "metadata_json", getattr(record, "metadata", {}))


class PostgresClient:
    def __init__(self, dsn: str):
        self.dsn = dsn
        self.engine = create_async_engine(dsn, echo=False)
        self.session_factory = async_sessionmaker(self.engine, class_=AsyncSession)

    async def init(self):
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

    async def close(self):
        await self.engine.dispose()

    async def store_document(self, doc: dict[str, Any]) -> str:
        session = self.session_factory()
        try:
            record = DocumentRecord(
                id=doc.get("id", uuid.uuid4()),
                source_type=doc["source_type"],
                source_url=doc.get("source_url", ""),
                content_type=doc.get("content_type", "article"),
                metadata_json=doc.get("metadata", {}),
                content_text=doc.get("content", {}).get("text", ""),
                chunks=doc.get("content", {}).get("chunks", []),
                quality_score=doc.get("quality_score", 0.0),
                processed_at=datetime.utcnow(),
            )
            add_result = session.add(record)
            if inspect.isawaitable(add_result):
                await add_result
            await session.commit()
            return str(record.id)
        finally:
            close = getattr(session, "close", None)
            if close:
                close_result = close()
                if inspect.isawaitable(close_result):
                    await close_result

    async def get_document(self, doc_id: str) -> dict[str, Any] | None:
        session = self.session_factory()
        try:
            result = await session.get(DocumentRecord, uuid.UUID(doc_id))
            if result:
                return {
                    "id": str(result.id),
                    "source_type": result.source_type,
                    "source_url": result.source_url,
                    "content_type": result.content_type,
                    "metadata": _get_record_metadata(result),
                    "content_text": result.content_text,
                    "chunks": result.chunks,
                    "quality_score": result.quality_score,
                    "created_at": result.created_at.isoformat(),
                    "processed_at": result.processed_at.isoformat() if result.processed_at else None,
                }
            return None
        finally:
            close = getattr(session, "close", None)
            if close:
                close_result = close()
                if inspect.isawaitable(close_result):
                    await close_result
