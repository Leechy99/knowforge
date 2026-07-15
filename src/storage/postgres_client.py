"""
PostgreSQL Client with pgvector support
"""
import inspect
import uuid
from typing import Any

from sqlalchemy import Column, DateTime, Float, Index, String, Text, or_, select, text
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase

from src.utils.time import utc_now


class Base(DeclarativeBase):
    """SQLAlchemy declarative model base."""


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
    created_at = Column(DateTime(timezone=True), nullable=False, default=utc_now)
    processed_at = Column(DateTime(timezone=True), nullable=True)

    __table_args__ = (
        Index("idx_source_type", "source_type"),
        Index("idx_content_type", "content_type"),
        Index("idx_quality_score", "quality_score"),
    )


def _get_record_metadata(record: Any) -> dict[str, Any]:
    return getattr(record, "metadata_json", getattr(record, "metadata", {}))


def _record_to_document(record: DocumentRecord) -> dict[str, Any]:
    return {
        "id": str(record.id),
        "source_type": record.source_type,
        "source_url": record.source_url,
        "content_type": record.content_type,
        "metadata": _get_record_metadata(record),
        "content": {
            "text": record.content_text,
            "chunks": record.chunks,
        },
        "content_text": record.content_text,
        "chunks": record.chunks,
        "quality_score": record.quality_score,
        "created_at": record.created_at.isoformat(),
        "processed_at": record.processed_at.isoformat() if record.processed_at else None,
    }


class PostgresClient:
    def __init__(self, dsn: str):
        self.dsn = dsn
        self.engine = create_async_engine(dsn, echo=False)
        self.session_factory = async_sessionmaker(self.engine, class_=AsyncSession)

    async def init(self) -> None:
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

    async def close(self) -> None:
        await self.engine.dispose()

    async def health_check(self) -> None:
        async with self.engine.connect() as connection:
            await connection.execute(text("SELECT 1"))

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
                processed_at=utc_now(),
            )
            session.add(record)
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
                return _record_to_document(result)
            return None
        finally:
            close = getattr(session, "close", None)
            if close:
                close_result = close()
                if inspect.isawaitable(close_result):
                    await close_result

    async def get_documents(self, doc_ids: list[str]) -> list[dict[str, Any]]:
        documents = []
        for doc_id in doc_ids:
            document = await self.get_document(doc_id)
            if document:
                documents.append(document)
        return documents

    async def list_documents(self, limit: int = 100) -> list[dict[str, Any]]:
        session = self.session_factory()
        try:
            statement = (
                select(DocumentRecord)
                .order_by(DocumentRecord.created_at.desc())
                .limit(limit)
            )
            result = await session.execute(statement)
            return [_record_to_document(record) for record in result.scalars()]
        finally:
            close = getattr(session, "close", None)
            if close:
                close_result = close()
                if inspect.isawaitable(close_result):
                    await close_result

    async def search_documents(self, query: str, limit: int = 100) -> list[dict[str, Any]]:
        session = self.session_factory()
        try:
            pattern = f"%{query}%"
            statement = (
                select(DocumentRecord)
                .where(
                    or_(
                        DocumentRecord.content_text.ilike(pattern),
                        DocumentRecord.source_url.ilike(pattern),
                    )
                )
                .order_by(DocumentRecord.created_at.desc())
                .limit(limit)
            )
            result = await session.execute(statement)
            return [_record_to_document(record) for record in result.scalars()]
        finally:
            close = getattr(session, "close", None)
            if close:
                close_result = close()
                if inspect.isawaitable(close_result):
                    await close_result
