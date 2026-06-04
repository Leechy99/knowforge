"""
AI Knowledge Base - FastAPI Application
"""
import inspect
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from src.api import export, failures, qa, search
from src.config import get_settings
from src.learning.failure_tracker import FailureTracker
from src.processors.vectorizer import ContentVectorizer
from src.query.rag import RAGQA
from src.query.vector_search import VectorSearch
from src.storage.neo4j_client import Neo4jGraphStore
from src.storage.postgres_client import PostgresClient
from src.storage.qdrant_client import QdrantVectorStore


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()
    app.state.settings = settings
    if not hasattr(app.state, "postgres_client"):
        app.state.postgres_client = PostgresClient(dsn=settings.postgres_dsn)
    if not hasattr(app.state, "vector_store"):
        app.state.vector_store = QdrantVectorStore(url=settings.qdrant_url)
    if not hasattr(app.state, "neo4j_client"):
        app.state.neo4j_client = Neo4jGraphStore(
            uri=settings.neo4j_uri,
            user=settings.neo4j_user,
            password=settings.neo4j_password,
        )
    if not hasattr(app.state, "vectorizer"):
        app.state.vectorizer = ContentVectorizer()
    if not hasattr(app.state, "qa_service"):
        app.state.qa_service = RAGQA(
            vector_store=app.state.vector_store,
            postgres_client=app.state.postgres_client,
            neo4j_client=app.state.neo4j_client,
            vectorizer=app.state.vectorizer,
        )
    if not hasattr(app.state, "search_service"):
        app.state.search_service = VectorSearch(
            vector_store=app.state.vector_store,
            vectorizer=app.state.vectorizer,
        )
    if not hasattr(app.state, "document_service"):
        app.state.document_service = app.state.postgres_client
    if not hasattr(app.state, "failure_tracker"):
        app.state.failure_tracker = FailureTracker(postgres_dsn=settings.postgres_dsn)
    try:
        yield
    finally:
        vectorizer = getattr(app.state, "vectorizer", None)
        unload = getattr(vectorizer, "unload", None)
        if unload is not None:
            unload()
        postgres_client = getattr(app.state, "postgres_client", None)
        postgres_close = getattr(postgres_client, "close", None)
        if postgres_close is not None:
            postgres_close_result = postgres_close()
            if inspect.isawaitable(postgres_close_result):
                await postgres_close_result
        neo4j_client = getattr(app.state, "neo4j_client", None)
        neo4j_close = getattr(neo4j_client, "close", None)
        if neo4j_close is not None:
            neo4j_close_result = neo4j_close()
            if inspect.isawaitable(neo4j_close_result):
                await neo4j_close_result


settings = get_settings()
app = FastAPI(
    title="KnowForge API",
    description="AI-ready knowledge ingestion and retrieval service",
    version="0.1.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(qa.router, prefix="/api/v1", tags=["QA"])
app.include_router(search.router, prefix="/api/v1", tags=["Search"])
app.include_router(export.router, prefix="/api/v1", tags=["Export"])
app.include_router(failures.router, prefix="/api/v1", tags=["Failures"])


@app.get("/health")
async def health_check():
    return {"status": "healthy"}
