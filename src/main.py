"""
AI Knowledge Base - FastAPI Application
"""
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from src.api import qa, search, export, failures
from src.config import get_settings
from src.learning.failure_tracker import FailureTracker


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()
    app.state.settings = settings
    if not hasattr(app.state, "failure_tracker"):
        app.state.failure_tracker = FailureTracker(postgres_dsn=settings.postgres_dsn)
    yield


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
