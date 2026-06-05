"""FastAPI application entrypoint.

Wires the lifespan (DB init + RuntimeManager), CORS, and routers. Routers
are added in later phases; the skeleton exposes only the health probe.
"""
from __future__ import annotations

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.config import settings
from app.db import init_db
from app.routers import envs, health, runtime, stream
from app.runtime.manager import RuntimeManager

# RuntimeManager is imported lazily inside the lifespan to avoid a circular
# import (manager imports routers, routers import schemas, etc.).


def _configure_logging() -> None:
    level = getattr(logging, settings.log_level.upper(), logging.INFO)
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)-7s %(name)s :: %(message)s",
    )


import asyncio

@asynccontextmanager
async def lifespan(app: FastAPI):
    _configure_logging()
    await init_db()
    app.state.runtime_manager = RuntimeManager()
    # Trigger auto-resume in the background so startup isn't blocked by Kafka broker connectivity
    asyncio.create_task(app.state.runtime_manager.auto_resume())
    try:
        yield
    finally:
        await app.state.runtime_manager.aclose_all()


def create_app() -> FastAPI:
    app = FastAPI(
        title="Kafka Fan-Out Configurator",
        version="0.1.0",
        lifespan=lifespan,
    )
    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.cors_origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    app.include_router(health.router)
    app.include_router(envs.router)
    app.include_router(runtime.router)
    app.include_router(stream.router)
    return app


app = create_app()
