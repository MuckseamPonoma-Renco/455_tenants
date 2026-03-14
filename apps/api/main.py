from contextlib import asynccontextmanager
from fastapi import FastAPI
from apps.api.routers import admin, cases, health, ingest, mobile
from packages.db import init_db


@asynccontextmanager
async def lifespan(_: FastAPI):
    init_db()
    yield


app = FastAPI(title='Tenant Issue OS', version='2.0.0', lifespan=lifespan)
app.include_router(health.router)
app.include_router(ingest.router, prefix='/ingest', tags=['ingest'])
app.include_router(admin.router, prefix='/admin', tags=['admin'])
app.include_router(mobile.router, prefix='/mobile', tags=['mobile'])
app.include_router(cases.router, prefix='/api', tags=['api'])
