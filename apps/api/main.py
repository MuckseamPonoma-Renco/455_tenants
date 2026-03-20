from fastapi import FastAPI
from apps.api.routers import admin, cases, health, ingest, mobile, report

app = FastAPI(title='Tenant Issue OS', version='2.0.0')
app.include_router(health.router)
app.include_router(ingest.router, prefix='/ingest', tags=['ingest'])
app.include_router(admin.router, prefix='/admin', tags=['admin'])
app.include_router(mobile.router, prefix='/mobile', tags=['mobile'])
app.include_router(cases.router, prefix='/api', tags=['api'])

app.include_router(report.router, tags=['report'])
