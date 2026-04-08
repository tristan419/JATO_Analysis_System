from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api.routes.analysis import router as analysis_router
from app.api.routes.crud import router as crud_router
from app.api.routes.filters import router as filters_router
from app.api.routes.health import router as health_router
from app.api.routes.metadata import router as metadata_router
from app.core.config import API_PREFIX, APP_NAME, APP_VERSION

app = FastAPI(title=APP_NAME, version=APP_VERSION)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(health_router)
app.include_router(metadata_router, prefix=API_PREFIX)
app.include_router(filters_router, prefix=API_PREFIX)
app.include_router(analysis_router, prefix=API_PREFIX)
app.include_router(crud_router, prefix=API_PREFIX)
