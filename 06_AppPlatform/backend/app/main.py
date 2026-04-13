from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api.routes.analysis import router as analysis_router
from app.api.routes.crud import router as crud_router
from app.api.routes.engineering import router as engineering_router
from app.api.routes.filters import router as filters_router
from app.api.routes.health import router as health_router
from app.api.routes.metadata import router as metadata_router
from app.api.routes.msrp import router as msrp_router
from app.api.routes.msrp_workflow import router as msrp_workflow_router
from app.api.routes.platform_db import router as platform_db_router
from app.api.routes.review import router as review_router
from app.api.routes.review_cases import router as review_cases_router
from app.core.config import API_PREFIX, APP_NAME, APP_VERSION, CORS_ORIGINS

app = FastAPI(title=APP_NAME, version=APP_VERSION)

app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(health_router)
app.include_router(metadata_router, prefix=API_PREFIX)
app.include_router(filters_router, prefix=API_PREFIX)
app.include_router(analysis_router, prefix=API_PREFIX)
app.include_router(crud_router, prefix=API_PREFIX)
app.include_router(platform_db_router, prefix=API_PREFIX)
app.include_router(engineering_router, prefix=API_PREFIX)
app.include_router(msrp_router, prefix=API_PREFIX)
app.include_router(msrp_workflow_router, prefix=API_PREFIX)
app.include_router(review_router, prefix=API_PREFIX)
app.include_router(review_cases_router, prefix=API_PREFIX)
