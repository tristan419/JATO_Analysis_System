from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api.routes.assistant import router as assistant_router
from app.api.routes.auth import router as auth_router
from app.api.routes.analysis import router as analysis_router
from app.api.routes.data_management import router as data_management_router
from app.api.routes.engineering import router as engineering_router
from app.api.routes.filters import router as filters_router
from app.api.routes.health import router as health_router
from app.api.routes.market_scan import router as market_scan_router
from app.api.routes.metadata import router as metadata_router
from app.api.routes.msrp_monthly_update import router as msrp_monthly_update_router
from app.api.routes.msrp import router as msrp_router
from app.api.routes.msrp_links import router as msrp_links_router
from app.api.routes.msrp_workflow import router as msrp_workflow_router
from app.api.routes.platform_db import router as platform_db_router
from app.api.routes.review import router as review_router
from app.api.routes.hermes import router as hermes_router
from app.api.routes.engineering_config import router as engineering_config_router
from app.api.routes.presence import router as presence_router
from app.api.routes.review_cases import router as review_cases_router
from app.api.routes.msrp_dryrun_dashboard import router as msrp_dryrun_dashboard_router
from app.core.config import API_PREFIX, APP_NAME, APP_VERSION, CORS_ORIGINS
from app.core.startup_validation import run_startup_validation

run_startup_validation()
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
app.include_router(assistant_router, prefix=API_PREFIX)
app.include_router(market_scan_router, prefix=API_PREFIX)
app.include_router(data_management_router, prefix=API_PREFIX)
app.include_router(platform_db_router, prefix=API_PREFIX)
app.include_router(engineering_router, prefix=API_PREFIX)
app.include_router(engineering_config_router, prefix=API_PREFIX)
app.include_router(msrp_router, prefix=API_PREFIX)
app.include_router(msrp_links_router, prefix=API_PREFIX)
app.include_router(msrp_workflow_router, prefix=API_PREFIX)
app.include_router(msrp_monthly_update_router, prefix=API_PREFIX)
app.include_router(review_router, prefix=API_PREFIX)
app.include_router(review_cases_router, prefix=API_PREFIX)
app.include_router(msrp_dryrun_dashboard_router, prefix=API_PREFIX)
app.include_router(hermes_router, prefix=API_PREFIX)
app.include_router(auth_router, prefix=API_PREFIX)
app.include_router(presence_router, prefix=API_PREFIX)
