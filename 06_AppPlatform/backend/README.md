# Backend (FastAPI)

## Run

```bash
cd 06_AppPlatform/backend
pip install -r requirements.txt
uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload

# Auth (enabled by default)
export APP_AUTH_ENABLED=true
export APP_AUTH_TOKEN=change-me

# Example request header
# X-Auth-Token: change-me
# X-User-Role: viewer|editor|admin
# X-User-Name: your-name
```

## API

- GET /healthz
- GET /v1/metadata/columns
- POST /v1/filters/options
- POST /v1/analysis/query
- GET /v1/crud/items
- POST /v1/crud/items
- PATCH /v1/crud/items/{item_id}
- DELETE /v1/crud/items/{item_id}

## Production

- Tencent Cloud Ubuntu deployment: `Markdown_Readme/Fullstack/TENCENT_CLOUD_DEPLOY.md`
- Manual CI/CD flow: `Markdown_Readme/Fullstack/MANUAL_CICD.md`
- systemd template: `03_Scripts/deploy/systemd/jato-fullstack-backend@.service`
- nginx template: `03_Scripts/deploy/nginx/jato_fullstack.conf.example`
