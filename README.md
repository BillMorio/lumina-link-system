# Lumina Link System

Greenfield, link-centric engagement tracker on the new schema (see `link-centric-system.html`
and `link-centric-model.sql` in the parent folder).

- **backend/** — FastAPI + PostgreSQL (`asyncpg`). Creates the schema on boot (`schema.sql`, idempotent).
  Deployed on Render (Docker) with a managed Postgres. Needs `DATABASE_URL`.
- **frontend/** — static dashboard (set the API URL in the top bar). Deployed on Vercel.

## Schema (14 tables, link-centric)
`campaigns`, `creators`, `campaign_creators`, **`links`** (core), `post_snapshots`,
`orders`, `order_snapshots`, `comments`, `transcripts`, `briefs`, `users`, `settings`,
`audit_log`, `pauses`.

## Local run
```
cd backend
python -m venv .venv && . .venv/Scripts/activate
pip install -r requirements.txt
set DATABASE_URL=postgres://...
uvicorn main:app --reload
```

## Endpoints
`GET /health` · `GET/POST /api/links` · `GET /api/links/{id}` · `GET/POST /api/orders`
