# Backend (FastAPI)

This service:
- Connects to AWS RDS Postgres
- Exposes read-only API endpoints for the dashboard:
  - /api/gantt
  - /api/kpis
  - /api/inventory
  - /api/dc_requests

## Running in GitHub Codespaces

1. Open this repo in a Codespace.
2. In the terminal inside Codespaces:
   ```bash
   cd backend
   python3 -m venv venv
   source venv/bin/activate
   pip install -r requirements.txt
   cp .env.example .env
   # edit .env with real RDS credentials
   uvicorn main:app --reload --host 0.0.0.0 --port 8000
