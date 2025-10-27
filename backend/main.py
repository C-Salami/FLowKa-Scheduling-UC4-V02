import os
import psycopg2
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv
from typing import List, Dict, Any

# Load environment variables from .env
load_dotenv()

app = FastAPI(
    title="Factory Control Tower API",
    description="Backend API for dashboard (schedule, KPIs, inventory, DC requests)",
    version="1.0.0"
)

# Allow frontend to call API (relaxed for demo)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],   # in prod you restrict this (CloudFront domain)
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def get_db_conn():
    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASS"),
    )

########################
# /api/gantt
########################
@app.get("/api/gantt")
def get_gantt() -> List[Dict[str, Any]]:
    sql = """
    SELECT
        s.line_id,
        l.line_name,
        s.production_date,
        s.product_id,
        p.product_name,
        s.planned_qty_cases,
        s.is_firm,
        lc.rate_cases_per_hour,
        (s.planned_qty_cases::decimal / lc.rate_cases_per_hour::decimal) AS hours_needed
    FROM schedule s
    JOIN products p ON p.product_id = s.product_id
    JOIN lines l ON l.line_id = s.line_id
    JOIN line_capability lc
      ON lc.line_id = s.line_id
     AND lc.product_id = s.product_id
    ORDER BY s.production_date, s.line_id;
    """

    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute(sql)
    rows = cur.fetchall()
    cur.close()
    conn.close()

    result = []
    for row in rows:
        (
            line_id,
            line_name,
            production_date,
            product_id,
            product_name,
            planned_qty_cases,
            is_firm,
            rate_cases_per_hour,
            hours_needed
        ) = row

        result.append({
            "line_id": line_id,
            "line_name": line_name,
            "production_date": production_date.isoformat(),
            "product_id": product_id,
            "product_name": product_name,
            "planned_qty_cases": int(planned_qty_cases),
            "is_firm": bool(is_firm),
            "rate_cases_per_hour": int(rate_cases_per_hour),
            "hours_needed": float(hours_needed),
        })
    return result


########################
# /api/kpis
########################
@app.get("/api/kpis")
def get_kpis() -> Dict[str, Any]:
    conn = get_db_conn()
    cur = conn.cursor()

    util_sql = """
    SELECT
        s.line_id,
        l.line_name,
        s.production_date,
        SUM(s.planned_qty_cases) AS total_cases,
        l.daily_capacity_cases,
        (SUM(s.planned_qty_cases)::decimal / l.daily_capacity_cases::decimal)*100 AS utilization_pct,
        (l.daily_capacity_cases - SUM(s.planned_qty_cases)) AS headroom_cases
    FROM schedule s
    JOIN lines l ON l.line_id = s.line_id
    GROUP BY s.line_id, l.line_name, s.production_date, l.daily_capacity_cases
    ORDER BY s.production_date, s.line_id;
    """
    cur.execute(util_sql)
    util_rows = cur.fetchall()

    line_utilization = []
    for row in util_rows:
        (
            line_id,
            line_name,
            production_date,
            total_cases,
            daily_capacity_cases,
            utilization_pct,
            headroom_cases
        ) = row
        line_utilization.append({
            "line_id": line_id,
            "line_name": line_name,
            "date": production_date.isoformat(),
            "total_cases": int(total_cases),
            "daily_capacity_cases": int(daily_capacity_cases),
            "utilization_pct": float(utilization_pct),
            "headroom_cases": int(headroom_cases),
        })

    cur.execute("""SELECT COUNT(*) FROM schedule WHERE is_firm = false;""")
    flexible_slots_count = cur.fetchone()[0]

    cur.execute("""SELECT COUNT(*) FROM dc_requests WHERE status = 'PENDING';""")
    pending_dc_requests = cur.fetchone()[0]

    cur.close()
    conn.close()

    return {
        "line_utilization": line_utilization,
        "flexible_slots_count": flexible_slots_count,
        "pending_dc_requests": pending_dc_requests
    }


########################
# /api/inventory
########################
@app.get("/api/inventory")
def get_inventory() -> List[Dict[str, Any]]:
    sql = """
    SELECT
        m.material_id,
        m.material_name,
        m.uom,
        m.supplier_lead_time_days,
        i.on_hand_qty
    FROM materials m
    JOIN inventory_materials i
      ON i.material_id = m.material_id
    ORDER BY m.material_id;
    """

    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute(sql)
    rows = cur.fetchall()
    cur.close()
    conn.close()

    result = []
    for row in rows:
        (
            material_id,
            material_name,
            uom,
            lead_time_days,
            on_hand_qty
        ) = row
        result.append({
            "material_id": material_id,
            "material_name": material_name,
            "uom": uom,
            "supplier_lead_time_days": int(lead_time_days) if lead_time_days is not None else None,
            "on_hand_qty": float(on_hand_qty)
        })
    return result


########################
# /api/dc_requests
########################
@app.get("/api/dc_requests")
def get_dc_requests() -> List[Dict[str, Any]]:
    sql = """
    SELECT
        r.request_id,
        r.dc_id,
        r.request_datetime,
        r.product_id,
        p.product_name,
        r.requested_qty_cases,
        r.requested_due_date,
        r.promo_reason,
        r.status
    FROM dc_requests r
    JOIN products p ON p.product_id = r.product_id
    ORDER BY r.request_datetime DESC
    LIMIT 20;
    """

    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute(sql)
    rows = cur.fetchall()
    cur.close()
    conn.close()

    result = []
    for row in rows:
        (
            request_id,
            dc_id,
            request_datetime,
            product_id,
            product_name,
            requested_qty_cases,
            requested_due_date,
            promo_reason,
            status
        ) = row

        result.append({
            "request_id": request_id,
            "dc_id": dc_id,
            "request_datetime": request_datetime.isoformat(),
            "product_id": product_id,
            "product_name": product_name,
            "requested_qty_cases": int(requested_qty_cases),
            "requested_due_date": requested_due_date.isoformat(),
            "promo_reason": promo_reason,
            "status": status
        })
    return result
