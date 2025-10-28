import os
import json
import re
from datetime import datetime
from typing import Optional, List, Dict, Any

import psycopg2
import pandas as pd
from fastapi import FastAPI
from pydantic import BaseModel
from dotenv import load_dotenv

# -------------------------
# ENV + APP SETUP
# -------------------------
load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")

app = FastAPI(title="Scheduling Agent API")

def get_db_conn():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
    )

# -------------------------
# Pydantic request/response models
# -------------------------

class EmailParseIn(BaseModel):
    raw_email: str

class EmailParseOut(BaseModel):
    sku: Optional[str]
    qty_requested: Optional[int]
    requested_date: Optional[str]  # "YYYY-MM-DD"
    dc_name: Optional[str]

class SimulateIn(BaseModel):
    sku: str
    qty_requested: int
    requested_date: str  # "YYYY-MM-DD"
    dc_name: str

class SimulateOut(BaseModel):
    sku: str
    product_name: str
    dc_name: str
    requested_date: str
    requested_qty: int

    status: str  # "full" | "partial" | "no"
    covered_qty_on_requested_date: int
    remaining_qty: int

    next_available_date: Optional[str]
    next_available_additional_qty: int

    existing_orders_used: List[Dict[str, Any]]
    capacity_notes: List[str]
    material_notes: List[str]

    explanation: str

class BuildReplyIn(BaseModel):
    sim_result: Dict[str, Any]

class BuildReplyOut(BaseModel):
    reply_text: str


# -------------------------
# DB loaders (pulled from your code)
# -------------------------

def load_master_data_for_sim_from_db(conn):
    schedule_sql = """
    SELECT
        s.line_id,
        s.production_date,
        s.product_id,
        s.planned_qty_cases,
        s.is_firm
    FROM schedule s;
    """

    lines_sql = """
    SELECT
        line_id,
        line_name,
        daily_capacity_cases
    FROM lines;
    """

    cap_sql = """
    SELECT
        line_id,
        product_id,
        rate_cases_per_hour
    FROM line_capability;
    """

    bom_sql = """
    SELECT
        b.product_id,
        b.material_id,
        b.qty_per_case,
        m.material_name,
        m.supplier_lead_time_days
    FROM bill_of_materials b
    JOIN materials m ON m.material_id = b.material_id;
    """

    inv_sql = """
    SELECT
        i.material_id,
        i.on_hand_qty
    FROM inventory_materials i;
    """

    products_sql = """
    SELECT
        product_id,
        product_name
    FROM products;
    """

    schedule_df  = pd.read_sql(schedule_sql,  conn)
    lines_df     = pd.read_sql(lines_sql,     conn)
    cap_df       = pd.read_sql(cap_sql,       conn)
    bom_df       = pd.read_sql(bom_sql,       conn)
    inv_df       = pd.read_sql(inv_sql,       conn)
    products_df  = pd.read_sql(products_sql,  conn)

    return {
        "schedule":   schedule_df,
        "lines":      lines_df,
        "capability": cap_df,
        "bom":        bom_df,
        "inventory":  inv_df,
        "products":   products_df,
    }


# -------------------------
# CORE SIMULATION LOGIC
# (refactored from your Streamlit simulate_request)
# -------------------------

def simulate_request_core(
    product_id: str,
    requested_qty_cases: int,
    requested_due_date: str,
    dc_name: str,
    data: dict
) -> dict:
    """
    Same logic as your simulate_request() in Streamlit,
    but returns a clean dict for API + email.
    """

    due_date_obj = pd.to_datetime(requested_due_date).date()

    schedule_df = data["schedule"].copy()
    lines_df = data["lines"].copy()
    cap_df = data["capability"].copy()
    bom_df = data["bom"].copy()
    inv_df = data["inventory"].copy()

    products_df = data["products"]
    row_match = products_df[products_df["product_id"] == product_id]
    product_name = row_match.iloc[0]["product_name"] if not row_match.empty else product_id

    schedule_df["production_date"] = pd.to_datetime(schedule_df["production_date"])
    schedule_df["prod_date_only"] = schedule_df["production_date"].dt.date

    # window = anything scheduled on/before due date
    sched_window = schedule_df[schedule_df["prod_date_only"] <= due_date_obj].copy()

    # how much of this SKU is already planned by then?
    sku_sched = sched_window[sched_window["product_id"] == product_id].copy()
    already_planned_cases = sku_sched["planned_qty_cases"].sum() if len(sku_sched) else 0

    # build rows for existing (already planned)
    covering_rows = []
    if len(sku_sched):
        sku_sched_sorted = sku_sched.sort_values(["prod_date_only", "line_id"])
        for _, r in sku_sched_sorted.iterrows():
            covering_rows.append({
                "line_id": str(r["line_id"]),
                "production_date": str(r["prod_date_only"]),
                "allocated_cases": int(r["planned_qty_cases"]),
                "source": "already_planned"
            })

    # CASE A: already fully covered
    if already_planned_cases >= requested_qty_cases:
        return {
            "sku": product_id,
            "product_name": product_name,
            "dc_name": dc_name,
            "requested_date": str(due_date_obj),
            "requested_qty": int(requested_qty_cases),

            "status": "full",
            "covered_qty_on_requested_date": int(requested_qty_cases),
            "remaining_qty": 0,

            "next_available_date": str(due_date_obj),
            "next_available_additional_qty": int(requested_qty_cases),

            "existing_orders_used": covering_rows,
            "capacity_notes": [],
            "material_notes": [],

            "explanation": (
                f"We can cover {requested_qty_cases} cases of {product_name} by {due_date_obj} "
                f"using already planned production. No extra run needed."
            ),
        }

    # need more than what's already planned
    need_after_plan = requested_qty_cases - already_planned_cases

    # check which lines can run this SKU
    capable_lines = cap_df[cap_df["product_id"] == product_id]["line_id"].unique().tolist()
    if not capable_lines:
        return {
            "sku": product_id,
            "product_name": product_name,
            "dc_name": dc_name,
            "requested_date": str(due_date_obj),
            "requested_qty": int(requested_qty_cases),

            "status": "no",
            "covered_qty_on_requested_date": int(already_planned_cases),
            "remaining_qty": int(need_after_plan),

            "next_available_date": None,
            "next_available_additional_qty": int(already_planned_cases),

            "existing_orders_used": covering_rows,
            "capacity_notes": ["No capable line for this SKU"],
            "material_notes": [],

            "explanation": (
                f"We only have {already_planned_cases} cases planned and there is no line able to run {product_name}."
            ),
        }

    # try to allocate more capacity on those capable lines (same horizon)
    extra_plan_rows = []
    remaining_capacity_allocation = need_after_plan

    for this_date in sorted(sched_window["prod_date_only"].unique()):
        if remaining_capacity_allocation <= 0:
            break

        for line_id in capable_lines:
            if remaining_capacity_allocation <= 0:
                break

            line_info = lines_df[lines_df["line_id"] == line_id]
            if line_info.empty:
                continue

            daily_capacity = int(line_info.iloc[0]["daily_capacity_cases"])

            todays_runs = sched_window[
                (sched_window["line_id"] == line_id) &
                (sched_window["prod_date_only"] == this_date)
            ]
            total_planned_now = todays_runs["planned_qty_cases"].sum() if len(todays_runs) else 0

            headroom = max(daily_capacity - total_planned_now, 0)
            if headroom <= 0:
                continue

            allocate_now = min(headroom, remaining_capacity_allocation)

            extra_plan_rows.append({
                "line_id": str(line_id),
                "production_date": str(this_date),
                "allocated_cases": int(allocate_now),
                "source": "new_plan"
            })

            remaining_capacity_allocation -= allocate_now

    new_capacity_cases = need_after_plan - remaining_capacity_allocation
    total_possible_capacity = already_planned_cases + new_capacity_cases
    capacity_shortfall = max(requested_qty_cases - total_possible_capacity, 0)

    # material check for the EXTRA part only
    mat_blockers = []
    if new_capacity_cases > 0:
        sku_bom = data["bom"][data["bom"]["product_id"] == product_id].copy()
        if len(sku_bom):
            sku_bom["needed_qty"] = sku_bom["qty_per_case"] * new_capacity_cases
            merged = pd.merge(sku_bom, data["inventory"], on="material_id", how="left")
            for _, r in merged.iterrows():
                need = float(r["needed_qty"])
                have = float(r["on_hand_qty"]) if pd.notnull(r["on_hand_qty"]) else 0.0
                lead = r["supplier_lead_time_days"]
                if need > have:
                    shortage = need - have
                    mat_blockers.append(
                        f"{r['material_name']} short by {shortage:,.0f} (lead {lead}d)"
                    )
        else:
            mat_blockers.append("No BOM defined for this SKU")

    # If capacity and materials are fine -> full
    if capacity_shortfall == 0 and len(mat_blockers) == 0:
        merged_rows = covering_rows + extra_plan_rows
        return {
            "sku": product_id,
            "product_name": product_name,
            "dc_name": dc_name,
            "requested_date": str(due_date_obj),
            "requested_qty": int(requested_qty_cases),

            "status": "full",
            "covered_qty_on_requested_date": int(requested_qty_cases),
            "remaining_qty": 0,

            "next_available_date": str(due_date_obj),
            "next_available_additional_qty": int(requested_qty_cases),

            "existing_orders_used": merged_rows,
            "capacity_notes": [],
            "material_notes": [],

            "explanation": (
                f"We can supply {requested_qty_cases} cases of {product_name} "
                f"by {due_date_obj}. {already_planned_cases} are already planned, "
                f"the balance fits in available capacity. Materials are OK."
            ),
        }

    # Partial / Not fully possible
    feasible_new_cases = new_capacity_cases
    if len(mat_blockers) > 0:
        # if materials block, we assume extra cannot be confirmed
        feasible_new_cases = 0

    total_feasible = already_planned_cases + feasible_new_cases
    remaining_after_feasible = max(requested_qty_cases - total_feasible, 0)

    cap_notes = []
    if capacity_shortfall > 0:
        cap_notes.append(
            f"Short {int(capacity_shortfall)} cases before {due_date_obj}"
        )

    partial_rows = covering_rows[:]
    if feasible_new_cases > 0 and len(mat_blockers) == 0:
        partial_rows.extend(extra_plan_rows)

    # TODO in future: compute real "next available date"
    next_date_guess = str(due_date_obj)

    status_val = "partial" if total_feasible > 0 else "no"

    return {
        "sku": product_id,
        "product_name": product_name,
        "dc_name": dc_name,
        "requested_date": str(due_date_obj),
        "requested_qty": int(requested_qty_cases),

        "status": status_val,
        "covered_qty_on_requested_date": int(total_feasible),
        "remaining_qty": int(remaining_after_feasible),

        "next_available_date": next_date_guess,
        "next_available_additional_qty": int(total_feasible),

        "existing_orders_used": partial_rows,
        "capacity_notes": cap_notes,
        "material_notes": mat_blockers,

        "explanation": (
            f"We can cover {int(total_feasible)} cases of {product_name} "
            f"by {due_date_obj}. The remaining {int(remaining_after_feasible)} "
            f"cases cannot be produced on time. "
            + ("Capacity is limiting. " if capacity_shortfall > 0 else "")
            + ("Material risk: " + "; ".join(mat_blockers) if len(mat_blockers) > 0 else "")
        ),
    }


# -------------------------
# REPLY BUILDER
# -------------------------

def build_dc_reply(sim_result: dict) -> str:
    sku_name = sim_result.get("product_name", sim_result.get("sku", ""))
    dc = sim_result.get("dc_name", "your DC")
    req_qty = sim_result.get("requested_qty", 0)
    req_date = sim_result.get("requested_date", "")
    covered = sim_result.get("covered_qty_on_requested_date", 0)
    remaining = sim_result.get("remaining_qty", 0)
    status = sim_result.get("status", "")

    # human-facing message
    if status == "full":
        body_lines = [
            f"We can supply the full {req_qty} cases of {sku_name} to {dc} by {req_date}.",
            "This volume is already in plan or can be added without impacting other confirmed orders.",
            "",
            "No further action needed unless you want to change quantities.",
        ]
    elif status == "partial" and covered > 0 and remaining > 0:
        body_lines = [
            f"Request recap: {req_qty} cases of {sku_name} for {dc} by {req_date}.",
            "",
            f"- We can confirm {covered} cases by {req_date}.",
            f"- The remaining {remaining} cases cannot be produced in time due to capacity and/or materials.",
            "",
            "Please choose one of these options:",
            f"1. Deliver only {covered} cases on {req_date}.",
            f"2. Split delivery: {covered} first, then {remaining} as soon as available.",
            "3. Cancel or adjust the request.",
        ]
    elif status == "no":
        body_lines = [
            f"Request recap: {req_qty} cases of {sku_name} for {dc} by {req_date}.",
            "",
            "We cannot cover this quantity on that date.",
            "Reason: capacity and/or material constraints.",
            "",
            "Options:",
            "1. Accept delivery later.",
            "2. Reduce the requested quantity.",
            "3. Cancel.",
        ]
    else:
        body_lines = [
            f"For {sku_name}: {covered}/{req_qty} cases are feasible by {req_date}.",
            "Some volume can’t be produced on time due to load or material availability.",
            "",
            "Do you prefer partial first shipment, or full order later?",
        ]

    tech_bits = []
    cap_notes = sim_result.get("capacity_notes", [])
    mat_notes = sim_result.get("material_notes", [])
    if cap_notes:
        tech_bits.append("Capacity notes: " + "; ".join(cap_notes))
    if mat_notes:
        tech_bits.append("Material notes: " + "; ".join(mat_notes))

    if tech_bits:
        body_lines += [
            "",
            "Info for planning:",
            *tech_bits
        ]

    signature = [
        "",
        "Thanks,",
        "Planning Team",
    ]

    return "\n".join(body_lines + signature)


# -------------------------
# EMAIL PARSER (LLM PLACEHOLDER)
# -------------------------
# For now we make it dumb but working.
# Later we replace with OpenAI call.

def parse_dc_email_with_llm(raw_email_text: str) -> dict:
    """
    TEMP VERSION:
    - Tries to guess qty (number like 12000 or 12k)
    - Tries to guess a date like 2025-11-03 format if in email
    - sku/dc_name left None for now unless obvious
    """
    # qty: catch "12k" or "12000"
    qty_requested = None
    m_k = re.search(r"(\d+)\s*k", raw_email_text, re.IGNORECASE)
    m_num = re.search(r"\b(\d{3,})\b", raw_email_text)

    if m_k:
        qty_requested = int(m_k.group(1)) * 1000
    elif m_num:
        qty_requested = int(m_num.group(1))

    # date ISO like 2025-11-03
    m_date_iso = re.search(r"\b(20\d{2}-\d{2}-\d{2})\b", raw_email_text)
    requested_date = m_date_iso.group(1) if m_date_iso else None

    # very naive DC name guess (looks for 'DC' word)
    m_dc = re.search(r"\b([A-Z][a-zA-Z]+(?:\s+DC))\b", raw_email_text)
    dc_name = m_dc.group(1) if m_dc else None

    # very naive SKU guess (first ALLCAPS token with underscore)
    m_sku = re.search(r"\b([A-Z0-9]+_[A-Z0-9_]+)\b", raw_email_text)
    sku = m_sku.group(1) if m_sku else None

    return {
        "sku": sku,
        "qty_requested": qty_requested,
        "requested_date": requested_date,
        "dc_name": dc_name,
    }


# -------------------------
# FASTAPI ENDPOINTS
# -------------------------

@app.post("/parse_dc_email", response_model=EmailParseOut)
def parse_dc_email(body: EmailParseIn):
    parsed = parse_dc_email_with_llm(body.raw_email)
    return parsed

@app.post("/simulate_request", response_model=SimulateOut)
def simulate_request(body: SimulateIn):
    conn = get_db_conn()
    try:
        data = load_master_data_for_sim_from_db(conn)
        result = simulate_request_core(
            product_id=body.sku,
            requested_qty_cases=body.qty_requested,
            requested_due_date=body.requested_date,
            dc_name=body.dc_name,
            data=data
        )
    finally:
        conn.close()
    return result

@app.post("/build_reply", response_model=BuildReplyOut)
def build_reply(body: BuildReplyIn):
    reply_txt = build_dc_reply(body.sim_result)
    return {"reply_text": reply_txt}


@app.get("/")
def root():
    return {"status": "ok", "message": "Scheduling Agent API is running"}
