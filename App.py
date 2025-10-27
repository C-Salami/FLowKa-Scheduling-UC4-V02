import os
import psycopg2
import pandas as pd
import plotly.express as px
import streamlit as st
from dotenv import load_dotenv

# -------------------------------------------------
# 1. Load DB credentials from .env
# -------------------------------------------------
load_dotenv()  # reads local .env if present

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")

# -------------------------------------------------
# 2. Helper: get a DB connection
# -------------------------------------------------
def get_conn():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
    )

# -------------------------------------------------
# 3. Query helpers
# -------------------------------------------------

def load_gantt_df():
    """
    Returns per-line per-day schedule with hours needed.
    We'll use this for a 'Gantt-like' bar chart.
    """
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
    conn = get_conn()
    df = pd.read_sql(sql, conn)
    conn.close()
    return df


def load_kpis():
    """
    Returns:
     - line_utilization: per line per date
     - flexible_slots_count
     - pending_dc_requests
    """
    conn = get_conn()

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
    df_util = pd.read_sql(util_sql, conn)

    flex_sql = "SELECT COUNT(*) AS flexible_slots_count FROM schedule WHERE is_firm = false;"
    df_flex = pd.read_sql(flex_sql, conn)

    pend_sql = "SELECT COUNT(*) AS pending_dc_requests FROM dc_requests WHERE status = 'PENDING';"
    df_pend = pd.read_sql(pend_sql, conn)

    conn.close()

    flexible_slots_count = int(df_flex['flexible_slots_count'][0]) if len(df_flex) > 0 else 0
    pending_dc_requests = int(df_pend['pending_dc_requests'][0]) if len(df_pend) > 0 else 0

    return df_util, flexible_slots_count, pending_dc_requests


def load_inventory_df():
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
    conn = get_conn()
    df = pd.read_sql(sql, conn)
    conn.close()
    return df


def load_dc_requests_df():
    sql = """
    SELECT
        r.request_id,
        r.dc_id,
        r.request_datetime,
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
    conn = get_conn()
    df = pd.read_sql(sql, conn)
    conn.close()
    return df

# -------------------------------------------------
# 4. Page layout
# -------------------------------------------------

st.set_page_config(
    page_title="Factory Control Tower",
    layout="wide"
)

st.title("Factory Control Tower 👇")

st.caption("Live view: production schedule, DC promo requests, KPIs, and critical materials")

# We'll create a 2x2 grid manually:
# Row 1: Gantt (left) | DC Requests (right)
# Row 2: KPIs (left)  | Inventory (right)

# Load all data up front
gantt_df = load_gantt_df()
kpi_df, flexible_slots_count, pending_dc_requests = load_kpis()
inv_df = load_inventory_df()
dc_df = load_dc_requests_df()

# -------------------------------------------------
# Row 1
# -------------------------------------------------
row1_col1, row1_col2 = st.columns(2)

with row1_col1:
    st.subheader("Production Schedule (Gantt-like)")

    st.caption("Each bar ≈ hours of work booked on that line that day. Green = firm, Yellow = flexible")

    # Prepare gantt-like chart data using Plotly bar chart
    # We'll treat each row as a bar whose length = hours_needed
    # y-axis = line_name + date
    if not gantt_df.empty:
        gantt_df["label"] = gantt_df["line_name"] + " " + gantt_df["production_date"].astype(str)
        gantt_df["color_status"] = gantt_df["is_firm"].apply(lambda x: "Firm" if x else "Flexible")

        fig = px.bar(
            gantt_df,
            x="hours_needed",
            y="label",
            color="color_status",
            orientation="h",
            hover_data=["product_name", "planned_qty_cases", "hours_needed", "production_date"],
            height=500
        )
        fig.update_layout(
            xaxis_title="Hours",
            yaxis_title="Line / Date",
            bargap=0.3,
            legend_title="Status"
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No schedule data found.")

with row1_col2:
    st.subheader("DC Requests / Promo Pulls")
    st.caption("Most recent high-demand asks from DCs")

    if dc_df.empty:
        st.info("No DC requests.")
    else:
        # Show as a simple table with some highlighted columns
        pretty_dc = dc_df.copy()
        pretty_dc["requested_datetime"] = pretty_dc["request_datetime"].astype(str)
        pretty_dc = pretty_dc[[
            "dc_id",
            "product_name",
            "requested_qty_cases",
            "requested_due_date",
            "promo_reason",
            "status",
            "requested_datetime"
        ]].rename(columns={
            "dc_id": "DC",
            "product_name": "Product",
            "requested_qty_cases": "Qty (cases)",
            "requested_due_date": "Due date",
            "promo_reason": "Reason",
            "status": "Status",
            "requested_datetime": "Requested at"
        })
        st.dataframe(pretty_dc, use_container_width=True, hide_index=True)

# -------------------------------------------------
# Row 2
# -------------------------------------------------
row2_col1, row2_col2 = st.columns(2)

with row2_col1:
    st.subheader("KPIs / Capacity Snapshot")

    # KPI tiles
    kpi_col_a, kpi_col_b, kpi_col_c = st.columns(3)
    kpi_col_a.metric("Flexible Slots", value=flexible_slots_count, help="How many planned runs are NOT firm (can be bumped)")
    kpi_col_b.metric("Pending DC Requests", value=pending_dc_requests)
    num_lines = kpi_df["line_id"].nunique() if not kpi_df.empty else 0
    kpi_col_c.metric("Active Lines", value=num_lines)

    st.markdown("#### Line Utilization by Day")

    if kpi_df.empty:
        st.info("No KPI data.")
    else:
        # Clean up data for table
        pretty_kpi = kpi_df.copy()
        pretty_kpi["utilization_pct"] = pretty_kpi["utilization_pct"].map(lambda x: round(float(x), 1))
        pretty_kpi = pretty_kpi[[
            "line_name",
            "production_date",
            "utilization_pct",
            "headroom_cases"
        ]].rename(columns={
            "line_name": "Line",
            "production_date": "Date",
            "utilization_pct": "Utilization (%)",
            "headroom_cases": "Headroom (cases)"
        })
        st.dataframe(pretty_kpi, use_container_width=True, hide_index=True)

with row2_col2:
    st.subheader("Critical Materials On Hand")
    st.caption("Sorted by supplier lead time (risk first)")

    if inv_df.empty:
        st.info("No inventory data.")
    else:
        pretty_inv = inv_df.copy()
        pretty_inv = pretty_inv.sort_values(
            by=["supplier_lead_time_days", "on_hand_qty"],
            ascending=[False, False]  # show long-lead + high usage first
        )

        pretty_inv = pretty_inv.rename(columns={
            "material_name": "Material",
            "on_hand_qty": "On Hand",
            "uom": "UoM",
            "supplier_lead_time_days": "Lead Time (days)"
        })

        st.dataframe(
            pretty_inv[["Material", "On Hand", "UoM", "Lead Time (days)"]],
            use_container_width=True,
            hide_index=True
        )
