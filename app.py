import os
import psycopg2
import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from dotenv import load_dotenv
from datetime import datetime, timedelta, date

# ---------------------------
# 1. Load environment
# ---------------------------
load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")

# ---------------------------
# 2. Streamlit page config
# ---------------------------
st.set_page_config(
    page_title="Factory Control Tower",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# Styling: ultra-compact single-page dashboard
st.markdown("""
    <style>
    /* Remove all padding and margins */
    .main .block-container {
        padding: 0.5rem 1rem 0rem 1rem;
        max-width: 100%;
    }
    /* Hide sidebar toggle */
    [data-testid="stSidebar"] {display: none;}
    /* Remove gaps between elements */
    .element-container {margin-bottom: 0 !important;}
    div[data-testid="stVerticalBlock"] > div {gap: 0.3rem;}

    /* Card styles */
    .card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        border-radius: 8px;
        padding: 10px 14px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        height: 100%;
        color: white;
    }
    .card-white {
        background: white;
        border-radius: 8px;
        padding: 10px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.07);
        border: 1px solid #e5e7eb;
        height: 100%;
    }
    .card-title {
        font-size: 0.7rem;
        font-weight: 700;
        margin-bottom: 4px;
        text-transform: uppercase;
        letter-spacing: 0.5px;
    }

    /* KPI tiles */
    .kpi-value {
        font-size: 2rem;
        font-weight: 700;
        line-height: 1;
        margin: 4px 0;
    }
    .kpi-label {
        font-size: 0.65rem;
        opacity: 0.9;
        font-weight: 500;
    }

    /* Metric boxes */
    .metric-box {
        background: #f8fafc;
        border-radius: 6px;
        padding: 8px;
        border-left: 3px solid #667eea;
        margin-bottom: 6px;
    }
    .metric-title {
        font-size: 0.65rem;
        color: #64748b;
        font-weight: 600;
        margin-bottom: 2px;
    }
    .metric-value {
        font-size: 1.1rem;
        color: #0f172a;
        font-weight: 700;
    }

    /* Section headers */
    .section-header {
        font-size: 0.8rem;
        font-weight: 700;
        color: #1e293b;
        margin-bottom: 6px;
        display: flex;
        justify-content: space-between;
        align-items: baseline;
    }

    /* Dashboard title */
    h1 {
        font-size: 1.5rem !important;
        font-weight: 800 !important;
        margin: 0 0 0.3rem 0 !important;
        padding: 0 !important;
        color: #1e293b;
    }
    </style>
""", unsafe_allow_html=True)

# ---------------------------
# 3. DB connection helper
# ---------------------------
def get_conn():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
    )

# ---------------------------
# 4. Queries / data loaders
# ---------------------------

def load_schedule_df():
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

def load_kpi_data():
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

    flexible_slots_count = int(df_flex.iloc[0]["flexible_slots_count"]) if len(df_flex) else 0
    pending_dc_requests = int(df_pend.iloc[0]["pending_dc_requests"]) if len(df_pend) else 0
    active_lines = df_util["line_id"].nunique() if len(df_util) else 0
    avg_util = df_util["utilization_pct"].mean() if len(df_util) else 0

    return flexible_slots_count, pending_dc_requests, active_lines, avg_util, df_util

def load_inventory_summary():
    sql = """
    SELECT
        m.material_name,
        i.on_hand_qty,
        m.supplier_lead_time_days
    FROM materials m
    JOIN inventory_materials i
         ON i.material_id = m.material_id
    ORDER BY m.supplier_lead_time_days DESC NULLS LAST
    LIMIT 3;
    """
    conn = get_conn()
    df = pd.read_sql(sql, conn)
    conn.close()
    return df

def load_dc_requests_summary():
    sql = """
    SELECT
        r.dc_id,
        p.product_name,
        r.requested_qty_cases,
        r.status
    FROM dc_requests r
    JOIN products p ON p.product_id = r.product_id
    WHERE r.status IN ('PENDING','APPROVED')
    ORDER BY r.request_datetime DESC
    LIMIT 3;
    """
    conn = get_conn()
    df = pd.read_sql(sql, conn)
    conn.close()
    return df

def load_master_data_for_sim():
    """
    Load everything needed for simulation: schedule, capacity, capability,
    BOM, inventory, products.
    """
    conn = get_conn()

    schedule_sql = """
    SELECT
        s.line_id,
        s.production_date,
        s.product_id,
        s.planned_qty_cases,
        s.is_firm
    FROM schedule s;
    """
    schedule_df = pd.read_sql(schedule_sql, conn)

    lines_sql = """
    SELECT
        line_id,
        line_name,
        daily_capacity_cases
    FROM lines;
    """
    lines_df = pd.read_sql(lines_sql, conn)

    cap_sql = """
    SELECT
        line_id,
        product_id,
        rate_cases_per_hour
    FROM line_capability;
    """
    cap_df = pd.read_sql(cap_sql, conn)

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
    bom_df = pd.read_sql(bom_sql, conn)

    inv_sql = """
    SELECT
        i.material_id,
        i.on_hand_qty
    FROM inventory_materials i;
    """
    inv_df = pd.read_sql(inv_sql, conn)

    products_sql = """
    SELECT
        product_id,
        product_name
    FROM products;
    """
    products_df = pd.read_sql(products_sql, conn)

    conn.close()

    return {
        "schedule": schedule_df,
        "lines": lines_df,
        "capability": cap_df,
        "bom": bom_df,
        "inventory": inv_df,
        "products": products_df,
    }

# ---------------------------
# 5. Scenario simulator core
# ---------------------------
def simulate_request(product_id, extra_cases, due_date_str, data):
    """
    - Try to allocate 'extra_cases' of product_id by due_date.
    - Use unused headroom + bump flexible slots, but don't bump firm.
    - Check BOM materials for allocated volume.
    """
    # Parse due date
    due_date_obj = pd.to_datetime(due_date_str).date()

    schedule_df = data["schedule"].copy()
    lines_df = data["lines"].copy()
    cap_df = data["capability"].copy()
    bom_df = data["bom"].copy()
    inv_df = data["inventory"].copy()

    # Ensure production_date is datetime
    schedule_df["production_date"] = pd.to_datetime(schedule_df["production_date"])

    # We'll create a helper column that's pure date (no time)
    schedule_df["prod_date_only"] = schedule_df["production_date"].dt.date

    # Only consider records with prod_date_only <= due_date_obj
    sched_window = schedule_df[schedule_df["prod_date_only"] <= due_date_obj].copy()

    # Which lines can actually run this product?
    capable_lines = cap_df[cap_df["product_id"] == product_id]["line_id"].unique().tolist()
    if not capable_lines:
        return {
            "can_fulfill": False,
            "allocated_total": 0,
            "remaining": extra_cases,
            "plan_rows": [],
            "material_blockers": ["No line can run this product"],
            "capacity_blockers": ["No capable line found"],
            "message": "No line can produce this SKU."
        }

    remaining = extra_cases
    plan_rows = []

    # Iterate in date order, then per capable line
    for this_date in sorted(sched_window["prod_date_only"].unique()):
        if remaining <= 0:
            break

        for line_id in capable_lines:
            if remaining <= 0:
                break

            # get line daily capacity
            line_cap_row = lines_df[lines_df["line_id"] == line_id]
            if line_cap_row.empty:
                continue

            daily_capacity = int(line_cap_row.iloc[0]["daily_capacity_cases"])

            # All planned runs for this line on this_date
            todays_runs = sched_window[
                (sched_window["line_id"] == line_id) &
                (sched_window["prod_date_only"] == this_date)
            ]

            total_planned = todays_runs["planned_qty_cases"].sum() if len(todays_runs) else 0
            headroom = max(daily_capacity - total_planned, 0)

            # Flexible capacity we can bump
            flex_cases = todays_runs[~todays_runs["is_firm"]]["planned_qty_cases"].sum() if len(todays_runs) else 0

            possible_today = headroom + flex_cases
            if possible_today <= 0:
                continue

            allocate_now = min(possible_today, remaining)

            plan_rows.append({
                "line_id": line_id,
                "production_date": this_date,  # this_date is already a date
                "allocated_cases": int(allocate_now),
                "used_headroom": int(min(headroom, allocate_now)),
                "bumped_flexible": int(max(0, allocate_now - headroom))
            })

            remaining -= allocate_now

    allocated_total = extra_cases - remaining

    # -------- MATERIAL CHECK --------
    # For allocated_total cases, multiply BOM and compare to inventory.
    mat_blockers = []
    sku_bom = bom_df[bom_df["product_id"] == product_id].copy()
    if len(sku_bom):
        sku_bom["needed_qty"] = sku_bom["qty_per_case"] * allocated_total
        merged = pd.merge(
            sku_bom,
            inv_df,
            on="material_id",
            how="left"
        )
        for _, r in merged.iterrows():
            need = float(r["needed_qty"])
            have = float(r["on_hand_qty"]) if pd.notnull(r["on_hand_qty"]) else 0.0
            if need > have:
                shortage = need - have
                mat_blockers.append(
                    f"{r['material_name']} short by {shortage:,.0f} (lead {r['supplier_lead_time_days']}d)"
                )
    else:
        mat_blockers.append("No BOM for this SKU.")

    # -------- CAPACITY BLOCKERS --------
    cap_blockers = []
    if remaining > 0:
        cap_blockers.append(
            f"Short {remaining} cases before {due_date_obj}"
        )

    # -------- MESSAGE FOR DC --------
    if remaining <= 0 and len(mat_blockers) == 0:
        message = (
            f"✅ We can produce all {extra_cases:,} cases by {due_date_obj} "
            f"without impacting firm orders. Approved."
        )
        can_fulfill = True
    else:
        partial = allocated_total
        bits = []
        if partial > 0:
            bits.append(f"⚠ We can cover {partial:,} cases by {due_date_obj}.")
        if remaining > 0:
            bits.append(
                f"Remaining {remaining:,} cases need later dates or bumping firm slots."
            )
        if mat_blockers:
            bits.append("Material constraints: " + "; ".join(mat_blockers))
        message = " ".join(bits)
        can_fulfill = False

    return {
        "can_fulfill": can_fulfill,
        "allocated_total": allocated_total,
        "remaining": remaining,
        "plan_rows": plan_rows,
        "material_blockers": mat_blockers,
        "capacity_blockers": cap_blockers,
        "message": message
    }

# ---------------------------
# 6. Gantt / calendar view
# ---------------------------

def build_gantt_figure(schedule_df):
    if schedule_df.empty:
        return None

    # Standardize production_date as Timestamp
    df = schedule_df.copy()
    df["production_date"] = pd.to_datetime(df["production_date"])

    # show ~10 day horizon from earliest date
    min_date = df["production_date"].min()
    max_date = min_date + timedelta(days=9)

    df = df[df["production_date"] <= max_date].copy()
    if df.empty:
        return None

    lines = sorted(df["line_name"].unique(), reverse=True)

    fig = go.Figure()
    firm_color = "#10b981"      # green
    flexible_color = "#fbbf24"  # yellow

    for _, row in df.iterrows():
        prod_date = row["production_date"]
        start_dt = datetime.combine(prod_date.date(), datetime.min.time()) + timedelta(hours=8)
        end_dt = start_dt + timedelta(hours=float(row["hours_needed"]))

        color = firm_color if row["is_firm"] else flexible_color
        duration_ms = row["hours_needed"] * 3600000

        fig.add_trace(go.Bar(
            x=[duration_ms],
            y=[row["line_name"]],
            base=start_dt,
            orientation='h',
            marker=dict(
                color=color,
                line=dict(color='#1e293b', width=0.5)
            ),
            text=f"{row['product_name'][:15]}<br>{int(row['planned_qty_cases'])}c",
            textposition='inside',
            textfont=dict(size=8, color='white', family='monospace'),
            hovertemplate=(
                f"<b>{row['product_name']}</b><br>"
                f"Line: {row['line_name']}<br>"
                f"Date: {prod_date.strftime('%Y-%m-%d')}<br>"
                f"Start: {start_dt.strftime('%H:%M')}<br>"
                f"Duration: {row['hours_needed']:.1f}h<br>"
                f"Cases: {int(row['planned_qty_cases'])}<br>"
                f"Status: {'Firm' if row['is_firm'] else 'Flexible'}<br>"
                "<extra></extra>"
            ),
            showlegend=False
        ))

    fig.update_layout(
        barmode='overlay',
        height=260,
        margin=dict(l=70, r=10, t=20, b=30),
        plot_bgcolor='#f8fafc',
        paper_bgcolor='white',
        font=dict(size=10, color='#1e293b'),
        xaxis=dict(
            title='',
            type='date',
            tickformat='%b %d\n%a',
            gridcolor='#cbd5e1',
            showgrid=True,
            zeroline=False,
            range=[min_date - timedelta(hours=4), max_date + timedelta(hours=20)]
        ),
        yaxis=dict(
            title='',
            categoryorder='array',
            categoryarray=lines,
            gridcolor='#e2e8f0',
            showgrid=True,
        ),
        hovermode='closest'
    )
    return fig


# ---------------------------
# 7. Load dashboard data
# ---------------------------

schedule_df = load_schedule_df()
flexible_slots, pending_dc, active_lines, avg_util, util_df = load_kpi_data()
inv_top = load_inventory_summary()
dc_top = load_dc_requests_summary()
sim_data = load_master_data_for_sim()  # for simulator dropdowns etc.

# ---------------------------
# 8. DASHBOARD LAYOUT
# ---------------------------

# HEADER ROW
col_h1, col_h2 = st.columns([3, 1])
with col_h1:
    st.markdown("# 🏭 Factory Control Tower")
with col_h2:
    st.markdown(
        f"<div style='text-align:right;padding-top:6px;'>"
        f"<span style='font-size:0.7rem;color:#64748b;'>Updated: {datetime.now().strftime('%H:%M')}</span>"
        f"</div>",
        unsafe_allow_html=True
    )

# KPI ROW
col1, col2, col3, col4 = st.columns(4)
with col1:
    st.markdown(f"""
    <div class="card">
        <div class="kpi-label">ACTIVE LINES</div>
        <div class="kpi-value">{active_lines}</div>
    </div>
    """, unsafe_allow_html=True)
with col2:
    st.markdown(f"""
    <div class="card">
        <div class="kpi-label">AVG UTILIZATION</div>
        <div class="kpi-value">{avg_util:.0f}%</div>
    </div>
    """, unsafe_allow_html=True)
with col3:
    st.markdown(f"""
    <div class="card">
        <div class="kpi-label">FLEXIBLE SLOTS</div>
        <div class="kpi-value">{flexible_slots}</div>
    </div>
    """, unsafe_allow_html=True)
with col4:
    st.markdown(f"""
    <div class="card">
        <div class="kpi-label">PENDING REQUESTS</div>
        <div class="kpi-value">{pending_dc}</div>
    </div>
    """, unsafe_allow_html=True)

# MAIN CHART ROW (Gantt)
st.markdown('<div class="card-white">', unsafe_allow_html=True)
st.markdown(
    '<div class="section-header">📊 Production Schedule — Next 10 Days'
    '<span style="font-size:0.65rem;font-weight:500;color:#64748b;">Green = firm / Yellow = flexible</span>'
    '</div>',
    unsafe_allow_html=True
)

fig = build_gantt_figure(schedule_df)
if fig is None:
    st.info("No schedule data")
else:
    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
st.markdown('</div>', unsafe_allow_html=True)

# BOTTOM ROW: 4 columns (Hotspots, DC Req, Materials, Simulator)
col_b1, col_b2, col_b3, col_b4 = st.columns(4)

# ---- CAPACITY HOTSPOTS ----
with col_b1:
    st.markdown('<div class="card-white">', unsafe_allow_html=True)
    st.markdown('<div class="section-header">🔧 Capacity Hotspots</div>', unsafe_allow_html=True)
    if not util_df.empty:
        top3 = util_df.nlargest(3, "utilization_pct")[["line_name", "production_date", "utilization_pct"]]
        for _, row in top3.iterrows():
            st.markdown(f"""
            <div class="metric-box">
                <div class="metric-title">{row['line_name']} • {row['production_date'].strftime('%b %d')}</div>
                <div class="metric-value">{row['utilization_pct']:.0f}%</div>
            </div>
            """, unsafe_allow_html=True)
    else:
        st.info("No data")
    st.markdown('</div>', unsafe_allow_html=True)

# ---- DC REQUESTS ----
with col_b2:
    st.markdown('<div class="card-white">', unsafe_allow_html=True)
    st.markdown('<div class="section-header">📦 DC Requests</div>', unsafe_allow_html=True)
    if not dc_top.empty:
        for _, row in dc_top.iterrows():
            status_color = "#10b981" if row['status'] == 'APPROVED' else "#f59e0b"
            st.markdown(f"""
            <div class="metric-box" style="border-left-color:{status_color};">
                <div class="metric-title">{row['dc_id']} • {row['product_name'][:18]}</div>
                <div class="metric-value">{int(row['requested_qty_cases'])} cases</div>
            </div>
            """, unsafe_allow_html=True)
    else:
        st.info("No requests")
    st.markdown('</div>', unsafe_allow_html=True)

# ---- CRITICAL MATERIALS ----
with col_b3:
    st.markdown('<div class="card-white">', unsafe_allow_html=True)
    st.markdown('<div class="section-header">⚠️ Critical Materials</div>', unsafe_allow_html=True)
    if not inv_top.empty:
        for _, row in inv_top.iterrows():
            lead_time = int(row['supplier_lead_time_days']) if pd.notna(row['supplier_lead_time_days']) else 0
            st.markdown(f"""
            <div class="metric-box">
                <div class="metric-title">{row['material_name'][:22]}</div>
                <div class="metric-value">{int(row['on_hand_qty'])} • {lead_time}d</div>
            </div>
            """, unsafe_allow_html=True)
    else:
        st.info("No data")
    st.markdown('</div>', unsafe_allow_html=True)

# ---- SIMULATOR ----
with col_b4:
    st.markdown('<div class="card-white">', unsafe_allow_html=True)
    st.markdown('<div class="section-header">🚀 Promo Request Simulation</div>', unsafe_allow_html=True)

    # product dropdown from sim_data["products"]
    product_options = {
        row["product_name"]: row["product_id"]
        for _, row in sim_data["products"].iterrows()
    }
    product_display_names = list(product_options.keys())
    selected_product_name = st.selectbox(
        "Product",
        options=product_display_names,
        key="sim_prod",
        help="Which SKU is the DC asking for?"
    )
    selected_product_id = product_options[selected_product_name]

    requested_qty = st.number_input(
        "Requested cases",
        min_value=100,
        max_value=100000,
        step=500,
        value=18000,
        key="sim_qty"
    )

    due_date_input = st.date_input(
        "Due date",
        value=date.today() + timedelta(days=3),
        key="sim_due"
    )

    run_it = st.button("Simulate", key="sim_button")

    if run_it:
        result = simulate_request(
            product_id=selected_product_id,
            extra_cases=int(requested_qty),
            due_date_str=str(due_date_input),
            data=sim_data
        )

        st.markdown(
            f"<div style='font-size:0.7rem;font-weight:600;color:#1e293b;margin-top:6px;'>Result</div>",
            unsafe_allow_html=True
        )
        st.write(result["message"])

        if len(result["plan_rows"]) > 0:
            plan_df = pd.DataFrame(result["plan_rows"])
            plan_df = plan_df.rename(columns={
                "line_id": "Line",
                "production_date": "Date",
                "allocated_cases": "Planned (cases)",
                "used_headroom": "Free Cap Used",
                "bumped_flexible": "Flex Bumped"
            })
            st.dataframe(
                plan_df,
                use_container_width=True,
                height=140
            )

        if result["material_blockers"]:
            st.markdown("<div style='font-size:0.7rem;color:#dc2626;font-weight:600;'>Material constraints</div>", unsafe_allow_html=True)
            for m in result["material_blockers"]:
                st.markdown(f"- {m}")

        if result["capacity_blockers"]:
            st.markdown("<div style='font-size:0.7rem;color:#f59e0b;font-weight:600;'>Capacity notes</div>", unsafe_allow_html=True)
            for c in result["capacity_blockers"]:
                st.markdown(f"- {c}")

    st.markdown('</div>', unsafe_allow_html=True)
