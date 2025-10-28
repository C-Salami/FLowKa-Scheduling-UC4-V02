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

# Ultra-compact styling
st.markdown("""
    <style>
    .main .block-container {
        padding: 0 0.5rem;
        max-width: 100%;
    }
    .main > div:first-child {
        padding-top: 0 !important;
    }
    div[data-testid="stVerticalBlock"]:first-child {
        padding-top: 0 !important;
    }
    .element-container {margin-bottom: 0 !important;}
    div[data-testid="stVerticalBlock"] > div {gap: 0.2rem;}
    
    /* Cards */
    .card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        border-radius: 6px;
        padding: 8px 12px;
        box-shadow: 0 1px 3px rgba(0,0,0,0.1);
        height: 100%;
        color: white;
    }
    .card-white {
        background: white;
        border-radius: 6px;
        padding: 8px;
        box-shadow: 0 1px 3px rgba(0,0,0,0.06);
        border: 1px solid #e5e7eb;
        height: 100%;
    }
    
    /* KPIs */
    .kpi-value {
        font-size: 1.6rem;
        font-weight: 700;
        line-height: 1;
        margin: 2px 0;
    }
    .kpi-label {
        font-size: 0.6rem;
        opacity: 0.9;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 0.5px;
    }
    
    /* Metrics */
    .metric-box {
        background: #f8fafc;
        border-radius: 4px;
        padding: 6px 8px;
        border-left: 3px solid #667eea;
        margin-bottom: 4px;
    }
    .metric-title {
        font-size: 0.6rem;
        color: #64748b;
        font-weight: 600;
        margin-bottom: 1px;
    }
    .metric-value {
        font-size: 0.95rem;
        color: #0f172a;
        font-weight: 700;
    }
    
    /* Headers */
    .section-header {
        font-size: 0.75rem;
        font-weight: 700;
        color: #1e293b;
        margin-bottom: 5px;
        display: flex;
        justify-content: space-between;
        align-items: baseline;
    }
    
    h1 {
        font-size: 1.3rem !important;
        font-weight: 800 !important;
        margin: 0 0 0.2rem 0 !important;
        padding: 0 !important;
        color: #1e293b;
    }
    
    /* Sidebar styling */
    [data-testid="stSidebar"] {
        background: #ffffff;
        border-right: 2px solid #e5e7eb;
    }
    [data-testid="stSidebar"] > div:first-child {
        padding: 1rem;
    }
    
    /* Hide default streamlit elements */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    header {visibility: hidden;}
    
    /* Compact dataframe */
    .stDataFrame {font-size: 0.7rem;}
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
    conn = get_conn()

    # removed s.order_id because it doesn't exist in your DB
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
# 5. Scenario simulator core (already updated logic)
# ---------------------------
def simulate_request(product_id, extra_cases, due_date_str, data):
    """
    1. Check if already planned production before due_date covers the ask.
    2. If not, check available capacity (without touching firm) to add more.
    3. If capacity ok, check materials.
    4. Otherwise give partial and what's missing.
    """
    due_date_obj = pd.to_datetime(due_date_str).date()

    schedule_df = data["schedule"].copy()
    lines_df = data["lines"].copy()
    cap_df = data["capability"].copy()
    bom_df = data["bom"].copy()
    inv_df = data["inventory"].copy()

    schedule_df["production_date"] = pd.to_datetime(schedule_df["production_date"])
    schedule_df["prod_date_only"] = schedule_df["production_date"].dt.date

    sched_window = schedule_df[schedule_df["prod_date_only"] <= due_date_obj].copy()

    # STEP 1: already planned qty for this SKU before the due date
    sku_sched = sched_window[sched_window["product_id"] == product_id].copy()
    already_planned_cases = sku_sched["planned_qty_cases"].sum() if len(sku_sched) else 0

    covering_rows = []
    if len(sku_sched):
        sku_sched_sorted = sku_sched.sort_values(["prod_date_only", "line_id"])
        for _, r in sku_sched_sorted.iterrows():
            covering_rows.append({
                "line_id": r["line_id"],
                "production_date": r["prod_date_only"],
                "allocated_cases": int(r["planned_qty_cases"]),
                "source": "already_planned"
            })

    if already_planned_cases >= extra_cases:
        msg = (
            f"Yes. Already planned before {due_date_obj} is "
            f"{int(already_planned_cases)} cases. "
            f"This covers {int(extra_cases)} cases. No new plan needed."
        )
        return {
            "can_fulfill": True,
            "allocated_total": int(extra_cases),
            "remaining": 0,
            "plan_rows": covering_rows,
            "material_blockers": [],
            "capacity_blockers": [],
            "message": msg
        }

    need_after_plan = extra_cases - already_planned_cases

    # STEP 2: capacity for new production for this SKU
    capable_lines = cap_df[cap_df["product_id"] == product_id]["line_id"].unique().tolist()
    if not capable_lines:
        return {
            "can_fulfill": False,
            "allocated_total": int(already_planned_cases),
            "remaining": int(need_after_plan),
            "plan_rows": covering_rows,
            "material_blockers": ["No line can run this product"],
            "capacity_blockers": ["No capable line found"],
            "message": "We cannot run this SKU on any line."
        }

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
                "line_id": line_id,
                "production_date": this_date,
                "allocated_cases": int(allocate_now),
                "source": "new_plan"
            })

            remaining_capacity_allocation -= allocate_now

    new_capacity_cases = need_after_plan - remaining_capacity_allocation
    total_possible_capacity = already_planned_cases + new_capacity_cases
    capacity_shortfall = max(extra_cases - total_possible_capacity, 0)

    # STEP 3: material check for only the NEW part
    mat_blockers = []
    if new_capacity_cases > 0:
        sku_bom = bom_df[bom_df["product_id"] == product_id].copy()
        if len(sku_bom):
            sku_bom["needed_qty"] = sku_bom["qty_per_case"] * new_capacity_cases
            merged = pd.merge(sku_bom, inv_df, on="material_id", how="left")
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
            mat_blockers.append("No BOM for this SKU.")

    if capacity_shortfall == 0 and len(mat_blockers) == 0:
        msg = (
            f"Yes. We can cover {int(extra_cases)} cases by {due_date_obj}. "
            f"{int(already_planned_cases)} already planned. "
            f"The rest can be added with free capacity. Materials are OK."
        )
        plan_rows = []
        plan_rows.extend(covering_rows)
        plan_rows.extend(extra_plan_rows)

        return {
            "can_fulfill": True,
            "allocated_total": int(extra_cases),
            "remaining": 0,
            "plan_rows": plan_rows,
            "material_blockers": [],
            "capacity_blockers": [],
            "message": msg
        }

    # STEP 4: partial only
    feasible_new_cases = new_capacity_cases
    if len(mat_blockers) > 0:
        # simple rule: material issue => cannot confirm the new part
        feasible_new_cases = 0

    total_feasible = already_planned_cases + feasible_new_cases
    remaining_after_feasible = max(extra_cases - total_feasible, 0)

    msg_bits = []
    if total_feasible > 0:
        msg_bits.append(
            f"We can cover {int(total_feasible)} cases by {due_date_obj}."
        )
    if remaining_after_feasible > 0:
        msg_bits.append(
            f"The remaining {int(remaining_after_feasible)} cases cannot be done on time."
        )
    if capacity_shortfall > 0:
        msg_bits.append("Capacity is not enough for full ask.")
    if len(mat_blockers) > 0:
        msg_bits.append("Material risk: " + "; ".join(mat_blockers))

    final_msg = " ".join(msg_bits)

    cap_blockers = []
    if capacity_shortfall > 0:
        cap_blockers.append(
            f"Short {int(capacity_shortfall)} cases before {due_date_obj}"
        )

    # build plan rows we show as 'feasible'
    partial_plan_rows = []
    partial_plan_rows.extend(covering_rows)
    if feasible_new_cases > 0 and len(mat_blockers) == 0:
        partial_plan_rows.extend(extra_plan_rows)

    return {
        "can_fulfill": False,
        "allocated_total": int(total_feasible),
        "remaining": int(remaining_after_feasible),
        "plan_rows": partial_plan_rows,
        "material_blockers": mat_blockers,
        "capacity_blockers": cap_blockers,
        "message": final_msg
    }

# ---------------------------
# 6. Gantt chart
# ---------------------------

def build_gantt_figure(schedule_df):
    if schedule_df.empty:
        return None

    df = schedule_df.copy()
    df["production_date"] = pd.to_datetime(df["production_date"])

    min_date = df["production_date"].min()
    max_date = min_date + timedelta(days=9)

    df = df[df["production_date"] <= max_date].copy()
    if df.empty:
        return None

    lines = sorted(df["line_name"].unique(), reverse=True)

    fig = go.Figure()
    firm_color = "#10b981"
    flexible_color = "#fbbf24"

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
            marker=dict(color=color, line=dict(color='#1e293b', width=0.5)),
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
        height=220,
        margin=dict(l=60, r=10, t=15, b=25),
        plot_bgcolor='#f8fafc',
        paper_bgcolor='white',
        font=dict(size=9, color='#1e293b'),
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
# 7. Load data
# ---------------------------

schedule_df = load_schedule_df()
flexible_slots, pending_dc, active_lines, avg_util, util_df = load_kpi_data()
inv_top = load_inventory_summary()
dc_top = load_dc_requests_summary()
sim_data = load_master_data_for_sim()

# ---------------------------
# 8. SIDEBAR SIMULATOR
# ---------------------------

with st.sidebar:
    st.markdown("### 🚀 Promo Request Simulation")
    
    product_options = {
        row["product_name"]: row["product_id"]
        for _, row in sim_data["products"].iterrows()
    }
    product_display_names = list(product_options.keys())
    selected_product_name = st.selectbox("Product", options=product_display_names, key="sim_prod")
    selected_product_id = product_options[selected_product_name]

    requested_qty = st.number_input("Requested cases", min_value=100, max_value=10000000, step=500, value=18000, key="sim_qty")
    due_date_input = st.date_input("Due date", value=date.today() + timedelta(days=3), key="sim_due")

    run_it = st.button("🔍 Simulate", key="sim_button", use_container_width=True)

    if run_it:
        result = simulate_request(
            product_id=selected_product_id,
            extra_cases=int(requested_qty),
            due_date_str=str(due_date_input),
            data=sim_data
        )

        st.markdown("---")
        st.markdown("**Result:**")
        st.info(result["message"])

        if len(result["plan_rows"]) > 0:
            plan_df = pd.DataFrame(result["plan_rows"])
            plan_df = plan_df.rename(columns={
                "line_id": "Line",
                "production_date": "Date",
                "allocated_cases": "Cases",
                "source": "Source"
            })
            st.dataframe(plan_df, use_container_width=True, height=200)

        if result["material_blockers"]:
            st.markdown("**⚠️ Material Constraints:**")
            for m in result["material_blockers"]:
                st.markdown(f"- {m}")

        if result["capacity_blockers"]:
            st.markdown("**⚙️ Capacity Notes:**")
            for c in result["capacity_blockers"]:
                st.markdown(f"- {c}")

# ---------------------------
# 9. MAIN DASHBOARD
# ---------------------------

# Header
col_h1, col_h2 = st.columns([3, 1])
with col_h1:
    st.markdown("# 🏭 Factory Control Tower")
with col_h2:
    st.markdown(
        f"<div style='text-align:right;padding-top:4px;'>"
        f"<span style='font-size:0.65rem;color:#64748b;'>Updated: {datetime.now().strftime('%H:%M')}</span>"
        f"</div>",
        unsafe_allow_html=True
    )

# KPIs
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

# Gantt chart
st.markdown('<div class="card-white">', unsafe_allow_html=True)
st.markdown(
    '<div class="section-header">📊 Production Schedule — Next 10 Days'
    '<span style="font-size:0.6rem;font-weight:500;color:#64748b;">Green = firm / Yellow = flexible</span>'
    '</div>',
    unsafe_allow_html=True
)

fig = build_gantt_figure(schedule_df)
if fig is None:
    st.info("No schedule data")
else:
    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
st.markdown('</div>', unsafe_allow_html=True)

# Bottom row
col_b1, col_b2, col_b3 = st.columns(3)

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
