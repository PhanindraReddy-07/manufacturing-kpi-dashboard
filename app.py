"""
app.py — Poplar Manufacturing KPI Dashboard
External Streamlit app connected to Snowflake via snowflake-connector-python.
Multi-page layout using st.navigation / st.Page (Streamlit ≥ 1.36).
"""

import streamlit as st
import pandas as pd
import altair as alt
import snowflake.connector

# ──────────────────────────────────────────────
# Page config  (must be FIRST st call)
# ──────────────────────────────────────────────
st.set_page_config(
    page_title="Poplar Manufacturing",
    page_icon="🏭",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ──────────────────────────────────────────────
# Global CSS
# ──────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Syne:wght@400;700;800&family=IBM+Plex+Mono:wght@400;600&display=swap');

html, body, [class*="css"] {
    font-family: 'IBM Plex Mono', monospace;
}
h1, h2, h3, .stMetricLabel, [data-testid="stMetricLabel"] {
    font-family: 'Syne', sans-serif !important;
    letter-spacing: -0.02em;
}

/* Sidebar */
[data-testid="stSidebar"] {
    background: #0d1117 !important;
    border-right: 2px solid #1f6feb;
}
[data-testid="stSidebar"] * {
    color: #c9d1d9 !important;
}
[data-testid="stSidebar"] .stRadio label {
    font-family: 'Syne', sans-serif !important;
    font-size: 1rem !important;
    font-weight: 700 !important;
    padding: 6px 4px;
}

/* Metric cards */
[data-testid="metric-container"] {
    background: linear-gradient(135deg, #161b22 60%, #0d1117);
    border: 1px solid #30363d;
    border-radius: 10px;
    padding: 14px 18px !important;
}
[data-testid="stMetricValue"] {
    font-family: 'Syne', sans-serif !important;
    font-size: 1.7rem !important;
    font-weight: 800 !important;
    color: #58a6ff !important;
}
[data-testid="stMetricLabel"] {
    color: #8b949e !important;
    font-size: 0.72rem !important;
    text-transform: uppercase;
    letter-spacing: 0.08em;
}

/* Dividers */
hr {
    border-color: #21262d !important;
    margin: 1rem 0 !important;
}

/* Page background */
.stApp {
    background: #0d1117;
    color: #c9d1d9;
}

/* Section headers */
.section-title {
    font-family: 'Syne', sans-serif;
    font-size: 1.5rem;
    font-weight: 800;
    color: #e6edf3;
    border-left: 4px solid #1f6feb;
    padding-left: 12px;
    margin: 1.5rem 0 0.75rem 0;
}

/* Status badges */
.badge-good  { background:#1a3a2a; color:#3fb950; border:1px solid #2ea043; border-radius:20px; padding:2px 10px; font-size:0.75rem; }
.badge-warn  { background:#3a2a0a; color:#d29922; border:1px solid #9e6a03; border-radius:20px; padding:2px 10px; font-size:0.75rem; }
.badge-bad   { background:#3a0a0a; color:#f85149; border:1px solid #da3633; border-radius:20px; padding:2px 10px; font-size:0.75rem; }

/* Selectboxes / inputs */
[data-baseweb="select"] > div {
    background: #161b22 !important;
    border-color: #30363d !important;
    color: #c9d1d9 !important;
}
</style>
""", unsafe_allow_html=True)

# ──────────────────────────────────────────────
# Snowflake connection
# ──────────────────────────────────────────────
@st.cache_resource
def get_connection():
    return snowflake.connector.connect(
        user=st.secrets["SNOWFLAKE_USER"],
        password=st.secrets["SNOWFLAKE_PASSWORD"],
        account=st.secrets["SNOWFLAKE_ACCOUNT"],
        warehouse=st.secrets["SNOWFLAKE_WAREHOUSE"],
        database="MANUFACTURING",
        schema="ANALYTICS",
    )

conn = get_connection()

# ──────────────────────────────────────────────
# Data loaders
# ──────────────────────────────────────────────
@st.cache_data(ttl=600)
def load_production() -> pd.DataFrame:
    return pd.read_sql(
        "SELECT * FROM VW_PRODUCTION_KPI ORDER BY DATE DESC",
        conn,
    )

@st.cache_data(ttl=600)
def load_raw_material() -> pd.DataFrame:
    return pd.read_sql(
        "SELECT * FROM VW_RAW_MATERIAL_KPI ORDER BY MONTH DESC",
        conn,
    )

@st.cache_data(ttl=600)
def load_plant_summary() -> pd.DataFrame:
    return pd.read_sql(
        "SELECT * FROM VW_PLANT_SUMMARY",
        conn,
    )

# ──────────────────────────────────────────────
# Shared Altair theme
# ──────────────────────────────────────────────
CHART_CONFIG = {
    "config": {
        "background": "#161b22",
        "view": {"stroke": "transparent"},
        "axis": {
            "gridColor": "#21262d",
            "domainColor": "#30363d",
            "tickColor": "#30363d",
            "labelColor": "#8b949e",
            "titleColor": "#8b949e",
            "labelFont": "IBM Plex Mono",
            "titleFont": "IBM Plex Mono",
        },
        "legend": {
            "labelColor": "#8b949e",
            "titleColor": "#8b949e",
            "labelFont": "IBM Plex Mono",
        },
        "title": {"color": "#e6edf3", "font": "Syne", "fontSize": 14},
    }
}
alt.themes.register("dark_mfg", lambda: CHART_CONFIG)
alt.themes.enable("dark_mfg")


def fpy_badge(val: float) -> str:
    if val >= 92:
        return f'<span class="badge-good">✓ {val:.1f}%</span>'
    if val >= 85:
        return f'<span class="badge-warn">⚠ {val:.1f}%</span>'
    return f'<span class="badge-bad">✗ {val:.1f}%</span>'


# ══════════════════════════════════════════════
# PAGE 1 — Executive Dashboard
# ══════════════════════════════════════════════
def page_executive():
    st.markdown('<div class="section-title">📊 Executive Dashboard</div>', unsafe_allow_html=True)

    df = load_production()

    # ── Sidebar filters ──────────────────────
    with st.sidebar:
        st.markdown("### 🔍 Filters")

        plants = sorted(df["PLANT_NAME"].unique())
        sel_plants = st.multiselect("Plant(s)", plants, default=plants, key="exec_plants")

        products = sorted(df["PRODUCT_TYPE"].unique())
        sel_products = st.multiselect("Product(s)", products, default=products, key="exec_products")

        months = sorted(df["MONTH"].unique(), reverse=True)
        sel_months = st.multiselect("Month(s)", months, default=months[:3], key="exec_months")

        fpy_min = float(df["FPY_PERCENT"].min())
        fpy_max = float(df["FPY_PERCENT"].max())
        fpy_range = st.slider(
            "FPY % range",
            min_value=fpy_min,
            max_value=fpy_max,
            value=(fpy_min, fpy_max),
            step=0.5,
            key="exec_fpy",
        )

        energy_max = float(df["TOTAL_ENERGY_KWH"].max())
        energy_limit = st.slider(
            "Max Energy (kWh)",
            min_value=0.0,
            max_value=energy_max,
            value=energy_max,
            step=100.0,
            key="exec_energy",
        )

    # ── Apply filters ────────────────────────
    f = df.copy()
    if sel_plants:
        f = f[f["PLANT_NAME"].isin(sel_plants)]
    if sel_products:
        f = f[f["PRODUCT_TYPE"].isin(sel_products)]
    if sel_months:
        f = f[f["MONTH"].isin(sel_months)]
    f = f[f["FPY_PERCENT"].between(*fpy_range)]
    f = f[f["TOTAL_ENERGY_KWH"] <= energy_limit]

    if f.empty:
        st.warning("No data matches the current filters.")
        return

    # ── KPI metrics ──────────────────────────
    k1, k2, k3, k4, k5 = st.columns(5)
    k1.metric("Total Production", f"{int(f['PRODUCED_QTY'].sum()):,} units")
    k2.metric("Avg FPY", f"{f['FPY_PERCENT'].mean():.2f}%")
    k3.metric("Total Energy", f"{f['TOTAL_ENERGY_KWH'].sum():,.0f} kWh")
    k4.metric(
        "Energy / Unit",
        f"{f['TOTAL_ENERGY_KWH'].sum() / f['PRODUCED_QTY'].sum():.3f}" if f["PRODUCED_QTY"].sum() > 0 else "—",
    )
    k5.metric("Rows", f"{len(f):,}")

    st.markdown("---")

    # ── Charts row 1 ─────────────────────────
    c1, c2 = st.columns(2)

    with c1:
        st.markdown('<div class="section-title" style="font-size:1rem">Production Trend</div>', unsafe_allow_html=True)
        daily = f.groupby("DATE")["PRODUCED_QTY"].sum().reset_index()
        chart = (
            alt.Chart(daily)
            .mark_area(
                line={"color": "#58a6ff", "strokeWidth": 2},
                color=alt.Gradient(
                    gradient="linear",
                    stops=[
                        alt.GradientStop(color="#1f6feb44", offset=0),
                        alt.GradientStop(color="#1f6feb00", offset=1),
                    ],
                    x1=1, x2=1, y1=1, y2=0,
                ),
            )
            .encode(
                x=alt.X("DATE:T", title="Date"),
                y=alt.Y("PRODUCED_QTY:Q", title="Units"),
                tooltip=["DATE:T", "PRODUCED_QTY:Q"],
            )
        )
        st.altair_chart(chart, use_container_width=True)

    with c2:
        st.markdown('<div class="section-title" style="font-size:1rem">FPY by Plant</div>', unsafe_allow_html=True)
        plant_fpy = f.groupby("PLANT_NAME")["FPY_PERCENT"].mean().reset_index()
        chart = (
            alt.Chart(plant_fpy)
            .mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4)
            .encode(
                x=alt.X("PLANT_NAME:N", title="Plant"),
                y=alt.Y("FPY_PERCENT:Q", title="FPY %", scale=alt.Scale(domain=[0, 100])),
                color=alt.Color(
                    "FPY_PERCENT:Q",
                    scale=alt.Scale(scheme="redyellowgreen", domain=[80, 100]),
                    legend=None,
                ),
                tooltip=["PLANT_NAME:N", alt.Tooltip("FPY_PERCENT:Q", format=".2f")],
            )
        )
        st.altair_chart(chart, use_container_width=True)

    # ── Charts row 2 ─────────────────────────
    c3, c4 = st.columns(2)

    with c3:
        st.markdown('<div class="section-title" style="font-size:1rem">Production by Product Type</div>', unsafe_allow_html=True)
        by_product = f.groupby("PRODUCT_TYPE")["PRODUCED_QTY"].sum().reset_index()
        chart = (
            alt.Chart(by_product)
            .mark_arc(innerRadius=55)
            .encode(
                theta=alt.Theta("PRODUCED_QTY:Q"),
                color=alt.Color("PRODUCT_TYPE:N", scale=alt.Scale(scheme="tableau10")),
                tooltip=["PRODUCT_TYPE:N", "PRODUCED_QTY:Q"],
            )
        )
        st.altair_chart(chart, use_container_width=True)

    with c4:
        st.markdown('<div class="section-title" style="font-size:1rem">Energy Consumption Trend</div>', unsafe_allow_html=True)
        energy_trend = f.groupby("DATE")["TOTAL_ENERGY_KWH"].sum().reset_index()
        chart = (
            alt.Chart(energy_trend)
            .mark_line(color="#f78166", strokeWidth=2, point=alt.OverlayMarkDef(color="#f78166", size=30))
            .encode(
                x=alt.X("DATE:T", title="Date"),
                y=alt.Y("TOTAL_ENERGY_KWH:Q", title="kWh"),
                tooltip=["DATE:T", alt.Tooltip("TOTAL_ENERGY_KWH:Q", format=",.0f")],
            )
        )
        st.altair_chart(chart, use_container_width=True)

    # ── Data table ───────────────────────────
    st.markdown('<div class="section-title" style="font-size:1rem">Detailed Records</div>', unsafe_allow_html=True)
    st.dataframe(
        f.head(200).reset_index(drop=True),
        use_container_width=True,
        height=320,
    )


# ══════════════════════════════════════════════
# PAGE 2 — Raw Material Analytics
# ══════════════════════════════════════════════
def page_raw_material():
    st.markdown('<div class="section-title">🌳 Raw Material Analytics</div>', unsafe_allow_html=True)

    df = load_raw_material()

    # ── Sidebar filters ──────────────────────
    with st.sidebar:
        st.markdown("### 🔍 Filters")

        months = sorted(df["MONTH"].unique(), reverse=True)
        sel_months = st.multiselect("Month(s)", months, default=months[:6], key="rm_months")

        suppliers = sorted(df["SUPPLIER_NAME"].unique())
        sel_suppliers = st.multiselect("Supplier(s)", suppliers, default=suppliers, key="rm_suppliers")

        yield_min = float(df["LOG_YIELD_PERCENT"].min())
        yield_max = float(df["LOG_YIELD_PERCENT"].max())
        yield_range = st.slider(
            "Yield % range",
            min_value=yield_min,
            max_value=yield_max,
            value=(yield_min, yield_max),
            step=0.5,
            key="rm_yield",
        )

        cost_max = float(df["AVG_COST_PER_CFT"].max())
        cost_limit = st.slider(
            "Max Cost / CFT (₹)",
            min_value=0.0,
            max_value=cost_max,
            value=cost_max,
            step=1.0,
            key="rm_cost",
        )

    # ── Apply filters ────────────────────────
    f = df.copy()
    if sel_months:
        f = f[f["MONTH"].isin(sel_months)]
    if sel_suppliers:
        f = f[f["SUPPLIER_NAME"].isin(sel_suppliers)]
    f = f[f["LOG_YIELD_PERCENT"].between(*yield_range)]
    f = f[f["AVG_COST_PER_CFT"] <= cost_limit]

    if f.empty:
        st.warning("No data matches the current filters.")
        return

    # ── KPIs ─────────────────────────────────
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Total Logs", f"{int(f['TOTAL_LOG_QTY'].sum()):,} CFT")
    k2.metric("Avg Yield", f"{f['LOG_YIELD_PERCENT'].mean():.2f}%")
    k3.metric("Total Waste", f"{int(f['TOTAL_WASTE'].sum()):,} CFT")
    k4.metric("Avg Cost/CFT", f"₹{f['AVG_COST_PER_CFT'].mean():.2f}")

    st.markdown("---")

    # ── Charts row 1 ─────────────────────────
    c1, c2 = st.columns(2)

    with c1:
        st.markdown('<div class="section-title" style="font-size:1rem">Yield by Supplier</div>', unsafe_allow_html=True)
        supplier_yield = (
            f.groupby("SUPPLIER_NAME")["LOG_YIELD_PERCENT"]
            .mean()
            .reset_index()
            .sort_values("LOG_YIELD_PERCENT")
        )
        chart = (
            alt.Chart(supplier_yield)
            .mark_bar(cornerRadiusTopRight=4, cornerRadiusBottomRight=4)
            .encode(
                x=alt.X("LOG_YIELD_PERCENT:Q", title="Yield %", scale=alt.Scale(domain=[0, 100])),
                y=alt.Y("SUPPLIER_NAME:N", sort="-x", title="Supplier"),
                color=alt.Color(
                    "LOG_YIELD_PERCENT:Q",
                    scale=alt.Scale(scheme="redyellowgreen", domain=[70, 100]),
                    legend=None,
                ),
                tooltip=["SUPPLIER_NAME:N", alt.Tooltip("LOG_YIELD_PERCENT:Q", format=".2f")],
            )
        )
        st.altair_chart(chart, use_container_width=True)

    with c2:
        st.markdown('<div class="section-title" style="font-size:1rem">Monthly Yield Trend</div>', unsafe_allow_html=True)
        monthly_yield = f.groupby("MONTH")["LOG_YIELD_PERCENT"].mean().reset_index()
        chart = (
            alt.Chart(monthly_yield)
            .mark_line(color="#3fb950", strokeWidth=2, point=alt.OverlayMarkDef(color="#3fb950", size=40))
            .encode(
                x=alt.X("MONTH:N", title="Month"),
                y=alt.Y("LOG_YIELD_PERCENT:Q", title="Yield %", scale=alt.Scale(domain=[0, 100])),
                tooltip=["MONTH:N", alt.Tooltip("LOG_YIELD_PERCENT:Q", format=".2f")],
            )
        )
        st.altair_chart(chart, use_container_width=True)

    # ── Charts row 2 ─────────────────────────
    c3, c4 = st.columns(2)

    with c3:
        st.markdown('<div class="section-title" style="font-size:1rem">Cost per CFT by Supplier</div>', unsafe_allow_html=True)
        cost_sup = f.groupby("SUPPLIER_NAME")["AVG_COST_PER_CFT"].mean().reset_index()
        chart = (
            alt.Chart(cost_sup)
            .mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4)
            .encode(
                x=alt.X("SUPPLIER_NAME:N", title="Supplier"),
                y=alt.Y("AVG_COST_PER_CFT:Q", title="₹ / CFT"),
                color=alt.Color(
                    "AVG_COST_PER_CFT:Q",
                    scale=alt.Scale(scheme="orangered"),
                    legend=None,
                ),
                tooltip=["SUPPLIER_NAME:N", alt.Tooltip("AVG_COST_PER_CFT:Q", format=".2f")],
            )
        )
        st.altair_chart(chart, use_container_width=True)

    with c4:
        st.markdown('<div class="section-title" style="font-size:1rem">Waste by Month</div>', unsafe_allow_html=True)
        waste_month = f.groupby("MONTH")["TOTAL_WASTE"].sum().reset_index()
        chart = (
            alt.Chart(waste_month)
            .mark_bar(color="#f78166", cornerRadiusTopLeft=4, cornerRadiusTopRight=4)
            .encode(
                x=alt.X("MONTH:N", title="Month"),
                y=alt.Y("TOTAL_WASTE:Q", title="CFT Waste"),
                tooltip=["MONTH:N", alt.Tooltip("TOTAL_WASTE:Q", format=",")],
            )
        )
        st.altair_chart(chart, use_container_width=True)

    st.markdown('<div class="section-title" style="font-size:1rem">Raw Material Records</div>', unsafe_allow_html=True)
    st.dataframe(f.reset_index(drop=True), use_container_width=True, height=300)


# ══════════════════════════════════════════════
# PAGE 3 — Plant Performance
# ══════════════════════════════════════════════
def page_plant():
    st.markdown('<div class="section-title">🏭 Plant Performance</div>', unsafe_allow_html=True)

    df = load_plant_summary()

    # ── Sidebar filters ──────────────────────
    with st.sidebar:
        st.markdown("### 🔍 Filters")
        plants = sorted(df["PLANT_NAME"].unique())
        sel_plants = st.multiselect("Plant(s)", plants, default=plants, key="plant_sel")

        min_prod = int(df["TOTAL_PRODUCTION"].min())
        max_prod = int(df["TOTAL_PRODUCTION"].max())
        prod_range = st.slider(
            "Total Production range",
            min_value=min_prod,
            max_value=max_prod,
            value=(min_prod, max_prod),
            step=100,
            key="plant_prod",
        )

        fpy_min = float(df["AVG_FPY_PERCENT"].min())
        fpy_max = float(df["AVG_FPY_PERCENT"].max())
        fpy_range = st.slider(
            "Avg FPY % range",
            min_value=fpy_min,
            max_value=fpy_max,
            value=(fpy_min, fpy_max),
            step=0.5,
            key="plant_fpy",
        )

        sort_by = st.selectbox(
            "Sort plants by",
            ["TOTAL_PRODUCTION", "AVG_FPY_PERCENT", "TOTAL_ENERGY_CONSUMED", "CAPACITY"],
            key="plant_sort",
        )
        sort_asc = st.toggle("Ascending order", value=False, key="plant_sort_asc")

    # ── Apply filters ────────────────────────
    f = df.copy()
    if sel_plants:
        f = f[f["PLANT_NAME"].isin(sel_plants)]
    f = f[f["TOTAL_PRODUCTION"].between(*prod_range)]
    f = f[f["AVG_FPY_PERCENT"].between(*fpy_range)]
    f = f.sort_values(sort_by, ascending=sort_asc)

    if f.empty:
        st.warning("No plants match the current filters.")
        return

    # ── Summary KPIs ─────────────────────────
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Plants shown", len(f))
    k2.metric("Total Capacity", f"{int(f['CAPACITY'].sum()):,}")
    k3.metric("Total Production", f"{int(f['TOTAL_PRODUCTION'].sum()):,}")
    k4.metric("Avg FPY", f"{f['AVG_FPY_PERCENT'].mean():.2f}%")

    st.markdown("---")

    # ── Plant cards ───────────────────────────
    for _, row in f.iterrows():
        with st.container():
            col_title, col_badge = st.columns([5, 1])
            with col_title:
                st.markdown(
                    f"<h3 style='font-family:Syne;color:#e6edf3;margin:0'>"
                    f"🏭 {row['PLANT_NAME']} "
                    f"<span style='font-size:0.8rem;color:#8b949e;font-weight:400'>📍 {row['LOCATION']}</span>"
                    f"</h3>",
                    unsafe_allow_html=True,
                )
            with col_badge:
                st.markdown(fpy_badge(row["AVG_FPY_PERCENT"]), unsafe_allow_html=True)

            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Capacity", f"{int(row['CAPACITY']):,}")
            c2.metric("Production", f"{int(row['TOTAL_PRODUCTION']):,}")
            util = row["TOTAL_PRODUCTION"] / row["CAPACITY"] * 100 if row["CAPACITY"] > 0 else 0
            c3.metric("Utilization", f"{util:.1f}%")
            c4.metric("Energy", f"{row['TOTAL_ENERGY_CONSUMED']:,.0f} kWh")
            st.markdown("---")

    # ── Comparison chart ─────────────────────
    st.markdown('<div class="section-title" style="font-size:1rem">Plant Comparison</div>', unsafe_allow_html=True)

    tab_a, tab_b, tab_c = st.tabs(["Production", "FPY %", "Energy"])

    with tab_a:
        chart = (
            alt.Chart(f)
            .mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4, color="#58a6ff")
            .encode(
                x=alt.X("PLANT_NAME:N", sort="-y", title="Plant"),
                y=alt.Y("TOTAL_PRODUCTION:Q", title="Units"),
                tooltip=["PLANT_NAME:N", "TOTAL_PRODUCTION:Q"],
            )
        )
        st.altair_chart(chart, use_container_width=True)

    with tab_b:
        chart = (
            alt.Chart(f)
            .mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4)
            .encode(
                x=alt.X("PLANT_NAME:N", sort="-y", title="Plant"),
                y=alt.Y("AVG_FPY_PERCENT:Q", title="FPY %", scale=alt.Scale(domain=[0, 100])),
                color=alt.Color(
                    "AVG_FPY_PERCENT:Q",
                    scale=alt.Scale(scheme="redyellowgreen", domain=[80, 100]),
                    legend=None,
                ),
                tooltip=["PLANT_NAME:N", alt.Tooltip("AVG_FPY_PERCENT:Q", format=".2f")],
            )
        )
        st.altair_chart(chart, use_container_width=True)

    with tab_c:
        chart = (
            alt.Chart(f)
            .mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4, color="#f78166")
            .encode(
                x=alt.X("PLANT_NAME:N", sort="-y", title="Plant"),
                y=alt.Y("TOTAL_ENERGY_CONSUMED:Q", title="kWh"),
                tooltip=["PLANT_NAME:N", alt.Tooltip("TOTAL_ENERGY_CONSUMED:Q", format=",.0f")],
            )
        )
        st.altair_chart(chart, use_container_width=True)


# ══════════════════════════════════════════════
# PAGE 4 — Energy & Efficiency
# ══════════════════════════════════════════════
def page_energy():
    st.markdown('<div class="section-title">⚡ Energy & Efficiency</div>', unsafe_allow_html=True)

    df = load_production()

    with st.sidebar:
        st.markdown("### 🔍 Filters")

        plants = sorted(df["PLANT_NAME"].unique())
        sel_plants = st.multiselect("Plant(s)", plants, default=plants, key="en_plants")

        months = sorted(df["MONTH"].unique(), reverse=True)
        sel_months = st.multiselect("Month(s)", months, default=months, key="en_months")

        group_by = st.selectbox("Group charts by", ["PLANT_NAME", "PRODUCT_TYPE", "MONTH"], key="en_group")

    f = df.copy()
    if sel_plants:
        f = f[f["PLANT_NAME"].isin(sel_plants)]
    if sel_months:
        f = f[f["MONTH"].isin(sel_months)]

    if f.empty:
        st.warning("No data matches the current filters.")
        return

    # ── KPIs ─────────────────────────────────
    total_prod = f["PRODUCED_QTY"].sum()
    total_energy = f["TOTAL_ENERGY_KWH"].sum()
    epu = total_energy / total_prod if total_prod > 0 else 0

    k1, k2, k3 = st.columns(3)
    k1.metric("Total Production", f"{int(total_prod):,} units")
    k2.metric("Total Energy", f"{total_energy:,.0f} kWh")
    k3.metric("Energy / Unit", f"{epu:.3f} kWh")

    st.markdown("---")

    # ── Scatter: Energy vs Production ────────
    st.markdown('<div class="section-title" style="font-size:1rem">Energy vs Production (by plant)</div>', unsafe_allow_html=True)
    scatter_df = f.groupby(group_by).agg(
        PRODUCED_QTY=("PRODUCED_QTY", "sum"),
        TOTAL_ENERGY_KWH=("TOTAL_ENERGY_KWH", "sum"),
    ).reset_index()

    chart = (
        alt.Chart(scatter_df)
        .mark_circle(size=120, opacity=0.85)
        .encode(
            x=alt.X("PRODUCED_QTY:Q", title="Production (units)"),
            y=alt.Y("TOTAL_ENERGY_KWH:Q", title="Energy (kWh)"),
            color=alt.Color(f"{group_by}:N", scale=alt.Scale(scheme="tableau10")),
            tooltip=[f"{group_by}:N", "PRODUCED_QTY:Q", alt.Tooltip("TOTAL_ENERGY_KWH:Q", format=",.0f")],
        )
    )
    st.altair_chart(chart, use_container_width=True)

    # ── Energy per unit heatmap ───────────────
    st.markdown('<div class="section-title" style="font-size:1rem">Energy / Unit Heatmap (Plant × Month)</div>', unsafe_allow_html=True)
    heat_df = (
        f.groupby(["PLANT_NAME", "MONTH"])
        .apply(lambda x: x["TOTAL_ENERGY_KWH"].sum() / x["PRODUCED_QTY"].sum() if x["PRODUCED_QTY"].sum() > 0 else 0)
        .reset_index(name="EPU")
    )
    chart = (
        alt.Chart(heat_df)
        .mark_rect()
        .encode(
            x=alt.X("MONTH:N", title="Month"),
            y=alt.Y("PLANT_NAME:N", title="Plant"),
            color=alt.Color(
                "EPU:Q",
                scale=alt.Scale(scheme="orangered"),
                title="kWh/unit",
            ),
            tooltip=["PLANT_NAME:N", "MONTH:N", alt.Tooltip("EPU:Q", format=".3f")],
        )
    )
    st.altair_chart(chart, use_container_width=True)


# ══════════════════════════════════════════════
# Navigation
# ══════════════════════════════════════════════
PAGES = {
    "📊 Executive Dashboard": page_executive,
    "🌳 Raw Material Analytics": page_raw_material,
    "🏭 Plant Performance": page_plant,
    "⚡ Energy & Efficiency": page_energy,
}

with st.sidebar:
    st.markdown(
        "<div style='font-family:Syne;font-size:1.4rem;font-weight:800;"
        "color:#58a6ff;padding:0.5rem 0 1rem 0'>🌲 Poplar Mfg</div>",
        unsafe_allow_html=True,
    )
    page_name = st.radio("Navigation", list(PAGES.keys()), label_visibility="collapsed")
    st.markdown("---")
    st.markdown(
        "<small style='color:#484f58'>Powered by Snowflake + Streamlit</small>",
        unsafe_allow_html=True,
    )

PAGES[page_name]()
