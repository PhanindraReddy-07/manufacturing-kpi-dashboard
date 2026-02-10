import streamlit as st
import pandas as pd
import altair as alt
from snowflake.snowpark.context import get_active_session

# --------------------------------------------------
# Page config
# --------------------------------------------------
st.set_page_config(
    page_title="Manufacturing KPI Dashboard",
    page_icon="🏭",
    layout="wide"
)

session = get_active_session()

# --------------------------------------------------
# Header
# --------------------------------------------------
st.markdown(
    "<h1 style='text-align:center;color:#1f77b4'>🌲 Poplar Manufacturing Dashboard</h1>",
    unsafe_allow_html=True
)
st.markdown("---")

# --------------------------------------------------
# LOAD DATA (ONCE)
# --------------------------------------------------
@st.cache_data
def load_production():
    return session.sql("""
        SELECT *
        FROM MANUFACTURING.ANALYTICS.VW_PRODUCTION_KPI
    """).to_pandas()

@st.cache_data
def load_rm():
    return session.sql("""
        SELECT *
        FROM MANUFACTURING.ANALYTICS.VW_RAW_MATERIAL_KPI
    """).to_pandas()

@st.cache_data
def load_plant():
    return session.sql("""
        SELECT *
        FROM MANUFACTURING.ANALYTICS.VW_PLANT_SUMMARY
    """).to_pandas()

df_prod = load_production()
df_rm = load_rm()
df_plant = load_plant()

# --------------------------------------------------
# 🌍 GLOBAL FILTERS (UNIQUE FEATURE)
# --------------------------------------------------
st.subheader("🔎 Global Filters")

g1, g2, g3, g4 = st.columns(4)

with g1:
    plant = st.selectbox(
        "Plant",
        ["All"] + sorted(df_prod["PLANT_NAME"].unique()),
        key="g_plant"
    )

with g2:
    product = st.selectbox(
        "Product",
        ["All"] + sorted(df_prod["PRODUCT_TYPE"].unique()),
        key="g_product"
    )

with g3:
    month = st.selectbox(
        "Month",
        ["All"] + sorted(df_prod["MONTH"].unique(), reverse=True),
        key="g_month"
    )

with g4:
    date_range = st.date_input(
        "Date Range",
        [df_prod["DATE"].min(), df_prod["DATE"].max()],
        key="g_date"
    )

# Apply global filters
filtered_prod = df_prod.copy()

if plant != "All":
    filtered_prod = filtered_prod[filtered_prod["PLANT_NAME"] == plant]

if product != "All":
    filtered_prod = filtered_prod[filtered_prod["PRODUCT_TYPE"] == product]

if month != "All":
    filtered_prod = filtered_prod[filtered_prod["MONTH"] == month]

filtered_prod = filtered_prod[
    (filtered_prod["DATE"] >= pd.to_datetime(date_range[0])) &
    (filtered_prod["DATE"] <= pd.to_datetime(date_range[1]))
]

st.markdown("---")

# --------------------------------------------------
# Tabs
# --------------------------------------------------
tab1, tab2, tab3 = st.tabs([
    "📊 Executive Dashboard",
    "🌳 Raw Material Analytics",
    "🏭 Plant Summary"
])

# ==================================================
# TAB 1: EXECUTIVE DASHBOARD
# ==================================================
with tab1:
    st.subheader("📊 Production KPIs")

    c1, c2, c3, c4 = st.columns(4)

    total_prod = filtered_prod["PRODUCED_QTY"].sum()
    avg_fpy = filtered_prod["FPY_PERCENT"].mean()
    energy = filtered_prod["TOTAL_ENERGY_KWH"].sum()
    efficiency = total_prod / energy if energy > 0 else 0

    c1.metric("Total Production", f"{int(total_prod):,}")
    c2.metric("Average FPY", f"{avg_fpy:.2f}%")
    c3.metric("Total Energy", f"{energy:,.0f} kWh")
    c4.metric("Efficiency", f"{efficiency:.3f} units/kWh")

    st.markdown("---")

    # 🔥 Production Trend
    daily = filtered_prod.groupby("DATE")["PRODUCED_QTY"].sum().reset_index()

    st.altair_chart(
        alt.Chart(daily)
        .mark_line(point=True)
        .encode(x="DATE:T", y="PRODUCED_QTY:Q"),
        use_container_width=True
    )

    # 🔥 Plant Ranking (UNIQUE)
    ranking = (
        filtered_prod.groupby("PLANT_NAME")["PRODUCED_QTY"]
        .sum()
        .reset_index()
        .sort_values("PRODUCED_QTY", ascending=False)
    )

    st.subheader("🏆 Plant Production Ranking")

    st.altair_chart(
        alt.Chart(ranking)
        .mark_bar()
        .encode(
            x="PRODUCED_QTY:Q",
            y=alt.Y("PLANT_NAME:N", sort="-x"),
            color="PRODUCED_QTY:Q"
        ),
        use_container_width=True
    )

    st.dataframe(filtered_prod.head(100).reset_index(drop=True), use_container_width=True)

# ==================================================
# TAB 2: RAW MATERIAL ANALYTICS
# ==================================================
with tab2:
    st.subheader("🌳 Raw Material Insights")

    filtered_rm = df_rm.copy()
    if month != "All":
        filtered_rm = filtered_rm[filtered_rm["MONTH"] == month]

    c1, c2, c3 = st.columns(3)
    c1.metric("Total Logs", f"{filtered_rm['TOTAL_LOG_QTY'].sum():,.0f} CFT")
    c2.metric("Avg Yield", f"{filtered_rm['LOG_YIELD_PERCENT'].mean():.2f}%")
    c3.metric("Waste Generated", f"{filtered_rm['TOTAL_WASTE'].sum():,.0f} CFT")

    st.markdown("---")

    yield_chart = (
        filtered_rm.groupby("SUPPLIER_NAME")["LOG_YIELD_PERCENT"]
        .mean()
        .reset_index()
    )

    st.altair_chart(
        alt.Chart(yield_chart)
        .mark_bar()
        .encode(
            x="LOG_YIELD_PERCENT:Q",
            y=alt.Y("SUPPLIER_NAME:N", sort="-x"),
            color="LOG_YIELD_PERCENT:Q"
        ),
        use_container_width=True
    )

# ==================================================
# TAB 3: PLANT SUMMARY
# ==================================================
with tab3:
    st.subheader("🏭 Plant Health Summary")

    for _, row in df_plant.iterrows():
        status = "🟢 Good" if row["AVG_FPY_PERCENT"] > 90 else "🟡 Average" if row["AVG_FPY_PERCENT"] > 80 else "🔴 Poor"

        st.markdown(f"### {row['PLANT_NAME']} ({row['LOCATION']}) — {status}")

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Capacity", f"{row['CAPACITY']:,}")
        c2.metric("Production", f"{row['TOTAL_PRODUCTION']:,}")
        c3.metric("FPY", f"{row['AVG_FPY_PERCENT']:.2f}%")
        c4.metric("Energy", f"{row['TOTAL_ENERGY_CONSUMED']:,.0f} kWh")

        st.markdown("---")

# --------------------------------------------------
# Footer
# --------------------------------------------------
st.markdown(
    "<center><small>Manufacturing KPI Dashboard • Real-World Analytics • Snowflake Powered</small></center>",
    unsafe_allow_html=True
)
