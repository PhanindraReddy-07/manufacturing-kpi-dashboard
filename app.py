import streamlit as st
import pandas as pd
import altair as alt
import snowflake.connector

# --------------------------------------------------
# Page configuration
# --------------------------------------------------
st.set_page_config(
    page_title="Manufacturing KPI Dashboard",
    page_icon="🏭",
    layout="wide"
)

# --------------------------------------------------
# Snowflake connection (EXTERNAL)
# --------------------------------------------------
@st.cache_resource
def get_connection():
    return snowflake.connector.connect(
        user=st.secrets["SNOWFLAKE_USER"],
        password=st.secrets["SNOWFLAKE_PASSWORD"],
        account=st.secrets["SNOWFLAKE_ACCOUNT"],
        warehouse=st.secrets["SNOWFLAKE_WAREHOUSE"],
        database="MANUFACTURING",
        schema="ANALYTICS"
    )

conn = get_connection()

# --------------------------------------------------
# Header
# --------------------------------------------------
st.markdown(
    "<h1 style='text-align:center;color:#1f77b4'>🌲 Poplar Manufacturing Dashboard</h1>",
    unsafe_allow_html=True
)
st.markdown("---")

# --------------------------------------------------
# Load data
# --------------------------------------------------
@st.cache_data
def load_production():
    return pd.read_sql(
        "SELECT * FROM VW_PRODUCTION_KPI ORDER BY DATE DESC",
        conn
    )

df = load_production()

# --------------------------------------------------
# Filters
# --------------------------------------------------
c1, c2, c3 = st.columns(3)

with c1:
    plant = st.selectbox(
        "Plant",
        ["All"] + sorted(df["PLANT_NAME"].unique()),
        key="plant"
    )

with c2:
    product = st.selectbox(
        "Product",
        ["All"] + sorted(df["PRODUCT_TYPE"].unique()),
        key="product"
    )

with c3:
    month = st.selectbox(
        "Month",
        ["All"] + sorted(df["MONTH"].unique(), reverse=True),
        key="month"
    )

filtered = df.copy()

if plant != "All":
    filtered = filtered[filtered["PLANT_NAME"] == plant]
if product != "All":
    filtered = filtered[filtered["PRODUCT_TYPE"] == product]
if month != "All":
    filtered = filtered[filtered["MONTH"] == month]

# --------------------------------------------------
# KPIs
# --------------------------------------------------
k1, k2, k3, k4 = st.columns(4)

k1.metric("Total Production", f"{int(filtered['PRODUCED_QTY'].sum()):,}")
k2.metric("Avg FPY", f"{filtered['FPY_PERCENT'].mean():.2f}%")
k3.metric("Total Energy", f"{filtered['TOTAL_ENERGY_KWH'].sum():,.0f} kWh")
k4.metric(
    "Efficiency",
    f"{(filtered['PRODUCED_QTY'].sum() / filtered['TOTAL_ENERGY_KWH'].sum()):.3f}"
    if filtered['TOTAL_ENERGY_KWH'].sum() > 0 else "0"
)

st.markdown("---")

# --------------------------------------------------
# Chart
# --------------------------------------------------
daily = filtered.groupby("DATE")["PRODUCED_QTY"].sum().reset_index()

chart = (
    alt.Chart(daily)
    .mark_line(point=True)
    .encode(
        x="DATE:T",
        y="PRODUCED_QTY:Q"
    )
)

st.altair_chart(chart, use_container_width=True)

# --------------------------------------------------
# Table
# --------------------------------------------------
st.dataframe(filtered.head(100).reset_index(drop=True), use_container_width=True)

# --------------------------------------------------
# Footer
# --------------------------------------------------
st.markdown(
    "<center><small>Real-world Manufacturing Dashboard • Powered by Snowflake</small></center>",
    unsafe_allow_html=True
)
