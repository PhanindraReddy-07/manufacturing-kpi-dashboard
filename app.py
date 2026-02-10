import streamlit as st
import pandas as pd
import altair as alt
import snowflake.connector

# --------------------------------------------------
# Page config
# --------------------------------------------------
st.set_page_config(
    page_title="Manufacturing KPI Dashboard",
    page_icon="🏭",
    layout="wide"
)

# --------------------------------------------------
# Snowflake connection (cached)
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
st.markdown("<h1 style='text-align:center'>🌲 Poplar Manufacturing Dashboard</h1>", unsafe_allow_html=True)
st.markdown("---")

# --------------------------------------------------
# Load data
# --------------------------------------------------
@st.cache_data
def load_production_data():
    return pd.read_sql(
        "SELECT * FROM VW_PRODUCTION_KPI ORDER BY DATE DESC",
        conn
    )

df = load_production_data()

# --------------------------------------------------
# Filters
# --------------------------------------------------
col1, col2 = st.columns(2)

with col1:
    plant = st.selectbox(
        "Plant",
        ["All"] + sorted(df["PLANT_NAME"].unique()),
        key="plant"
    )

with col2:
    month = st.selectbox(
        "Month",
        ["All"] + sorted(df["MONTH"].unique(), reverse=True),
        key="month"
    )

if plant != "All":
    df = df[df["PLANT_NAME"] == plant]
if month != "All":
    df = df[df["MONTH"] == month]

# --------------------------------------------------
# Metrics
# --------------------------------------------------
c1, c2, c3 = st.columns(3)

c1.metric("Total Production", f"{int(df['PRODUCED_QTY'].sum()):,}")
c2.metric("Avg FPY", f"{df['FPY_PERCENT'].mean():.2f}%")
c3.metric("Energy Used", f"{df['TOTAL_ENERGY_KWH'].sum():,.0f} kWh")

st.markdown("---")

# --------------------------------------------------
# Chart
# --------------------------------------------------
daily = df.groupby("DATE")["PRODUCED_QTY"].sum().reset_index()

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
st.dataframe(df.head(100), use_container_width=True)
