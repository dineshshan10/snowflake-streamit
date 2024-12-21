import streamlit as st
import snowflake.connector
import pandas as pd
import plotly.express as px

# Title of the app
st.title("Snowflake Benchmarking Insights")
st.write("This app visualizes Snowflake performance benchmarking data.")

# Sidebar for Snowflake connection details
st.sidebar.header("Snowflake Connection")
account = st.sidebar.text_input("Account", value="your_account_name")
user = st.sidebar.text_input("User", value="your_username")
password = st.sidebar.text_input("Password", value="", type="password")
database = st.sidebar.text_input("Database", value="SNOWFLAKE_SAMPLE_DATA")
warehouse = st.sidebar.text_input("Warehouse", value="COMPUTE_WH")

# Function to connect to Snowflake
@st.cache_resource
def connect_to_snowflake():
    conn = snowflake.connector.connect(
        account=account,
        user=user,
        password=password,
        database=database,
        warehouse=warehouse
    )
    return conn

# Query benchmarking data
def fetch_benchmarking_data(conn):
    query = """
    SELECT *
    FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.LINEITEM
    LIMIT 1000
    """
    return pd.read_sql(query, conn)

# Connect to Snowflake and fetch data
if st.sidebar.button("Fetch Data"):
    try:
        conn = connect_to_snowflake()
        st.write("Connected to Snowflake!")
        
        # Fetch data
        data = fetch_benchmarking_data(conn)
        st.write("Sample Benchmarking Data:")
        st.dataframe(data.head())
        
        # Visualization
        st.write("Visualization: Distribution of Discounts")
        fig = px.histogram(data, x='L_DISCOUNT', nbins=20, title="Discount Distribution")
        st.plotly_chart(fig)
        
        st.write("Visualization: Average Quantity by Ship Mode")
        avg_quantity = data.groupby("L_SHIPMODE")["L_QUANTITY"].mean().reset_index()
        fig2 = px.bar(avg_quantity, x="L_SHIPMODE", y="L_QUANTITY", title="Average Quantity by Ship Mode")
        st.plotly_chart(fig2)
        
    except Exception as e:
        st.error(f"Error: {e}")
