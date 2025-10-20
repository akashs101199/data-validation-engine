import streamlit as st
import pandas as pd
import os
import plotly.express as px
import plotly.graph_objects as go

# --- Page Configuration ---
st.set_page_config(
    page_title="Data Validation & Quality Hub",
    page_icon="✅",
    layout="wide",
    initial_sidebar_state="expanded",
)

# --- Custom CSS for a Polished Look ---
st.markdown("""
    <style>
        .reportview-container .main .block-container {
            padding-top: 2rem;
            padding-right: 2rem;
            padding-left: 2rem;
            padding-bottom: 2rem;
        }
        .stMetric {
            background-color: #f0f2f6;
            border-radius: 10px;
            padding: 15px;
            box-shadow: 0 4px 8px rgba(0,0,0,0.1);
        }
        h1, h2, h3 {
            color: #1f77b4; /* A nice blue color */
        }
    </style>
""", unsafe_allow_html=True)

# --- File Paths ---
VALIDATED_DATA_PATH = "data/validated_output.csv"
FAILED_ROWS_PATH = "reports/failed_rows.csv"
ANOMALOUS_DATA_PATH = "data/anomalous_records.csv"
DRIFT_REPORT_PATH = "dashboards/drift_report.html"

# --- Helper Functions ---
@st.cache_data
def load_data(path):
    if os.path.exists(path) and os.path.getsize(path) > 0:
        return pd.read_csv(path)
    return None

def display_kpis(df):
    total_rows = df.shape[0]
    total_cols = df.shape[1]
    total_nulls = df.isnull().sum().sum()
    
    col1, col2, col3 = st.columns(3)
    col1.metric(label="**Total Rows**", value=f"{total_rows:,}")
    col2.metric(label="**Total Columns**", value=total_cols)
    col3.metric(label="**Missing Values**", value=f"{total_nulls:,}")

# --- Load Data ---
df_validated = load_data(VALIDATED_DATA_PATH)
df_failed = load_data(FAILED_ROWS_PATH)
df_anomalous = load_data(ANOMALOUS_DATA_PATH)

# --- Sidebar Navigation ---
st.sidebar.title("📊 Navigation")
page = st.sidebar.radio("Go to", [
    "Executive Summary", 
    "Deep Dive: Data Explorer",
    "Quality Issues: Failed Rows",
    "Advanced: Anomaly Detection",
    "Advanced: Drift Report"
])

# ==============================================================================
# --- Page 1: Executive Summary ---
# ==============================================================================
if page == "Executive Summary":
    st.title("Executive Summary: Data Quality at a Glance")
    st.markdown("---")
    
    if df_validated is not None:
        st.header("Overall Dataset Health")
        display_kpis(df_validated)
        st.markdown("---")

        col1, col2 = st.columns(2)

        with col1:
            st.header("Data Quality Scorecard")
            total_records = len(df_validated)
            failed_count = len(df_failed) if df_failed is not None else 0
            anomalous_count = len(df_anomalous) if df_anomalous is not None else 0
            
            pass_rate = ((total_records - failed_count) / total_records) * 100 if total_records > 0 else 100
            
            st.metric(label="**Business Rule Pass Rate**", value=f"{pass_rate:.2f}%")
            st.metric(label="**Rows with Quality Issues**", value=f"{failed_count:,}")
            st.metric(label="**Detected Anomalies**", value=f"{anomalous_count:,}")

        with col2:
            st.header("Key Column Distributions")
            if 'Location' in df_validated.columns:
                fig = px.pie(df_validated, names='Location', title='Customer Distribution by Location', hole=0.3)
                st.plotly_chart(fig, use_container_width=True)

    else:
        st.warning("No validated data found. Please run the main pipeline.")

# ==============================================================================
# --- Page 2: Deep Dive Data Explorer ---
# ==============================================================================
elif page == "Deep Dive: Data Explorer":
    st.title("Deep Dive: Interactive Data Explorer")
    st.markdown("Explore the clean, validated dataset.")
    
    if df_validated is not None:
        # Interactive Filtering
        st.sidebar.header("Filter Data")
        location_filter = st.sidebar.multiselect(
            'Filter by Location:',
            options=df_validated['Location'].unique(),
            default=df_validated['Location'].unique()
        )
        
        income_filter = st.sidebar.multiselect(
            'Filter by Income Level:',
            options=df_validated['Income_Level'].unique(),
            default=df_validated['Income_Level'].unique()
        )
        
        filtered_df = df_validated[
            df_validated['Location'].isin(location_filter) &
            df_validated['Income_Level'].isin(income_filter)
        ]

        st.dataframe(filtered_df)
        st.markdown("---")
        
        st.header("Visualize Data Distributions")
        col_to_plot = st.selectbox("Select a column to visualize:", options=filtered_df.select_dtypes(include='number').columns)
        fig = px.histogram(filtered_df, x=col_to_plot, title=f"Distribution of {col_to_plot}", nbins=50)
        st.plotly_chart(fig, use_container_width=True)
        
    else:
        st.warning("No validated data to explore.")

# ==============================================================================
# --- Page 3: Failed Rows Analysis ---
# ==============================================================================
elif page == "Quality Issues: Failed Rows":
    st.title("Analysis of Rows with Quality Issues")
    
    if df_failed is not None and not df_failed.empty:
        st.metric(label="Total Rows Failing Business Rules", value=len(df_failed))
        st.dataframe(df_failed)
        
        # Breakdown of failures
        if 'failure_reason' in df_failed.columns:
            st.header("Breakdown of Failures by Reason")
            failure_counts = df_failed['failure_reason'].value_counts()
            fig = px.bar(failure_counts, x=failure_counts.index, y=failure_counts.values, title="Count of Failures by Rule")
            st.plotly_chart(fig, use_container_width=True)
            
    else:
        st.success("🎉 No failed rows detected! Your data is clean.")

# ==============================================================================
# --- Page 4: Anomaly Detection ---
# ==============================================================================
elif page == "Advanced: Anomaly Detection":
    st.title("Anomaly Detection Insights")
    
    if df_anomalous is not None and not df_anomalous.empty:
        st.metric(label="Total Anomalous Records Detected", value=len(df_anomalous))
        st.dataframe(df_anomalous)
        
        st.header("Explore Anomalies in Context")
        numeric_cols = df_anomalous.select_dtypes(include='number').columns.tolist()
        
        col1, col2 = st.columns(2)
        x_axis = col1.selectbox("Select X-axis for scatter plot:", options=numeric_cols, index=0)
        y_axis = col2.selectbox("Select Y-axis for scatter plot:", options=numeric_cols, index=1 if len(numeric_cols)>1 else 0)
        
        fig = px.scatter(df_anomalous, x=x_axis, y=y_axis, title=f"Anomalies: {x_axis} vs. {y_axis}")
        st.plotly_chart(fig, use_container_width=True)

    else:
        st.info("No anomalous records were found in the dataset.")

# ==============================================================================
# --- Page 5: Drift Report ---
# ==============================================================================
elif page == "Advanced: Drift Report":
    st.title("Data Drift Detection Report")
    
    if os.path.exists(DRIFT_REPORT_PATH):
        with open(DRIFT_REPORT_PATH, 'r') as f:
            html_content = f.read()
        st.components.v1.html(html_content, height=600, scrolling=True)
    else:
        st.warning("Drift report not found. Please ensure the pipeline has run successfully.")