import streamlit as st
import gridstatus
import pandas as pd
import plotly.express as px

# 1. Page Configuration
st.set_page_config(page_title="ERCOT Live LMP Tracker", layout="wide")
st.title("⚡ ERCOT Real-Time LMP Dashboard")
st.markdown("Live 5-minute Locational Marginal Pricing directly from the ERCOT market.")

# 2. Fetch Data
@st.cache_data(ttl=300) # Cache clears every 5 minutes
def load_live_ercot_data():
    ercot = gridstatus.Ercot()
    df = ercot.get_lmp(date="today")
    return df

with st.spinner("Fetching live ERCOT data..."):
    df_live = load_live_ercot_data()

# 3. Data Filtering (Fault-tolerant Hub selection)
if 'Location Type' in df_live.columns:
    # Look for 'hub' regardless of how it is capitalized
    hubs_only = df_live[df_live['Location Type'].str.lower() == 'hub']
else:
    # Fallback: ERCOT hubs usually start with 'HB_' (e.g., HB_NORTH)
    hubs_only = df_live[df_live['Location'].str.startswith('HB_')]

available_hubs = hubs_only['Location'].unique()

# Provide a default selection only if there are hubs available to select
default_selections = available_hubs[:3] if len(available_hubs) >= 3 else available_hubs
selected_hubs = st.multiselect("Select Hubs to Graph:", available_hubs, default=default_selections)

# Filter dataframe based on your selection
df_filtered = hubs_only[hubs_only['Location'].isin(selected_hubs)]

# 4. Visualization
st.subheader("Live Pricing Trends")
if not df_filtered.empty:
    fig = px.line(
        df_filtered, 
        x="Time", 
        y="LMP", 
        color="Location",
        title="Real-Time LMP over Time ($/MWh)",
        markers=True
    )
    st.plotly_chart(fig, use_container_width=True)
else:
    st.warning("Please select at least one hub to display the graph.")

# 5. Show the Raw Data (Dynamically checking columns to prevent KeyErrors)
st.subheader("Raw Data Extract")

# Start with the core columns we know ERCOT always has
cols_to_show = ['Time', 'Location', 'LMP']

# Add these extra columns only if ERCOT actually provided them today
for optional_col in ['Energy', 'Congestion', 'Loss', 'Location Type']:
    if optional_col in df_filtered.columns:
        cols_to_show.append(optional_col)

st.dataframe(df_filtered[cols_to_show], use_container_width=True)
