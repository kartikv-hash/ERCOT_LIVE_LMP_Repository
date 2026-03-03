import streamlit as st
import gridstatus
import pandas as pd
import plotly.express as px

# 1. Page Configuration
st.set_page_config(page_title="ERCOT Live LMP Tracker", layout="wide")
st.title("⚡ ERCOT Real-Time LMP Dashboard")
st.markdown("Live 5-minute Locational Marginal Pricing directly from the ERCOT market.")

# 2. Fetch Data (Cached so it doesn't overload the API every time you click a button)
@st.cache_data(ttl=300) # Cache clears every 5 minutes (300 seconds)
def load_live_ercot_data():
    ercot = gridstatus.ERCOT()
    # Fetch today's live 5-minute data
    df = ercot.get_lmp(date="today", market="REAL_TIME_5_MIN")
    return df

with st.spinner("Fetching live ERCOT data..."):
    df_live = load_live_ercot_data()

# 3. Data Filtering (Let's focus on the major Hubs to keep the chart clean)
hubs_only = df_live[df_live['Location Type'] == 'HUB']

# Create a multiselect box so you can choose which hubs to view
available_hubs = hubs_only['Location'].unique()
selected_hubs = st.multiselect("Select Hubs to Graph:", available_hubs, default=available_hubs[:3])

# Filter dataframe based on your selection
df_filtered = hubs_only[hubs_only['Location'].isin(selected_hubs)]

# 4. Visualization (Replacing Excel)
st.subheader("Live Pricing Trends")
if not df_filtered.empty:
    # Plotly makes highly interactive charts (zoom, pan, hover)
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

# 5. Show the Raw Data (The Repository View)
st.subheader("Raw Data Extract")
st.dataframe(df_filtered[['Time', 'Location', 'LMP', 'Energy', 'Congestion', 'Loss']], use_container_width=True)
