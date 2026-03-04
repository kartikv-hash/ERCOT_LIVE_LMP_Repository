# app.py
import streamlit as st
import pandas as pd
import plotly.express as px
import requests
from datetime import datetime, timedelta
from supabase import create_client

# ── Supabase ─────────────────────────────────────────────────
@st.cache_resource
def get_supabase():
    return create_client(st.secrets["SUPABASE_URL"], st.secrets["SUPABASE_KEY"])

supabase = get_supabase()
HUBS = ["HB_BUSAVG", "HB_HOUSTON", "HB_NORTH", "HB_PAN", "HB_SOUTH", "HB_WEST"]

# ── ERCOT Fetch (subscription key only) ──────────────────────
def fetch_ercot(delivery_date: str) -> pd.DataFrame:
    url = "https://api.ercot.com/api/public-reports/np6-905-cd/spp_node_zone_hub"
    headers = {"Ocp-Apim-Subscription-Key": st.secrets["ERCOT_API_KEY"]}
    params  = {
        "deliveryDateFrom": delivery_date,
        "deliveryDateTo":   delivery_date,
        "size": 10000,
    }
    r = requests.get(url, headers=headers, params=params, timeout=30)
    r.raise_for_status()
    rows = []
    for rec in r.json().get("data", []):
        hub = rec.get("settlementPoint", "")
        if hub in HUBS:
            rows.append({
                "timestamp":     rec.get("deliveryDate") + "T" + f"{int(rec.get('deliveryHour', 0)):02d}:00:00",
                "hub":           hub,
                "lmp":           float(rec.get("settlementPointPrice", 0)),
                "delivery_date": rec.get("deliveryDate"),
                "delivery_hour": int(rec.get("deliveryHour", 0)),
                "interval":      int(rec.get("deliveryInterval", 1)),
            })
    df = pd.DataFrame(rows)
    if not df.empty:
        df["timestamp"] = pd.to_datetime(df["timestamp"])
    return df

# ── Supabase Write ────────────────────────────────────────────
def save_to_supabase(df: pd.DataFrame) -> int:
    if df.empty:
        return 0
    records = df.copy()
    records["timestamp"] = records["timestamp"].astype(str)
    supabase.table("ercot_lmp").upsert(
        records.to_dict(orient="records"),
        on_conflict="timestamp,hub"
    ).execute()
    return len(records)

# ── Supabase Read ─────────────────────────────────────────────
def load_from_supabase(hubs, start, end) -> pd.DataFrame:
    result = (
        supabase.table("ercot_lmp")
        .select("timestamp, hub, lmp, delivery_date, delivery_hour")
        .in_("hub", hubs)
        .gte("delivery_date", start)
        .lte("delivery_date", end)
        .order("timestamp")
        .execute()
    )
    df = pd.DataFrame(result.data)
    if not df.empty:
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        df["lmp"] = pd.to_numeric(df["lmp"])
    return df

# ── Stats ─────────────────────────────────────────────────────
def repo_stats():
    try:
        r = supabase.table("ercot_lmp").select("id", count="exact").execute()
        total = r.count
        dates = supabase.table("ercot_lmp").select("delivery_date").order("delivery_date").execute()
        df = pd.DataFrame(dates.data)
        return total, df["delivery_date"].min() if not df.empty else "—", df["delivery_date"].max() if not df.empty else "—"
    except:
        return 0, "—", "—"

# ── UI ────────────────────────────────────────────────────────
st.set_page_config(page_title="ERCOT LMP Agent", page_icon="⚡", layout="wide")
st.title("⚡ ERCOT LMP Data Repository")
st.caption("Live Settlement Point Prices · Powered by Supabase")

total, min_d, max_d = repo_stats()
c1, c2, c3, c4 = st.columns(4)
c1.metric("📦 Total Records",  f"{total:,}")
c2.metric("📅 Earliest Date",  min_d)
c3.metric("📅 Latest Date",    max_d)
c4.metric("🏷️ Hubs Tracked",  len(HUBS))
st.divider()

with st.sidebar:
    st.header("📥 Fetch & Store")
    fetch_date = st.date_input("Delivery Date", value=datetime.today() - timedelta(days=1))
    fetch_hubs = st.multiselect("Hubs", HUBS, default=HUBS)
    if st.button("🔄 Fetch from ERCOT & Save", type="primary", use_container_width=True):
        with st.spinner("Fetching..."):
            try:
                df_new = fetch_ercot(str(fetch_date))
                n = save_to_supabase(df_new[df_new["hub"].isin(fetch_hubs)])
                st.success(f"✅ Saved {n} records")
                st.rerun()
            except Exception as e:
                st.error(f"Error: {e}")

    st.divider()
    st.header("📊 Query")
    q_hubs  = st.multiselect("Hubs", HUBS, default=["HB_NORTH", "HB_HOUSTON"])
    q_start = st.date_input("From", value=datetime.today() - timedelta(days=7))
    q_end   = st.date_input("To",   value=datetime.today() - timedelta(days=1))
    load_btn = st.button("📈 Load Chart", use_container_width=True)

if load_btn:
    with st.spinner("Loading from Supabase..."):
        df = load_from_supabase(q_hubs, str(q_start), str(q_end))
    if df.empty:
        st.warning("No data found. Fetch first.")
    else:
        st.session_state["df"] = df

if "df" in st.session_state:
    df = st.session_state["df"]
    tab1, tab2, tab3, tab4 = st.tabs(["📈 Time Series", "📊 Daily Avg", "🔥 Heatmap", "📋 Raw Data"])
    with tab1:
        st.plotly_chart(px.line(df, x="timestamp", y="lmp", color="hub",
            title="Real-Time LMP ($/MWh)", template="plotly_dark",
            labels={"lmp": "LMP ($/MWh)", "timestamp": ""}), use_container_width=True)
    with tab2:
        daily = df.groupby(["delivery_date", "hub"])["lmp"].mean().reset_index()
        st.plotly_chart(px.bar(daily, x="delivery_date", y="lmp", color="hub",
            barmode="group", title="Daily Average LMP", template="plotly_dark"), use_container_width=True)
    with tab3:
        pivot = df.pivot_table(index="hub", columns="delivery_date", values="lmp", aggfunc="mean")
        st.plotly_chart(px.imshow(pivot, color_continuous_scale="RdYlGn_r",
            title="LMP Heatmap ($/MWh)", template="plotly_dark"), use_container_width=True)
    with tab4:
        st.dataframe(df.sort_values("timestamp", ascending=False), use_container_width=True)
        st.download_button("⬇️ Download CSV", df.to_csv(index=False).encode(), "ercot_lmp.csv", "text/csv")
else:
    st.info("👈 Select hubs and date range, then click Load Chart")
