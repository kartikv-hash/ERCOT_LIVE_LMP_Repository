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

# ── ERCOT Config ─────────────────────────────────────────────
# Endpoint: DAM Hourly LMPs (NP4-183-CD) — confirmed in your subscription
ERCOT_URL = "https://api.ercot.com/api/public-reports/np4-183-cd/dam_hourly_lmp"

# ── Fetch from ERCOT ─────────────────────────────────────────
def get_ercot_token() -> str:
    """Get Bearer token from apiexplorer.ercot.com portal."""
    r = requests.post(
        "https://apiexplorer.ercot.com/api/oauth2/v2.0/token",
        data={
            "grant_type": "password",
            "username":   st.secrets["ERCOT_USERNAME"],
            "password":   st.secrets["ERCOT_PASSWORD"],
            "scope":      "openid",
        },
        timeout=30,
    )
    st.sidebar.code(f"Token status: {r.status_code}\n{r.text[:300]}")
    if r.status_code != 200:
        # Try without token — subscription key only
        return None
    data = r.json()
    return data.get("id_token") or data.get("access_token")

def fetch_ercot(delivery_date: str) -> pd.DataFrame:
    token = get_ercot_token()
    headers = {"Ocp-Apim-Subscription-Key": st.secrets["ERCOT_PRIMARY_KEY"]}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    params  = {
        "deliveryDateFrom": delivery_date,
        "deliveryDateTo":   delivery_date,
        "size":             10000,
    }
    r = requests.get(ERCOT_URL, headers=headers, params=params, timeout=30)
    st.sidebar.code(f"Status: {r.status_code}\n{r.text[:300]}", language="json")
    r.raise_for_status()

    rows = []
    for rec in r.json().get("data", []):
        rows.append({
            "timestamp":     delivery_date + "T" + f"{int(rec.get('hourEnding', 1)):02d}:00:00",
            "hub":           rec.get("busName", "UNKNOWN"),
            "lmp":           float(rec.get("LMP", 0)),
            "delivery_date": delivery_date,
            "delivery_hour": int(rec.get("hourEnding", 1)),
            "interval":      1,
        })
    df = pd.DataFrame(rows)
    if not df.empty:
        df["timestamp"] = pd.to_datetime(df["timestamp"])
    return df

# ── Save to Supabase ─────────────────────────────────────────
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

# ── Load from Supabase ───────────────────────────────────────
def load_from_supabase(hubs, start, end) -> pd.DataFrame:
    q = (
        supabase.table("ercot_lmp")
        .select("timestamp, hub, lmp, delivery_date, delivery_hour")
        .gte("delivery_date", start)
        .lte("delivery_date", end)
        .order("timestamp")
    )
    if hubs:
        q = q.in_("hub", hubs)
    df = pd.DataFrame(q.execute().data)
    if not df.empty:
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        df["lmp"] = pd.to_numeric(df["lmp"])
    return df

# ── Repo Stats ───────────────────────────────────────────────
def repo_stats():
    try:
        r = supabase.table("ercot_lmp").select("id", count="exact").execute()
        total = r.count
        dates = pd.DataFrame(supabase.table("ercot_lmp").select("delivery_date").execute().data)
        min_d = dates["delivery_date"].min() if not dates.empty else "—"
        max_d = dates["delivery_date"].max() if not dates.empty else "—"
        return total, min_d, max_d
    except:
        return 0, "—", "—"

# ── UI ────────────────────────────────────────────────────────
st.set_page_config(page_title="ERCOT LMP Agent", page_icon="⚡", layout="wide")
st.title("⚡ ERCOT LMP Data Repository")
st.caption("DAM Hourly LMPs (NP4-183-CD) · Powered by Supabase")

total, min_d, max_d = repo_stats()
c1, c2, c3 = st.columns(3)
c1.metric("📦 Total Records", f"{total:,}")
c2.metric("📅 Earliest Date", min_d)
c3.metric("📅 Latest Date",   max_d)
st.divider()

# ── Sidebar ───────────────────────────────────────────────────
with st.sidebar:
    st.header("📥 Fetch & Store")
    fetch_date = st.date_input("Delivery Date", value=datetime.today() - timedelta(days=1))

    if st.button("🔄 Fetch from ERCOT & Save", type="primary", use_container_width=True):
        with st.spinner("Fetching from ERCOT..."):
            try:
                df_new = fetch_ercot(str(fetch_date))
                n = save_to_supabase(df_new)
                st.success(f"✅ Saved {n} records")
                st.rerun()
            except Exception as e:
                st.error(f"Error: {e}")

    st.divider()
    st.header("📊 Query")
    q_start  = st.date_input("From", value=datetime.today() - timedelta(days=7))
    q_end    = st.date_input("To",   value=datetime.today() - timedelta(days=1))

    # Hub filter — populated from what's actually in the DB
    try:
        hubs_in_db = sorted(set(
            r["hub"] for r in supabase.table("ercot_lmp").select("hub").execute().data
        ))
    except:
        hubs_in_db = []

    q_hubs = st.multiselect("Filter by Bus/Hub", hubs_in_db, default=hubs_in_db[:5] if hubs_in_db else [])
    load_btn = st.button("📈 Load Chart", use_container_width=True)

# ── Charts ────────────────────────────────────────────────────
if load_btn:
    with st.spinner("Loading from Supabase..."):
        df = load_from_supabase(q_hubs, str(q_start), str(q_end))
    if df.empty:
        st.warning("No data found. Fetch data first.")
    else:
        st.session_state["df"] = df

if "df" in st.session_state:
    df = st.session_state["df"]
    tab1, tab2, tab3, tab4 = st.tabs(["📈 Time Series", "📊 Daily Avg", "🔥 Heatmap", "📋 Raw Data"])

    with tab1:
        st.plotly_chart(px.line(df, x="timestamp", y="lmp", color="hub",
            title="DAM Hourly LMP ($/MWh)", template="plotly_dark",
            labels={"lmp": "LMP ($/MWh)", "timestamp": ""}), use_container_width=True)

    with tab2:
        daily = df.groupby(["delivery_date", "hub"])["lmp"].mean().reset_index()
        st.plotly_chart(px.bar(daily, x="delivery_date", y="lmp", color="hub",
            barmode="group", title="Daily Average LMP by Bus",
            template="plotly_dark"), use_container_width=True)

    with tab3:
        pivot = df.pivot_table(index="hub", columns="delivery_date", values="lmp", aggfunc="mean")
        st.plotly_chart(px.imshow(pivot, color_continuous_scale="RdYlGn_r",
            title="LMP Heatmap ($/MWh)", template="plotly_dark"), use_container_width=True)

    with tab4:
        st.dataframe(df.sort_values("timestamp", ascending=False), use_container_width=True)
        st.download_button("⬇️ Download CSV",
            df.to_csv(index=False).encode(), "ercot_lmp.csv", "text/csv")
else:
    st.info("👈 Set date range in the sidebar, then click Load Chart")
