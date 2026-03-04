# app.py
import streamlit as st
import pandas as pd
import plotly.express as px
import zipfile
import io
from datetime import datetime, timedelta
from supabase import create_client

# ── Supabase ─────────────────────────────────────────────────
@st.cache_resource
def get_supabase():
    return create_client(st.secrets["SUPABASE_URL"], st.secrets["SUPABASE_KEY"])

supabase = get_supabase()

# ── Parse uploaded CSV/ZIP ────────────────────────────────────
def parse_upload(uploaded_file) -> pd.DataFrame:
    name = uploaded_file.name.lower()

    # Handle ZIP files
    if name.endswith(".zip"):
        z = zipfile.ZipFile(io.BytesIO(uploaded_file.read()))
        csv_name = [n for n in z.namelist() if n.endswith(".csv")][0]
        df = pd.read_csv(z.open(csv_name))
    else:
        df = pd.read_csv(uploaded_file)

    # Clean column names
    df.columns = [c.strip() for c in df.columns]

    # Show raw columns for debugging
    st.sidebar.caption(f"Columns: {df.columns.tolist()}")

    # Normalize column names
    col_map = {}
    for c in df.columns:
        cl = c.strip().lower().replace(" ", "").replace("_", "")
        if cl == "deliverydate":        col_map[c] = "delivery_date"
        elif cl == "hourending":        col_map[c] = "delivery_hour"
        elif cl in ["busname","hub","settlementpoint"]: col_map[c] = "hub"
        elif cl == "lmp":               col_map[c] = "lmp"
    df = df.rename(columns=col_map)

    # Build timestamp
    if "delivery_date" in df.columns and "delivery_hour" in df.columns:
        df["delivery_date"] = pd.to_datetime(df["delivery_date"]).dt.date.astype(str)
        df["delivery_hour"] = df["delivery_hour"].astype(str).str.replace(":00","").str.strip().str.zfill(2)
        df["timestamp"] = pd.to_datetime(df["delivery_date"] + " " + df["delivery_hour"] + ":00:00", errors="coerce")
    else:
        st.error("Could not find DeliveryDate or HourEnding columns.")
        return pd.DataFrame()

    df["lmp"]      = pd.to_numeric(df.get("lmp", 0), errors="coerce")
    df["interval"] = 1

    keep = ["timestamp", "hub", "lmp", "delivery_date", "delivery_hour", "interval"]
    keep = [c for c in keep if c in df.columns]
    return df[keep].dropna(subset=["timestamp", "lmp"])

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
        df["lmp"]       = pd.to_numeric(df["lmp"])
    return df

# ── Repo Stats ───────────────────────────────────────────────
def repo_stats():
    try:
        total = supabase.table("ercot_lmp").select("id", count="exact").execute().count
        dates = pd.DataFrame(supabase.table("ercot_lmp").select("delivery_date").execute().data)
        min_d = dates["delivery_date"].min() if not dates.empty else "—"
        max_d = dates["delivery_date"].max() if not dates.empty else "—"
        return total, min_d, max_d
    except:
        return 0, "—", "—"

# ════════════════════════════════════════════════════════════
# UI
# ════════════════════════════════════════════════════════════
st.set_page_config(page_title="ERCOT LMP Agent", page_icon="⚡", layout="wide")
st.title("⚡ ERCOT LMP Data Repository")
st.caption("DAM Hourly LMPs · Powered by Supabase")

total, min_d, max_d = repo_stats()
c1, c2, c3 = st.columns(3)
c1.metric("📦 Total Records", f"{total:,}")
c2.metric("📅 Earliest Date", min_d)
c3.metric("📅 Latest Date",   max_d)
st.divider()

# ── Sidebar ───────────────────────────────────────────────────
with st.sidebar:
    st.header("📥 Upload & Store")
    st.info(
        "**How to get data:**\n\n"
        "1. Go to ercot.com → Data Access Portal\n"
        "2. Search **DAM Hourly LMPs**\n"
        "3. Click **Download** on any date\n"
        "4. Upload the ZIP or CSV file below"
    )

    uploaded = st.file_uploader(
        "Upload ERCOT CSV or ZIP",
        type=["csv", "zip"],
        accept_multiple_files=True
    )

    if uploaded and st.button("💾 Save to Supabase", type="primary", use_container_width=True):
        total_saved = 0
        for f in uploaded:
            with st.spinner(f"Processing {f.name}..."):
                try:
                    df_parsed = parse_upload(f)
                    n = save_to_supabase(df_parsed)
                    total_saved += n
                    st.success(f"✅ {f.name} → {n} records saved")
                except Exception as e:
                    st.error(f"❌ {f.name}: {e}")
        if total_saved > 0:
            st.rerun()

    st.divider()
    st.header("📊 Query Repository")
    q_start = st.date_input("From", value=datetime.today() - timedelta(days=7))
    q_end   = st.date_input("To",   value=datetime.today() - timedelta(days=1))

    try:
        hubs_in_db = sorted(set(
            r["hub"] for r in supabase.table("ercot_lmp").select("hub").execute().data
        ))
    except:
        hubs_in_db = []

    q_hubs   = st.multiselect("Filter by Bus", hubs_in_db, default=hubs_in_db[:5] if hubs_in_db else [])
    load_btn = st.button("📈 Load Chart", use_container_width=True)

# ── Charts ────────────────────────────────────────────────────
if load_btn:
    with st.spinner("Loading from Supabase..."):
        df = load_from_supabase(q_hubs, str(q_start), str(q_end))
    if df.empty:
        st.warning("No data found. Upload files first.")
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
            barmode="group", title="Daily Average LMP", template="plotly_dark"), use_container_width=True)
    with tab3:
        pivot = df.pivot_table(index="hub", columns="delivery_date", values="lmp", aggfunc="mean")
        st.plotly_chart(px.imshow(pivot, color_continuous_scale="RdYlGn_r",
            title="LMP Heatmap ($/MWh)", template="plotly_dark"), use_container_width=True)
    with tab4:
        st.dataframe(df.sort_values("timestamp", ascending=False), use_container_width=True)
        st.download_button("⬇️ Download CSV",
            df.to_csv(index=False).encode(), "ercot_lmp.csv", "text/csv")
else:
    st.info("👈 Upload ERCOT files in the sidebar to get started")
