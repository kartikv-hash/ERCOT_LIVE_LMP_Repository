# app.py
import streamlit as st
import pandas as pd
import plotly.express as px
import requests
import zipfile
import io
from datetime import datetime, timedelta
from supabase import create_client

# ── Supabase ─────────────────────────────────────────────────
@st.cache_resource
def get_supabase():
    return create_client(st.secrets["SUPABASE_URL"], st.secrets["SUPABASE_KEY"])

supabase = get_supabase()

# ── ERCOT Data Access Portal ─────────────────────────────────
# No API key needed — public CSV downloads
ERCOT_DAP = "https://www.ercot.com/misapp/GetReports.do"

def get_file_list() -> pd.DataFrame:
    r = requests.get(
        ERCOT_DAP,
        params={"reportTypeId": "12331", "documentType": "csv"},
        headers=HEADERS,
        timeout=30,
    )
    r.raise_for_status()
    rows = []
    import re
    # Parse doc IDs and dates from HTML response
    matches = re.findall(r'docId=(\d+).*?(\d{4}-\d{2}-\d{2})', r.text, re.DOTALL)
    for doc_id, date in matches:
        rows.append({"doc_id": doc_id, "posted": date})
    return pd.DataFrame(rows).drop_duplicates("posted").head(30)

# ── Download and parse one CSV file ─────────────────────────
def download_csv(doc_id: str) -> pd.DataFrame:
    r = requests.get(
        "https://www.ercot.com/misapp/servlets/IceDocFetch.exe",
        params={"docId": doc_id},
        headers=HEADERS,
        timeout=60,
    )
    r.raise_for_status()
    # Files are zipped
    z = zipfile.ZipFile(io.BytesIO(r.content))
    csv_name = [n for n in z.namelist() if n.endswith(".csv")][0]
    df = pd.read_csv(z.open(csv_name))
    df.columns = [c.strip() for c in df.columns]
    return df

# ── Parse ERCOT DAM LMP CSV ───────────────────────────────────
def parse_lmp(df_raw: pd.DataFrame, delivery_date: str) -> pd.DataFrame:
    # Typical columns: DeliveryDate, HourEnding, BusName, LMP, ...
    df = df_raw.copy()
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

    # Normalize column names
    col_map = {
        "deliverydate": "delivery_date",
        "hourending":   "delivery_hour",
        "busname":      "hub",
        "lmp":          "lmp",
    }
    df = df.rename(columns={k: v for k, v in col_map.items() if k in df.columns})

    if "delivery_date" not in df.columns:
        df["delivery_date"] = delivery_date
    if "delivery_hour" in df.columns:
        df["delivery_hour"] = df["delivery_hour"].astype(str).str.replace(":00", "").str.strip()
        df["timestamp"] = pd.to_datetime(
            df["delivery_date"].astype(str) + " " + df["delivery_hour"].astype(str).str.zfill(2) + ":00:00",
            errors="coerce"
        )
    df["lmp"] = pd.to_numeric(df.get("lmp", 0), errors="coerce")
    df["interval"] = 1
    return df[["timestamp", "hub", "lmp", "delivery_date", "delivery_hour", "interval"]].dropna()

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

# ════════════════════════════════════════════════════════════
# UI
# ════════════════════════════════════════════════════════════
st.set_page_config(page_title="ERCOT LMP Agent", page_icon="⚡", layout="wide")
st.title("⚡ ERCOT LMP Data Repository")
st.caption("DAM Hourly LMPs · ERCOT Data Access Portal · Powered by Supabase")

total, min_d, max_d = repo_stats()
c1, c2, c3 = st.columns(3)
c1.metric("📦 Total Records", f"{total:,}")
c2.metric("📅 Earliest Date", min_d)
c3.metric("📅 Latest Date",   max_d)
st.divider()

# ── Sidebar ───────────────────────────────────────────────────
with st.sidebar:
    st.header("📥 Fetch & Store")

    # Load available files from ERCOT
    if st.button("🔍 Load Available Files", use_container_width=True):
        with st.spinner("Fetching file list from ERCOT..."):
            try:
                files = get_file_list()
                st.session_state["files"] = files
            except Exception as e:
                st.error(f"Error: {e}")

    if "files" in st.session_state:
        files = st.session_state["files"]
        if files.empty:
            st.warning("No files found. ERCOT portal may be unavailable.")
        else:
            files["label"] = files["posted"].str[:10]
            selected = st.selectbox("Select Date to Fetch", files["label"].tolist())
            doc_id = files[files["label"] == selected]["doc_id"].values[0]

        if st.button("⬇️ Fetch & Save to Supabase", type="primary", use_container_width=True):
            with st.spinner(f"Downloading {selected}..."):
                try:
                    df_raw  = download_csv(doc_id)
                    df_parsed = parse_lmp(df_raw, selected)
                    n = save_to_supabase(df_parsed)
                    st.success(f"✅ Saved {n} records for {selected}")
                    st.rerun()
                except Exception as e:
                    st.error(f"Error: {e}")

    st.divider()
    st.header("📊 Query Repository")
    q_start  = st.date_input("From", value=datetime.today() - timedelta(days=7))
    q_end    = st.date_input("To",   value=datetime.today() - timedelta(days=1))

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
        st.warning("No data found. Fetch data first using the sidebar.")
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
            barmode="group", title="Daily Average LMP",
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
    st.info("👈 Click 'Load Available Files' in the sidebar to get started")
