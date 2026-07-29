"""Streamlit serving layer for the Job Market AWS pipeline — reads the Gold marts from Athena.

Tầng 3 (serving): biến bảng Gold thành biểu đồ cho người không-kỹ-thuật đọc.

Data flow / luồng dữ liệu:
    S3 gold/*.parquet  ->  Glue Data Catalog (jobmarket_aws_gold)  ->  Athena SQL  ->  this app

Run:  streamlit run dashboard/app.py

READ-ONLY: every query is a SELECT, and `ctas_approach=False` keeps awswrangler from creating
temporary CTAS tables (its default WOULD write to S3 and the Catalog).
"""

import os

import awswrangler as wr
import boto3
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# ---------------------------------------------------------------------------------------------
# Config — read from the environment, never hardcoded.
#
# Vì sao? Cùng lý do Terraform dùng `variables.tf`: config tách khỏi code. Cùng một app trỏ được
# sang account/môi trường khác chỉ bằng biến môi trường, không phải sửa (và commit lại) code.
# Defaults chính là giá trị thật của stack hiện tại, lấy từ `terraform output`.
# ---------------------------------------------------------------------------------------------
AWS_REGION = os.getenv("AWS_REGION", "ap-southeast-1")
ATHENA_DATABASE = os.getenv("ATHENA_DATABASE", "jobmarket_aws_gold")
ATHENA_WORKGROUP = os.getenv("ATHENA_WORKGROUP", "jobmarket-aws")

# Note: no results-bucket setting here on purpose. The workgroup is created with
# `enforce_workgroup_configuration = true` (infra/athena.tf), so Athena applies its own result
# location. Hạ tầng đã ép cấu hình -> app không cần lặp lại.

CACHE_TTL_SECONDS = 900  # 15 min

# Palette: one accent for magnitude (bars), a contrasting cool hue for the remote signal.
ACCENT = "#B96A0E"
COOL = "#2F6F6A"

st.set_page_config(page_title="Tech Job Market — Gold marts", page_icon="📊", layout="wide")


# ---------------------------------------------------------------------------------------------
# Athena access
# ---------------------------------------------------------------------------------------------
@st.cache_resource
def _session() -> boto3.Session:
    """One boto3 session for the app (credentials come from `aws configure` / env / IAM role).

    cache_resource (not cache_data): a client is a live object, not a value to serialize.
    """
    return boto3.Session(region_name=AWS_REGION)


def _read_sql(sql: str) -> pd.DataFrame:
    """Run one SELECT against Athena and return a pandas DataFrame.

    ctas_approach=False is deliberate: awswrangler's default (True) wraps the query in a
    CREATE TABLE AS SELECT — it writes Parquet to S3 and registers a temp table in the Catalog.
    Mặc định của awswrangler sẽ GHI dữ liệu; ở đây ta chỉ được phép ĐỌC nên phải tắt.
    """
    return wr.athena.read_sql_query(
        sql,
        database=ATHENA_DATABASE,
        workgroup=ATHENA_WORKGROUP,
        ctas_approach=False,
        boto3_session=_session(),
    )


# Every loader is cached. Athena bills per BYTE SCANNED, so an uncached query would re-scan S3 on
# every widget click / rerender. Cache = tiền. TTL giữ số liệu tươi trong 15 phút.
@st.cache_data(ttl=CACHE_TTL_SECONDS, show_spinner="Querying Athena…")
def load_snapshots() -> list[str]:
    """Available snapshot_date partitions, newest first.

    Selecting only the partition column reads Catalog metadata — effectively 0 bytes scanned.
    """
    df = _read_sql(
        "SELECT DISTINCT snapshot_date FROM fact_job_posting ORDER BY snapshot_date DESC"
    )
    return [str(v) for v in df["snapshot_date"].tolist()]


@st.cache_data(ttl=CACHE_TTL_SECONDS, show_spinner="Querying Athena…")
def load_kpis(snapshot: str) -> pd.Series:
    """Headline numbers for one snapshot — postings, companies, sources, remote split.

    One query instead of four: each extra query would re-scan the same partition. Gộp 1 query để
    chỉ quét partition đúng 1 lần.
    """
    df = _read_sql(f"""
        SELECT
            count(*)                            AS postings,
            count(DISTINCT company)             AS companies,
            count(DISTINCT source)              AS sources,
            sum(if(is_remote, 1, 0))            AS remote_postings
        FROM fact_job_posting
        WHERE snapshot_date = '{snapshot}'
    """)
    return df.iloc[0]


@st.cache_data(ttl=CACHE_TTL_SECONDS, show_spinner="Querying Athena…")
def load_demand(snapshot: str) -> pd.DataFrame:
    """demand_by_role for one snapshot. Column is `job_count` (not posting_count)."""
    return _read_sql(f"""
        SELECT role, job_count
        FROM demand_by_role
        WHERE snapshot_date = '{snapshot}'
        ORDER BY job_count DESC
    """)


@st.cache_data(ttl=CACHE_TTL_SECONDS, show_spinner="Querying Athena…")
def load_top_companies(snapshot: str, limit: int = 15) -> pd.DataFrame:
    return _read_sql(f"""
        SELECT company, count(*) AS postings
        FROM fact_job_posting
        WHERE snapshot_date = '{snapshot}'
        GROUP BY company
        ORDER BY postings DESC
        LIMIT {limit}
    """)


@st.cache_data(ttl=CACHE_TTL_SECONDS, show_spinner="Querying Athena…")
def load_opportunity(snapshot: str) -> pd.DataFrame:
    """role_opportunity — the decision mart: demand rank + remote share + top hiring company."""
    return _read_sql(f"""
        SELECT demand_rank, role, job_count, remote_pct, top_company, top_company_count
        FROM role_opportunity
        WHERE snapshot_date = '{snapshot}'
        ORDER BY demand_rank
    """)


# ---------------------------------------------------------------------------------------------
# UI
# ---------------------------------------------------------------------------------------------
st.title("Tech Job Market — demand from the Gold marts")
st.caption(
    f"Live Athena queries · database `{ATHENA_DATABASE}` · workgroup `{ATHENA_WORKGROUP}` · "
    f"{AWS_REGION} — results cached {CACHE_TTL_SECONDS // 60} min to limit bytes scanned."
)

# --- Sidebar: snapshot filter ---
try:
    snapshots = load_snapshots()
except Exception as exc:  # credentials, missing table, torn-down infra...
    st.error(
        "Could not reach Athena. Check that (1) `aws configure` is set up, (2) the pipeline "
        f"infrastructure still exists, (3) the database/workgroup names are right.\n\n`{exc}`"
    )
    st.stop()

if not snapshots:
    st.warning("No snapshots found — has the pipeline run yet?")
    st.stop()

with st.sidebar:
    st.header("Filters")
    snapshot = st.selectbox("Snapshot date", snapshots, index=0)
    st.caption(f"{len(snapshots)} snapshot(s) available.\nEach run writes a new partition.")
    if st.button("Refresh data", help="Clear the cache and re-query Athena"):
        st.cache_data.clear()
        st.rerun()

kpis = load_kpis(snapshot)
postings = int(kpis["postings"])
remote_postings = int(kpis["remote_postings"] or 0)
onsite_postings = postings - remote_postings
remote_pct = (100.0 * remote_postings / postings) if postings else 0.0

# --- 1) KPI row ---
c1, c2, c3, c4 = st.columns(4)
c1.metric("Active postings", f"{postings:,}")
c2.metric("Hiring companies", f"{int(kpis['companies']):,}")
c3.metric("Source boards", f"{int(kpis['sources']):,}")
c4.metric("Remote", f"{remote_pct:.1f}%", f"{remote_postings:,} roles")

st.divider()

# --- 2) Top roles by demand ---
st.subheader("Demand by role")
demand = load_demand(snapshot)
if demand.empty:
    st.info("No role demand rows for this snapshot.")
else:
    fig = px.bar(
        demand.sort_values("job_count"),   # ascending -> largest bar on top in a horizontal chart
        x="job_count",
        y="role",
        orientation="h",
        text="job_count",
    )
    fig.update_traces(marker_color=ACCENT, textposition="outside", cliponaxis=False)
    fig.update_layout(
        height=420,
        margin=dict(l=0, r=30, t=10, b=0),
        xaxis_title="Distinct postings",
        yaxis_title=None,
        showlegend=False,
        plot_bgcolor="rgba(0,0,0,0)",
    )
    fig.update_xaxes(showgrid=True, gridcolor="rgba(128,128,128,.18)")
    st.plotly_chart(fig, width="stretch")

    with st.expander("Decision view — which role should I target? (`role_opportunity` mart)"):
        opp = load_opportunity(snapshot)
        st.dataframe(
            opp,
            width="stretch",
            hide_index=True,
            column_config={
                "demand_rank": st.column_config.NumberColumn("#", width="small"),
                "job_count": st.column_config.NumberColumn("Demand"),
                "remote_pct": st.column_config.NumberColumn("Remote %", format="%.1f%%"),
                "top_company": st.column_config.TextColumn("Top employer"),
                "top_company_count": st.column_config.NumberColumn("their postings"),
            },
        )
        st.caption(
            "This mart joins demand, remote share and the top hiring company per role — "
            "the table a candidate actually decides on."
        )

st.divider()

# --- 3) Company & remote insight ---
left, right = st.columns([3, 2])

with left:
    st.subheader("Top 15 hiring companies")
    companies = load_top_companies(snapshot)
    if companies.empty:
        st.info("No company rows for this snapshot.")
    else:
        fig_c = px.bar(
            companies.sort_values("postings"),
            x="postings",
            y="company",
            orientation="h",
            text="postings",
        )
        fig_c.update_traces(marker_color=ACCENT, textposition="outside", cliponaxis=False)
        fig_c.update_layout(
            height=520,
            margin=dict(l=0, r=30, t=10, b=0),
            xaxis_title="Postings",
            yaxis_title=None,
            showlegend=False,
            plot_bgcolor="rgba(0,0,0,0)",
        )
        fig_c.update_xaxes(showgrid=True, gridcolor="rgba(128,128,128,.18)")
        st.plotly_chart(fig_c, width="stretch")

with right:
    st.subheader("Remote vs on-site")
    fig_r = go.Figure(
        go.Pie(
            labels=["Remote", "On-site"],
            values=[remote_postings, onsite_postings],
            hole=0.58,
            marker=dict(colors=[COOL, ACCENT], line=dict(color="rgba(255,255,255,.85)", width=2)),
            textinfo="label+percent",
            sort=False,
        )
    )
    fig_r.update_layout(
        height=520,
        margin=dict(l=0, r=0, t=10, b=0),
        showlegend=False,
        annotations=[
            dict(text=f"<b>{remote_pct:.0f}%</b><br>remote", x=0.5, y=0.5,
                 font_size=18, showarrow=False)
        ],
    )
    st.plotly_chart(fig_r, width="stretch")

st.caption(
    f"Snapshot `{snapshot}` · every query filters on the `snapshot_date` partition "
    "(partition pruning) so Athena reads only that day's Parquet files."
)
