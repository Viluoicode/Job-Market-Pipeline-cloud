"""One-page workshop dashboard. Run: streamlit run dashboard/app.py."""
import logging
import os

import pandas as pd
import plotly.express as px
import streamlit as st
from data_access import PublicationUnavailable, create_reader_session, query_fact, read_publication
from model import REMOTE_LABELS, ROLE_FAMILIES, classify, remote_label, safe_link

st.set_page_config(page_title="Job Market | AWS Workshop", page_icon="📊", layout="wide")
# Static presentation styles only; source text never enters HTML.
st.markdown("""
<style>
    [data-testid="stMetric"] {
        background: #FFFFFF;
        border: 1px solid #C7DCCE;
        border-top: 4px solid #15803D;
        border-radius: 12px;
        padding: 16px 20px;
    }
    [data-testid="stMetricLabel"] { color: #375B45; }
    [data-testid="stMetricValue"] { color: #14532D; }
</style>
""", unsafe_allow_html=True)


def style_chart(figure):
    """Keep the charts legible against the workshop's light green theme."""
    figure.update_layout(
        paper_bgcolor="#FFFFFF", plot_bgcolor="#FFFFFF",
        font={"color": "#163326", "size": 13},
        title_font={"size": 18, "color": "#14532D"},
        margin={"l": 16, "r": 24, "t": 64, "b": 32}, height=460,
        hoverlabel={"bgcolor": "#FFFFFF", "font_color": "#163326"},
    )
    figure.update_xaxes(gridcolor="#E2EDE5", zeroline=False, title_text="Postings")
    figure.update_yaxes(showgrid=False, automargin=True)
    figure.update_traces(marker_line_width=0, hovertemplate="%{y}<br>Postings: %{x:,}<extra></extra>")
    # Keep a single remaining category from becoming a full-height rectangle.
    category_count = len(figure.data[0].y) if figure.data else 0
    if 0 < category_count < 4:
        figure.update_yaxes(range=[-0.5, 3.5])
    return figure


st.title("Job Market Overview")
st.caption("AWS data engineering workshop · A configured sample of public job boards, not the whole job market.")

bucket = os.getenv("LAKE_BUCKET")
lock_table = os.getenv("PIPELINE_LOCK_TABLE")
reader_role_arn = os.getenv("AWS_DASHBOARD_ROLE_ARN")
if not bucket or not lock_table or not reader_role_arn:
    st.info("Set LAKE_BUCKET, PIPELINE_LOCK_TABLE and AWS_DASHBOARD_ROLE_ARN before starting. See docs/workshop.md.")
    st.stop()
region = os.getenv("AWS_REGION", "ap-southeast-1")
database = os.getenv("ATHENA_DATABASE", "jobmarket_aws_gold")
workgroup = os.getenv("ATHENA_WORKGROUP", "jobmarket-aws")
@st.cache_resource(ttl=2700, show_spinner=False)
def reader_session(region, role_arn):
    # Refresh before the one-hour STS credentials expire.
    return create_reader_session(region, role_arn)


@st.cache_data(ttl=900, show_spinner=False)
def load_fact(region, database, workgroup, bucket, reader_role_arn, fingerprint, snapshot):
    # Fingerprint is part of the cache key: a same-day republish must invalidate the cache.
    return query_fact(reader_session(region, reader_role_arn), database, workgroup, snapshot)

if st.button("Refresh data", type="primary"):
    load_fact.clear()
try:
    with st.spinner("Checking publication and reading the current snapshot…"):
        session = reader_session(region, reader_role_arn)
        metadata, before = read_publication(session, bucket, lock_table)
        rows, query = load_fact(region, database, workgroup, bucket, reader_role_arn, before, metadata["snapshot_date"])
        _, after = read_publication(session, bucket, lock_table)
        if before != after:
            raise PublicationUnavailable("Publication changed while loading. Refresh to try again.")
except PublicationUnavailable as exc:
    st.warning(str(exc))
    st.stop()
except Exception:
    logging.exception("Dashboard AWS read failed")
    st.error("Could not verify or read the published data. Check AWS access and the operations runbook.")
    st.stop()

st.success(f"Published snapshot (UTC): {metadata['snapshot_date']} · "
           f"Ingestion age: {metadata['ingestion_age_hours']:.1f} hours · "
           f"Boards succeeded: {metadata['successful_boards']}")
st.caption(f"Observed at {metadata['ingested_at']} · Published at {metadata['published_at']} · "
           "Daily schedule: 01:00 UTC+7 (previous calendar date in UTC).")
if metadata["incomplete_boards"] or metadata["failed_boards"]:
    st.info(f"Coverage: {metadata['incomplete_boards']} incomplete board(s), "
            f"{metadata['failed_boards']} failed board(s). "
            "Incomplete sources are not evidence that unseen jobs have closed.")

frame = pd.DataFrame(rows)
for column in ("title", "company", "location", "source"):
    frame[column] = frame[column].fillna("Unknown")
FAMILY_COUNT = len(ROLE_FAMILIES)
frame["roles"] = frame["title"].map(classify)
frame["work_mode"] = frame["is_remote"].map(remote_label)
frame["apply_url"] = frame["apply_url"].map(safe_link)
left, middle, right = st.columns(3)
sources = left.multiselect("Source platform", sorted(frame.source.unique()))
roles = middle.multiselect(
    "Role family", sorted({role for values in frame.roles for role in values}),
    format_func=lambda role: f"Not matched to the {FAMILY_COUNT} role families" if role == "Unclassified" else role,
)
locations = right.multiselect("Location (as reported)", sorted(frame.location.unique()))
search = st.text_input("Search title or company", placeholder="For example: data engineer")
work_modes = st.multiselect("Work mode", REMOTE_LABELS,
                            help="Unknown = the source gave no remote/onsite signal; it is not counted as remote.")
filtered = frame
if sources:
    filtered = filtered[filtered.source.isin(sources)]
if roles:
    filtered = filtered[filtered.roles.map(lambda values: bool(set(values) & set(roles)))]
if locations:
    filtered = filtered[filtered.location.isin(locations)]
if search.strip():
    needle = search.strip()
    filtered = filtered[filtered.title.str.contains(needle, case=False, regex=False) |
                        filtered.company.str.contains(needle, case=False, regex=False)]
if work_modes:
    filtered = filtered[filtered.work_mode.isin(work_modes)]

cards = st.columns(4)
cards[0].metric("Unique postings", f"{len(filtered):,}")
cards[1].metric("Companies (reported)", f"{filtered.company.nunique():,}")
cards[2].metric("Locations (reported)", f"{filtered.location.nunique():,}")
mode_share = filtered.work_mode.value_counts(normalize=True).mul(100) if len(filtered) else {}
cards[3].metric("Remote (of all postings)", f"{mode_share.get('Remote', 0):.1f}%",
                help=f"Onsite / hybrid {mode_share.get('Onsite / hybrid', 0):.1f}% · "
                     f"Unknown {mode_share.get('Unknown', 0):.1f}%. Unknown is never counted as remote.")
unmatched_count = int(filtered["roles"].map(lambda values: "Unclassified" in values).sum())
matched_count = len(filtered) - unmatched_count
unmatched_share = 100 * unmatched_count / len(filtered) if len(filtered) else 0
st.markdown(
    f"**Role coverage in this view:** {matched_count:,} postings match one of the {FAMILY_COUNT} role families · "
    f"**{unmatched_count:,} not matched ({unmatched_share:.1f}%)**."
)
st.caption(
    "Not matched means the title does not match the configured rules: it may be outside the "
    f"{FAMILY_COUNT} families (sales, marketing, hardware…) or missed by the rules. These postings remain "
    "in the totals and table unless filtered out."
)
if filtered.empty:
    st.info("No postings match these filters. Clear a filter to broaden the view.")
else:
    col1, col2 = st.columns(2)
    role_counts = filtered.explode("roles").groupby("roles").size().sort_values().rename("Postings").reset_index()
    companies = filtered.groupby("company").size().nlargest(10).sort_values().rename("Postings").reset_index()
    role_counts = role_counts[role_counts["roles"] != "Unclassified"]
    if len(roles) == 1:
        source_counts = filtered.groupby("source").size().sort_values().rename("Postings").reset_index()
        col1.plotly_chart(style_chart(px.bar(
            source_counts, x="Postings", y="source", orientation="h", title="Postings by source platform",
            labels={"source": ""}, color_discrete_sequence=["#15803D"])), width="stretch", theme=None)
    elif role_counts.empty:
        col1.info(f"No postings in this view match the {FAMILY_COUNT} role families. "
                  "They are still shown in the table.")
    else:
        col1.plotly_chart(style_chart(px.bar(
            role_counts, x="Postings", y="roles", orientation="h",
            title=f"Postings by role family ({FAMILY_COUNT} families)",
            labels={"roles": ""}, color_discrete_sequence=["#15803D"])), width="stretch", theme=None)
    col2.plotly_chart(style_chart(px.bar(
        companies, x="Postings", y="company", orientation="h", title="Top 10 companies in this view",
        labels={"company": ""}, color_discrete_sequence=["#3D8060"])), width="stretch", theme=None)
st.caption("The role chart includes matched families only; each posting belongs to exactly one family. "
           "Work mode Unknown means the source gave no remote signal; it is not counted as remote or onsite. "
           "All metrics follow the filters above.")
st.subheader("Explore the actual postings")
st.dataframe(filtered[["title", "company", "location", "source", "work_mode", "last_seen_at", "apply_url"]],
    hide_index=True, width="stretch",
    column_config={"apply_url": st.column_config.LinkColumn("Original posting", display_text="Open posting"),
                   "work_mode": "Work mode", "last_seen_at": "Last observed (UTC)"})
with st.expander("Data scope and verification"):
    st.write("These records were active and fresh when Gold was built. Availability can change afterwards; "
             "verify on the original employer page. This workshop does not rank personal suitability "
             "or cover Vietnam specifically.")
    st.write(f"Run: {metadata['run_id']}")
    st.write(f"Raw observations: {metadata['rows_ingested']:,}. "
             "Gold counts can differ because of deduplication and lifecycle retention.")
    st.caption(f"Athena query: {query['query_id']} · Scanned: {query['bytes_scanned']:,} bytes. "
               "Results are cached for up to 15 minutes; publication health is checked on each page interaction. "
               "An idle page does not refresh automatically.")
