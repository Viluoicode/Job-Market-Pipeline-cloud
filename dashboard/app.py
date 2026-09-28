"""One-page workshop dashboard. Run: streamlit run dashboard/app.py."""
import logging
import os

import boto3
import pandas as pd
import plotly.express as px
import streamlit as st

from data_access import PublicationUnavailable, read_publication, query_fact
from model import classify, safe_link

st.set_page_config(page_title="Job Market | AWS Workshop", page_icon="📊", layout="wide")
st.title("Job Market Overview")
st.caption("AWS data engineering workshop · A configured sample of public job boards, not the whole job market.")

bucket = os.getenv("LAKE_BUCKET")
lock_table = os.getenv("PIPELINE_LOCK_TABLE")
if not bucket or not lock_table:
    st.info("Set LAKE_BUCKET and PIPELINE_LOCK_TABLE before starting. See docs/workshop.md.")
    st.stop()
region = os.getenv("AWS_REGION", "ap-southeast-1")
database = os.getenv("ATHENA_DATABASE", "jobmarket_aws_gold")
workgroup = os.getenv("ATHENA_WORKGROUP", "jobmarket-aws")
session = boto3.Session(region_name=region)

@st.cache_data(ttl=900, show_spinner=False)
def load_fact(region, database, workgroup, bucket, fingerprint, snapshot):
    # Fingerprint is part of the cache key: a same-day republish must invalidate the cache.
    return query_fact(boto3.Session(region_name=region), database, workgroup, snapshot)

if st.button("Refresh data"):
    load_fact.clear()
try:
    with st.spinner("Checking publication and reading the current snapshot…"):
        metadata, before = read_publication(session, bucket, lock_table)
        rows, query = load_fact(region, database, workgroup, bucket, before, metadata["snapshot_date"])
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
    st.info(f"Coverage: {metadata['incomplete_boards']} incomplete board(s), {metadata['failed_boards']} failed board(s). "
            "Incomplete sources are not evidence that unseen jobs have closed.")

frame = pd.DataFrame(rows)
for column in ("title", "company", "location", "source"):
    frame[column] = frame[column].fillna("Unknown")
frame["roles"] = frame["title"].map(classify)
frame["apply_url"] = frame["apply_url"].map(safe_link)
left, middle, right = st.columns(3)
sources = left.multiselect("Source platform", sorted(frame.source.unique()))
roles = middle.multiselect("Role family", sorted({role for values in frame.roles for role in values}))
locations = right.multiselect("Location (as reported)", sorted(frame.location.unique()))
search = st.text_input("Search title or company", placeholder="For example: data engineer")
remote_only = st.checkbox("Only postings marked remote")
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
if remote_only:
    filtered = filtered[filtered.is_remote]

cards = st.columns(4)
cards[0].metric("Unique postings", f"{len(filtered):,}")
cards[1].metric("Companies (reported)", f"{filtered.company.nunique():,}")
cards[2].metric("Locations (reported)", f"{filtered.location.nunique():,}")
remote_share = 100 * filtered.is_remote.mean() if len(filtered) else 0
cards[3].metric("Marked remote", f"{remote_share:.1f}%")
if filtered.empty:
    st.info("No postings match these filters. Clear a filter to broaden the view.")
else:
    col1, col2 = st.columns(2)
    role_counts = filtered.explode("roles").groupby("roles").size().sort_values().rename("Postings").reset_index()
    companies = filtered.groupby("company").size().nlargest(10).sort_values().rename("Postings").reset_index()
    col1.plotly_chart(px.bar(role_counts, x="Postings", y="roles", orientation="h", title="Postings by role family",
                            labels={"roles": ""}, color_discrete_sequence=["#287D8E"]), width="stretch")
    col2.plotly_chart(px.bar(companies, x="Postings", y="company", orientation="h", title="Top 10 companies in this view",
                            labels={"company": ""}, color_discrete_sequence=["#B96A0E"]), width="stretch")
st.caption("Role families use title keywords and may overlap; Unclassified is retained. "
           "Not marked remote does not mean confirmed onsite. All metrics follow the filters above.")
st.subheader("Explore the actual postings")
st.dataframe(filtered[["title", "company", "location", "source", "is_remote", "last_seen_at", "apply_url"]],
    hide_index=True, width="stretch",
    column_config={"apply_url": st.column_config.LinkColumn("Original posting", display_text="Open posting"),
                   "is_remote": "Marked remote", "last_seen_at": "Last observed (UTC)"})
with st.expander("Data scope and verification"):
    st.write("These records were active and fresh when Gold was built. Availability can change afterwards; "
             "verify on the original employer page. This workshop does not rank personal suitability or cover Vietnam specifically.")
    st.write(f"Run: {metadata['run_id']}")
    st.write(f"Raw observations: {metadata['rows_ingested']:,}. Gold counts can differ because of deduplication and lifecycle retention.")
    st.caption(f"Athena query: {query['query_id']} · Scanned: {query['bytes_scanned']:,} bytes. "
               "Results are cached for up to 15 minutes; publication health is checked on each page interaction. "
               "An idle page does not refresh automatically.")
