"""One-page workshop dashboard. Run: streamlit run dashboard/app.py."""
import html
import logging
import os

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from data_access import PublicationUnavailable, create_reader_session, query_fact, read_publication
from model import (
    REMOTE_LABELS,
    ROLE_FAMILIES,
    classify,
    format_utc,
    platform_label,
    remote_label,
    safe_link,
    snapshot_stamp,
)

st.set_page_config(page_title="Job postings | AWS workshop", page_icon=":material/work:", layout="wide")

# Graphite neutrals; ACCENT marks one thing per view (remote postings, the primary action).
ACCENT, NEUTRAL, QUIET = "#1E6B43", "#A3A3A3", "#D4D4D4"
TEXT, SECONDARY, MUTED = "#171717", "#525252", "#737373"
MODE_COLORS = dict(zip(REMOTE_LABELS, [ACCENT, NEUTRAL, QUIET]))
SANS, MONO = "IBM Plex Sans, sans-serif", "IBM Plex Mono, monospace"

# Static presentation styles only; source text never enters HTML.
st.markdown(f"""
<style>
    .block-container {{ max-width: 1440px; padding-top: 2.5rem; padding-bottom: 4rem; }}
    h1 {{ letter-spacing: -0.01em; }}
    [data-testid="stMetric"] {{
        background: #FFFFFF; border: 1px solid rgba(23, 23, 23, 0.08); border-radius: 12px; padding: 16px 20px;
    }}
    [data-testid="stMetricLabel"] p {{ color: {SECONDARY}; font-size: 0.875rem; }}
    [data-testid="stMetricValue"] {{ font-family: {MONO}; font-variant-numeric: tabular-nums; color: {TEXT}; }}
    .jm-stamp {{
        display: inline-flex; align-items: center; gap: 8px; padding: 4px 12px; margin: 4px 0 8px;
        font-family: {MONO}; font-size: 13px; color: {SECONDARY};
        background: #FFFFFF; border: 1px solid rgba(23, 23, 23, 0.10); border-radius: 8px;
    }}
    .jm-stamp i {{ width: 8px; height: 8px; border-radius: 50%; background: {ACCENT}; display: inline-block; }}
    .jm-modes {{ display: flex; flex-direction: column; gap: 8px; margin-top: 4px; }}
    .jm-mode {{ display: grid; grid-template-columns: 12px 1fr auto auto; gap: 8px; align-items: center;
                font-size: 14px; color: {SECONDARY}; }}
    .jm-mode i {{ width: 10px; height: 10px; border-radius: 2px; display: inline-block; }}
    .jm-mode b {{ font-family: {MONO}; font-weight: 500; color: {TEXT}; font-variant-numeric: tabular-nums; }}
    .jm-mode span:last-child {{ font-family: {MONO}; color: {MUTED}; min-width: 64px; text-align: right; }}
</style>
""", unsafe_allow_html=True)


def chart_layout(figure, height):
    """Shared chart look: transparent on its card, muted grid and mono figures."""
    figure.update_layout(
        height=height, margin={"l": 8, "r": 56, "t": 8, "b": 8}, bargap=0.35,
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        font={"family": SANS, "color": SECONDARY, "size": 13},
        hoverlabel={"bgcolor": "#FFFFFF", "bordercolor": "#E5E5E5", "font": {"family": SANS, "color": TEXT}},
    )
    figure.update_xaxes(showgrid=True, gridcolor="#EFEFEF", zeroline=False, tickformat=",",
                        tickfont={"family": MONO, "color": MUTED, "size": 12}, showline=False)
    figure.update_yaxes(showgrid=False, automargin=True, tickfont={"color": TEXT, "size": 13}, ticksuffix="  ")
    return figure


def stacked_by_mode(frame, category):
    """Horizontal bars per category, each split into remote / onsite or hybrid / not stated."""
    counts = frame.groupby([category, "work_mode"]).size().unstack(fill_value=0)
    counts = counts.reindex(columns=REMOTE_LABELS, fill_value=0)
    counts = counts.loc[counts.sum(axis=1).sort_values().index]
    totals = counts.sum(axis=1)
    figure = go.Figure()
    for mode in REMOTE_LABELS:
        share = (100 * counts[mode] / totals).round(1)
        figure.add_bar(
            y=counts.index, x=counts[mode], orientation="h", name="Not stated" if mode == "Unknown" else mode,
            marker={"color": MODE_COLORS[mode], "line": {"width": 0}}, customdata=share,
            hovertemplate="%{y}<br>" + mode + ": %{x:,} (%{customdata}%)<extra></extra>")
    figure.add_scatter(y=counts.index, x=totals, mode="text", text=[f"  {v:,}" for v in totals],
                       textposition="middle right", textfont={"family": MONO, "color": SECONDARY, "size": 12},
                       hoverinfo="skip", showlegend=False)
    figure.update_layout(barmode="stack", legend={"orientation": "h", "x": 0, "y": 1.0, "yanchor": "bottom",
                                                  "font": {"size": 13, "color": SECONDARY}, "traceorder": "normal"})
    # Keep a single remaining category from becoming a full-height rectangle.
    figure = chart_layout(figure, max(4, len(counts)) * 32 + 88)
    figure.update_layout(margin={"b": 32})          # room for the axis figures
    return figure


def ranked_bars(series):
    """Thin neutral bars with the figure at the end, largest first."""
    series = series.sort_values()
    figure = go.Figure(go.Bar(
        y=series.index, x=series.values, orientation="h", marker={"color": NEUTRAL, "line": {"width": 0}},
        text=[f"{v:,}" for v in series.values], textposition="outside", cliponaxis=False,
        textfont={"family": MONO, "color": SECONDARY, "size": 12},
        hovertemplate="%{y}<br>Postings: %{x:,}<extra></extra>"))
    figure = chart_layout(figure, max(4, len(series)) * 30 + 16)
    figure.update_layout(bargap=0.55)
    figure.update_xaxes(showgrid=False, showticklabels=False)
    return figure


head, action = st.columns([5, 1], vertical_alignment="bottom")
head.title("Engineering job postings")
head.caption("Open postings from public company job boards, collected every night on AWS. "
             "A configured sample of employers, not the whole job market")

bucket = os.getenv("LAKE_BUCKET")
lock_table = os.getenv("PIPELINE_LOCK_TABLE")
reader_role_arn = os.getenv("AWS_DASHBOARD_ROLE_ARN")
if not bucket or not lock_table or not reader_role_arn:
    st.info("Set LAKE_BUCKET, PIPELINE_LOCK_TABLE and AWS_DASHBOARD_ROLE_ARN before starting. See docs/workshop.md")
    st.stop()
region = os.getenv("AWS_REGION", "ap-southeast-1")
database = os.getenv("ATHENA_DATABASE", "jobmarket_aws_gold_iceberg")
workgroup = os.getenv("ATHENA_WORKGROUP", "jobmarket-aws")


@st.cache_resource(ttl=2700, show_spinner=False)
def reader_session(region, role_arn):
    # Refresh before the one-hour STS credentials expire.
    return create_reader_session(region, role_arn)


@st.cache_data(ttl=900, show_spinner=False)
def load_fact(region, database, workgroup, bucket, reader_role_arn, fingerprint, snapshot, snapshot_id):
    # Fingerprint and table version are part of the cache key: a same-day republish invalidates it.
    return query_fact(reader_session(region, reader_role_arn), database, workgroup, snapshot, snapshot_id)


if action.button("Refresh data", icon=":material/refresh:", width="stretch"):
    load_fact.clear()
try:
    with st.spinner("Checking the publication and reading the current snapshot"):
        session = reader_session(region, reader_role_arn)
        metadata, before = read_publication(session, bucket, lock_table)
        rows, query = load_fact(region, database, workgroup, bucket, reader_role_arn, before,
                                metadata["snapshot_date"], metadata["fact_snapshot_id"])
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

# ---- snapshot stamp and coverage ------------------------------------------------------------------
boards = metadata["successful_boards"] + metadata["failed_boards"]
st.markdown(f'<div class="jm-stamp"><i></i>{html.escape(snapshot_stamp(metadata))}</div>', unsafe_allow_html=True)
coverage = (f"{metadata['successful_boards']} of {boards} boards collected · "
            f"observed {format_utc(metadata['ingested_at'])} · published {format_utc(metadata['published_at'])} · "
            "runs daily at 01:00 UTC+7, which is the previous date in UTC")
if metadata["incomplete_boards"]:
    coverage += (f" · {metadata['incomplete_boards']} board read in part; its unseen postings are not "
                 "treated as closed")
st.caption(coverage)
if metadata["failed_boards"]:
    st.warning(f"{metadata['failed_boards']} of {boards} boards failed last night. Their postings keep their "
               "last observation until they are seen again or expire")

frame = pd.DataFrame(rows)
for column in ("title", "company", "location", "source"):
    frame[column] = frame[column].fillna("Unknown")
FAMILY_COUNT = len(ROLE_FAMILIES)
frame["roles"] = frame["title"].map(classify)
frame["work_mode"] = frame["is_remote"].map(remote_label)
frame["apply_url"] = frame["apply_url"].map(safe_link)
frame["platform"] = frame["source"].map(platform_label)

# ---- filters ----------------------------------------------------------------------------------------
search_col, source_col, role_col, location_col, mode_col = st.columns([2, 1, 1.4, 1, 1])
search = search_col.text_input("Search", placeholder="Title or company, like data engineer")
sources = source_col.multiselect("Platform", sorted(frame.platform.unique()), placeholder="All")
roles = role_col.multiselect(
    "Role family", sorted({role for values in frame.roles for role in values}), placeholder="All",
    format_func=lambda role: f"Outside the {FAMILY_COUNT} families" if role == "Unclassified" else role,
)
locations = location_col.multiselect("Location", sorted(frame.location.unique()), placeholder="All",
                                     help="As the job board reports it")
work_modes = mode_col.multiselect("Work mode", REMOTE_LABELS, placeholder="All",
                                  format_func=lambda mode: "Not stated" if mode == "Unknown" else mode,
                                  help="Not stated means the source gave no remote or onsite signal. "
                                       "It is never counted as remote")
filtered = frame
if sources:
    filtered = filtered[filtered.platform.isin(sources)]
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

# ---- figures ----------------------------------------------------------------------------------------
cards = st.columns(4)
cards[0].metric("Postings", f"{len(filtered):,}")
cards[1].metric("Employers", f"{filtered.company.nunique():,}")
cards[2].metric("Locations", f"{filtered.location.nunique():,}", help="As the job boards report them")
mode_share = filtered.work_mode.value_counts(normalize=True).mul(100) if len(filtered) else {}
cards[3].metric("Remote", f"{mode_share.get('Remote', 0):.1f}%",
                help=f"Onsite or hybrid {mode_share.get('Onsite / hybrid', 0):.1f}% · "
                     f"not stated {mode_share.get('Unknown', 0):.1f}%. Not stated is never counted as remote")

if filtered.empty:
    st.info("No postings match these filters. Clear a filter to see more")
else:
    unmatched_count = int(filtered["roles"].map(lambda values: "Unclassified" in values).sum())
    matched = filtered[filtered["roles"].map(lambda values: "Unclassified" not in values)]
    if len(roles) == 1:
        st.subheader("Postings by platform")
        st.caption(f"Within {roles[0]}. Green is the share marked remote")
        with st.container(border=True):
            st.plotly_chart(stacked_by_mode(filtered, "platform"), width="stretch", theme=None,
                            config={"displayModeBar": False})
    else:
        st.subheader("Postings by role family")
        st.caption(f"{len(matched):,} postings match one of the {FAMILY_COUNT} families, decided by title; "
                   f"{unmatched_count:,} ({100 * unmatched_count / len(filtered):.1f}%) do not, such as sales or "
                   "hardware roles, and stay in the totals and the table. Green is the share marked remote")
        with st.container(border=True):
            if matched.empty:
                st.caption(f"No postings in this view match the {FAMILY_COUNT} role families")
            else:
                st.plotly_chart(stacked_by_mode(matched.explode("roles"), "roles"), width="stretch", theme=None,
                                config={"displayModeBar": False})

    employers_col, mode_col = st.columns([3, 2], gap="large")
    row_height = 300                                   # the two cards in this row share one height
    with employers_col:
        st.subheader("Employers with the most postings")
        with st.container(border=True, height=row_height):
            top = filtered.groupby("company").size().nlargest(8)
            st.plotly_chart(ranked_bars(top), width="stretch", theme=None, config={"displayModeBar": False})
    with mode_col:
        st.subheader("Work mode")
        with st.container(border=True, height=row_height):
            counts = filtered.work_mode.value_counts().reindex(REMOTE_LABELS, fill_value=0)
            figure_share = go.Figure()
            for mode in REMOTE_LABELS:
                figure_share.add_bar(x=[counts[mode]], y=[""], orientation="h",
                                     marker={"color": MODE_COLORS[mode], "line": {"width": 0}},
                                     hovertemplate=f"{'Not stated' if mode == 'Unknown' else mode}: %{{x:,}}"
                                                   "<extra></extra>")
            figure_share = chart_layout(figure_share, 56)
            figure_share.update_layout(barmode="stack", showlegend=False, bargap=0.2, margin={"l": 0, "r": 0})
            figure_share.update_xaxes(visible=False)
            figure_share.update_yaxes(visible=False)
            st.plotly_chart(figure_share, width="stretch", theme=None, config={"displayModeBar": False})
            total = max(1, int(counts.sum()))
            lines = "".join(
                f'<div class="jm-mode"><i style="background:{MODE_COLORS[mode]}"></i>'
                f'<span>{"Not stated" if mode == "Unknown" else mode}</span><b>{counts[mode]:,}</b>'
                f"<span>{100 * counts[mode] / total:.1f}%</span></div>" for mode in REMOTE_LABELS)
            st.markdown(f'<div class="jm-modes">{lines}</div>', unsafe_allow_html=True)
            st.caption("Not stated means the job board gave no remote or onsite signal")

# ---- postings ---------------------------------------------------------------------------------------
st.subheader("Postings")
st.caption("Every figure above follows the filters. Each row links to the employer's own posting")
# Postings in one of the role families first, then most recently seen, then employer. Most rows share
# the night's observation time, so recency alone would fall back to alphabetical employers.
table = filtered.assign(matched=filtered["roles"].map(lambda values: "Unclassified" not in values)).sort_values(
    ["matched", "last_seen_at", "company", "title"], ascending=[False, False, True, True]).assign(
    last_seen=lambda t: t["last_seen_at"].astype(str).str.slice(0, 16),
    work_mode=lambda t: t["work_mode"].replace({"Unknown": "Not stated"}))
st.dataframe(
    table[["title", "company", "location", "platform", "work_mode", "last_seen", "apply_url"]],
    hide_index=True, width="stretch",
    column_config={"title": st.column_config.TextColumn("Title", width="large"),
                   "company": "Employer", "location": "Location", "platform": "Platform",
                   "work_mode": "Work mode", "last_seen": "Last seen (UTC)",
                   "apply_url": st.column_config.LinkColumn("Posting", display_text="Open")})

with st.expander("Data scope and verification"):
    st.write("These postings were open and fresh when Gold was built. Availability can change afterwards, so "
             "check the employer's page. The workshop does not rank personal fit and does not cover Vietnam "
             "specifically.")
    st.write(f"{metadata['rows_ingested']:,} raw observations last night. Gold counts can differ because of "
             "deduplication and lifecycle retention.")
    st.caption(f"Run {metadata['run_id']} · Iceberg snapshot {metadata['fact_snapshot_id']} · "
               f"Athena query {query['query_id']}, {query['bytes_scanned'] / 2**20:.1f} MiB scanned. "
               "Results are cached for up to 15 minutes; publication health is checked on each interaction, "
               "and an idle page does not refresh on its own.")
