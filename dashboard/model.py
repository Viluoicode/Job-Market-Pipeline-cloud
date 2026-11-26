"""Display semantics; title rules intentionally match the Gold transform."""
import re
from datetime import datetime, timezone
from urllib.parse import urlparse

# Identical copy of the Gold taxonomy in glue/jobs/silver_to_gold.py (tests enforce equality).
ROLE_EXCLUDE = [
    r"\b(?:product|program|project|technical program|account|marketing|partner|delivery) manager\b",
    r"\b(?:recruit\w*|sourcer|assistant|counsel|account executive|account director|account manager)\b",
    r"\b(?:market|ux|user|product|qualitative|design|experience) researcher\b",
]
DEFAULT_ROLES = [
    ("Forward Deployed / Solutions", [
        r"\bforward[\- ]deployed\b", r"\bfde\b", r"\bdeployment strategist\b",
        r"\bsolutions? (?:engineer\w*|architect|consultant)\b",
        r"\b(?:sales|presales|pre-sales|customer|field|partner|value) engineer\w*\b",
        r"\bimplementation (?:engineer|consultant)\b", r"\bdeveloper (?:advocate|relations)\b",
    ]),
    ("Engineering Manager", [
        r"\bengineering manager\b", r"\bmanager,? (?:of )?(?:software )?engineering\b",
        r"\b(?:head|director|vp) of engineering\b",
    ]),
    ("Research Scientist / Engineer", [r"\bresearch (?:engineer|scientist)\b", r"\bresearcher\b"]),
    ("ML / AI Engineer", [
        r"\bmachine learning (?:engineer|systems engineer)\b", r"\bml (?:engineer|ops)\b", r"\bmlops\b",
        r"\b(?:applied )?ai (?:engineer|software engineer)\b", r"\bllm engineer\b",
    ]),
    ("Data Scientist", [r"\bdata scien(?:tist|ce)\b", r"\b(?:machine learning|applied) scientist\b"]),
    ("Data Engineer", [r"\bdata engineer(?:ing)?\b", r"\betl (?:engineer|developer)\b", r"\banalytics engineer\b"]),
    ("Data Analyst / BI", [
        r"\bdata analyst\b", r"\b(?:business intelligence|bi) (?:analyst|engineer|developer)\b", r"\bproduct analyst\b",
    ]),
    ("Security Engineer", [
        r"\bsecurity (?:engineer|software engineer|operations engineer|systems engineer)\b",
        r"\b(?:application|product|cloud|infrastructure|offensive) security engineer\b",
        r"\bdetection (?:and response )?engineer\b",
    ]),
    ("DevOps / SRE / Platform", [
        r"\bdevops\b", r"\bsite reliability\b", r"\bsre\b",
        r"\b(?:platform|infrastructure|cloud|kubernetes|production) engineer\b",
    ]),
    ("Mobile Engineer", [r"\b(?:mobile|ios|android)\b"]),
    ("Full Stack Engineer", [r"\bfull[\- ]?stack\b"]),
    ("Frontend Engineer", [r"\bfront[\- ]?end\b", r"\bui engineer\b", r"\bweb (?:engineer|developer)\b"]),
    ("Backend Engineer", [
        r"\bback[\- ]?end\b", r"\bapi engineer\b", r"\bdistributed systems\b",
        r"\b(?:java|golang|go|python|scala|rust|c\+\+) (?:software )?(?:engineer|developer)\b",
        r"\.net (?:software )?(?:engineer|developer)\b",
    ]),
    # Second pass: the specialisation written as a qualifier of a generic engineering title.
    ("Data Engineer", [
        r"(?:engineer|engineering|developer|entwickler)\b.*\b(?:data (?:platform|infrastructure|pipelines?)|big data)\b",  # noqa: E501
    ]),
    ("ML / AI Engineer", [
        r"(?:engineer|engineering|developer|entwickler)\b.*\b(?:machine learning|ml infrastructure)\b",
    ]),
    ("Security Engineer", [r"(?:engineer|engineering|developer|entwickler)\b.*\bsecurity\b"]),
    ("DevOps / SRE / Platform", [
        r"(?:engineer|engineering|developer|entwickler)\b.*\b(?:infrastructure|infra|observability|ci/cd|developer productivity|developer infrastructure|reliability)\b",  # noqa: E501
    ]),
    ("Technical Support Engineer", [r"\bsupport engineer\b", r"\btechnical (?:support|services) engineer\b"]),
    ("Software Engineer (General)", [
        r"\bsoftware (?:engineer|developer|engineering)\b", r"\bsoftwareentwickler\b",
        r"\bmember of technical staff\b", r"\bfounding engineer\b", r"^(?:senior |staff |principal |lead )?engineer\b",
    ]),
]
ROLE_FAMILIES = list(dict.fromkeys(role for role, _ in DEFAULT_ROLES))
_EXCLUDE = [re.compile(regex) for regex in ROLE_EXCLUDE]
_ROLE_REGEXES = [(role, [re.compile(regex) for regex in regexes]) for role, regexes in DEFAULT_ROLES]


def classify(title):
    """[role] for the first matching family by priority, or ["Unclassified"] (same rule as Gold)."""
    title = (title or "").lower()
    if not any(regex.search(title) for regex in _EXCLUDE):
        for role, regexes in _ROLE_REGEXES:
            if any(regex.search(title) for regex in regexes):
                return [role]
    return ["Unclassified"]


# is_remote is tri-state: True = fully remote, False = onsite or hybrid, None = no signal.
REMOTE_LABELS = ["Remote", "Onsite / hybrid", "Unknown"]


def remote_label(value):
    """Label for one is_remote value; accepts Python/numpy booleans and None/NaN/pd.NA as unknown."""
    try:
        if value == True:  # noqa: E712 - numpy.bool_ is not `True`, but compares equal
            return "Remote"
        if value == False:  # noqa: E712
            return "Onsite / hybrid"
    except TypeError:  # pd.NA cannot be coerced to bool
        pass
    return "Unknown"


def safe_link(value):
    value = str(value or "")
    parsed = urlparse(value)
    return value if parsed.scheme in {"https", "http"} and parsed.hostname else None


# ---- presentation ---------------------------------------------------------------------------------
PLATFORM_NAMES = {"greenhouse": "Greenhouse", "lever": "Lever", "ashby": "Ashby", "arbeitnow": "Arbeitnow"}


def platform_label(source):
    return PLATFORM_NAMES.get(source, str(source).capitalize())


def format_utc(value):
    """One timestamp format for the whole page, e.g. "25 Nov 2026, 18:01 UTC".

    The manifest, the completion marker and Athena write timestamps differently (offset, Z, none).
    """
    if not value:
        return "Unknown"
    text = str(value).replace("Z", "+00:00").replace(" ", "T", 1)
    try:
        moment = datetime.fromisoformat(text)
    except ValueError:
        return str(value)
    moment = moment.replace(tzinfo=timezone.utc) if moment.tzinfo is None else moment.astimezone(timezone.utc)
    return f"{moment.day} {moment:%b %Y, %H:%M} UTC"


def snapshot_stamp(metadata):
    """The page's snapshot stamp: published date, pinned Iceberg version and data age."""
    day = datetime.fromisoformat(metadata["snapshot_date"])
    return (f"Snapshot {day.day} {day:%b %Y} · Iceberg version …{metadata['fact_snapshot_id'][-4:]} · "
            f"{metadata['ingestion_age_hours']:.1f} h old")
