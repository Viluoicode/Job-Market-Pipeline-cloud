"""Display semantics; title rules intentionally match the Gold transform."""
from urllib.parse import urlparse

DEFAULT_ROLES = [('Backend Engineer',
  ['backend engineer', 'back-end engineer', 'backend developer', 'backend software engineer']),
 ('Frontend Engineer',
  ['frontend engineer', 'front-end engineer', 'frontend developer', 'ui engineer']),
 ('Full Stack Engineer', ['full stack', 'fullstack', 'full-stack engineer']),
 ('Data Engineer', ['data engineer', 'etl engineer', 'analytics engineer']),
 ('Data Scientist', ['data scientist', 'machine learning scientist']),
 ('Machine Learning Engineer', ['machine learning engineer', 'ml engineer', 'ai engineer']),
 ('DevOps Engineer',
  ['devops', 'site reliability', 'sre', 'platform engineer', 'infrastructure engineer']),
 ('Mobile Engineer', ['mobile engineer', 'ios engineer', 'android engineer', 'mobile developer'])]


def classify(title):
    title = (title or "").lower()
    return [role for role, patterns in DEFAULT_ROLES if any(p in title for p in patterns)] or ["Unclassified"]


def safe_link(value):
    value = str(value or "")
    parsed = urlparse(value)
    return value if parsed.scheme in {"https", "http"} and parsed.hostname else None
