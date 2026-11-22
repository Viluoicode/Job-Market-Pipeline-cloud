"""Published evidence must stay valid JSON and must not expose the AWS account ID."""
import json
import re
from pathlib import Path

import pytest

EVIDENCE = sorted((Path(__file__).resolve().parents[1] / "docs" / "evidence").glob("*.json"))
# An account ID appears as the 5th ARN field or inside this project's bucket names.
ACCOUNT_IN_ARN = re.compile(r"arn:aws:[a-z0-9-]+:[a-z0-9-]*:(\d{12}):")
ACCOUNT_IN_BUCKET = re.compile(r"jobmarket-aws-(\d{12})-")
ACCOUNT_FIELD = re.compile(r'"(?:account|account_id|Account)"\s*:\s*"(\d{12})"')


@pytest.mark.parametrize("path", EVIDENCE, ids=lambda p: p.name)
def test_evidence_is_json_without_account_id(path):
    text = path.read_text(encoding="utf-8")
    json.loads(text)
    leaks = {m for pattern in (ACCOUNT_IN_ARN, ACCOUNT_IN_BUCKET, ACCOUNT_FIELD) for m in pattern.findall(text)}
    assert not leaks, f"{path.name} exposes AWS account ID(s); replace with REDACTED_ACCOUNT_ID"
