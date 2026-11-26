"""Salary is read from fields each ATS publishes as data; anything else stays NULL."""
import httpx
import land_to_bronze as ingest
import pytest

NONE = {"salary_min": None, "salary_max": None, "salary_currency": None, "salary_interval": None}

# Shapes copied from real Bronze raw_json (2026-11-26 run).
ASHBY = {"compensation": {"summaryComponents": [
    {"compensationType": "EquityPercentage", "interval": "NONE", "currencyCode": None, "minValue": None, "maxValue": None},
    {"compensationType": "Salary", "interval": "1 YEAR", "currencyCode": "USD", "minValue": 165000, "maxValue": 330000},
]}}
LEVER = {"salaryRange": {"interval": "per-year-salary", "currency": "USD", "min": 150000, "max": 180000}}


def greenhouse(title, low, high):
    block = (f'<div class="title">{title}</div><div class="pay-range"><span>{low}</span>'
             f'<span class="divider">&mdash;</span><span>{high}</span></div>')
    # The API entity-encodes content HTML.
    return {"content": block.replace("<", "&lt;").replace(">", "&gt;")}


@pytest.mark.parametrize("source, job, expected", [
    ("ashby", ASHBY, {"salary_min": 165000.0, "salary_max": 330000.0, "salary_currency": "USD", "salary_interval": "year"}),
    ("lever", LEVER, {"salary_min": 150000.0, "salary_max": 180000.0, "salary_currency": "USD", "salary_interval": "year"}),
    ("lever", {"salaryRange": {"interval": "per-hour-wage", "currency": "usd", "min": 40, "max": 55}},
     {"salary_min": 40.0, "salary_max": 55.0, "salary_currency": "USD", "salary_interval": "hour"}),
    ("greenhouse", greenhouse("Canada Annual Pay Range", "$58,000", "$68,000 CAD"),
     {"salary_min": 58000.0, "salary_max": 68000.0, "salary_currency": "CAD", "salary_interval": "year"}),
    ("greenhouse", greenhouse("Germany Annual Pay Range", "€114.000", "€142.000 EUR"),
     {"salary_min": 114000.0, "salary_max": 142000.0, "salary_currency": "EUR", "salary_interval": "year"}),
    ("greenhouse", greenhouse("Mexico Monthly Pay Range", "$147,000", "$183,000 MXN"),
     {"salary_min": 147000.0, "salary_max": 183000.0, "salary_currency": "MXN", "salary_interval": "month"}),
    # "Pay Range" alone: keep the amounts, never guess the period.
    ("greenhouse", greenhouse("Pay Range", "$170.40", "$255.60 USD"),
     {"salary_min": 170.4, "salary_max": 255.6, "salary_currency": "USD", "salary_interval": None}),
])
def test_structured_salary_per_source(source, job, expected):
    assert ingest.extract_salary(source, job) == expected


@pytest.mark.parametrize("source, job", [
    ("ashby", {"compensation": {"summaryComponents": [{"compensationType": "EquityPercentage"}]}}),
    ("ashby", {"compensation": "oops"}),
    ("lever", {"salaryRange": {"min": 0, "max": None}}),
    ("lever", {"salaryRange": {"min": 200000, "max": 100000, "interval": "per-year-salary"}}),  # inverted
    ("greenhouse", {"content": "Competitive salary and equity"}),                                # free text is not parsed
    ("greenhouse", greenhouse("Pay Range", "DOE", "TBD")),
    ("arbeitnow", {"salary": "60k"}),
    ("ashby", None),
])
def test_missing_or_unusable_salary_is_null(source, job):
    assert ingest.extract_salary(source, job) == NONE


@pytest.mark.parametrize("text, value", [
    ("$58,000", 58000.0), ("€1.250.000", 1250000.0), ("£45,500.50", 45500.5), ("12,75 €", 12.75),
    ("45.5", 45.5), ("$100", 100.0), (True, None), ("-", None), (-5, None),
])
def test_amount_formats(text, value):
    assert ingest._amount(text) == value


def test_one_sided_range_uses_the_same_value_twice():
    salary = ingest.extract_salary("lever", {"salaryRange": {"min": 90000, "currency": "EUR", "interval": "per-year-salary"}})
    assert (salary["salary_min"], salary["salary_max"]) == (90000.0, 90000.0)


def test_bronze_record_carries_salary_columns():
    job = {"id": "a", "title": "Data Engineer", **ASHBY}
    with httpx.Client(transport=httpx.MockTransport(lambda req: httpx.Response(200, json={"jobs": [job]}))) as client:
        record = ingest.fetch_ashby(client, "t", "T")[0]
    assert (record["salary_min"], record["salary_currency"], record["salary_interval"]) == (165000.0, "USD", "year")
