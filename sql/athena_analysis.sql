-- Athena analysis over the Gold marts (database: jobmarket_aws_gold).
-- Console: pick the `jobmarket-aws` workgroup, then run these one at a time.
-- Parquet + snapshot_date partitions keep every query well under 1 cent (the workgroup also
-- caps each query at 10 GB scanned). Screenshot the results for your portfolio.

-- 1) Headline leaderboard — demand by target role for the latest snapshot.
SELECT role, job_count
FROM jobmarket_aws_gold.demand_by_role
WHERE snapshot_date = (SELECT max(snapshot_date) FROM jobmarket_aws_gold.demand_by_role)
ORDER BY job_count DESC;

-- 2) Remote vs on-site split across all active postings (latest snapshot).
SELECT
    is_remote,
    count(*)                                              AS postings,
    round(100.0 * count(*) / sum(count(*)) OVER (), 1)   AS pct
FROM jobmarket_aws_gold.fact_job_posting
WHERE snapshot_date = (SELECT max(snapshot_date) FROM jobmarket_aws_gold.fact_job_posting)
GROUP BY is_remote
ORDER BY postings DESC;

-- 3) Top hiring companies (latest snapshot).
SELECT company, count(*) AS postings
FROM jobmarket_aws_gold.fact_job_posting
WHERE snapshot_date = (SELECT max(snapshot_date) FROM jobmarket_aws_gold.fact_job_posting)
GROUP BY company
ORDER BY postings DESC
LIMIT 20;

-- 4) Where the postings come from — volume by source platform.
SELECT source, count(*) AS postings
FROM jobmarket_aws_gold.fact_job_posting
WHERE snapshot_date = (SELECT max(snapshot_date) FROM jobmarket_aws_gold.fact_job_posting)
GROUP BY source
ORDER BY postings DESC;

-- 5) Remote ratio by source (which platforms skew remote).
SELECT
    source,
    count(*)                                                          AS postings,
    sum(if(is_remote, 1, 0))                                          AS remote_postings,
    round(100.0 * sum(if(is_remote, 1, 0)) / count(*), 1)            AS remote_pct
FROM jobmarket_aws_gold.fact_job_posting
WHERE snapshot_date = (SELECT max(snapshot_date) FROM jobmarket_aws_gold.fact_job_posting)
GROUP BY source
ORDER BY postings DESC;

-- 6) Demand trend over time — handy once you have more than one snapshot.
SELECT snapshot_date, role, job_count
FROM jobmarket_aws_gold.demand_by_role
ORDER BY snapshot_date DESC, job_count DESC;

-- 7) The Data Engineer market (the role this portfolio targets) — sample of live postings.
SELECT company, title, location, is_remote, apply_url
FROM jobmarket_aws_gold.fact_job_posting
WHERE snapshot_date = (SELECT max(snapshot_date) FROM jobmarket_aws_gold.fact_job_posting)
  AND lower(title) LIKE '%data engineer%'
ORDER BY company
LIMIT 50;
