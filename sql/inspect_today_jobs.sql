-- Workgroup: jobmarket-aws. Database: jobmarket_aws_gold.
-- Require a healthy pipeline before interpreting the latest snapshot as current.
-- Observations are stored as UTC; display and filter the calendar day in Vietnam (UTC+7).
-- These are observed postings, not necessarily newly created vacancies.
SELECT source, board_token, company, title, location, is_remote, apply_url,
       last_seen_at AS observed_at_utc,
       date_format(last_seen_at + INTERVAL '7' HOUR, '%Y-%m-%d %H:%i:%s') AS observed_at_vietnam,
       first_seen_at, snapshot_date
FROM jobmarket_aws_gold.fact_job_posting
WHERE snapshot_date = (SELECT max(snapshot_date) FROM jobmarket_aws_gold.fact_job_posting)
  AND CAST(last_seen_at + INTERVAL '7' HOUR AS DATE)
      = CAST(current_timestamp AT TIME ZONE 'Asia/Ho_Chi_Minh' AS DATE)
ORDER BY last_seen_at DESC, company, title
LIMIT 100;
