SELECT
  project_id,
  user_email,
  SPLIT(user_email, '@')[OFFSET(0)] AS user_name,
  job_id,
  destination_table.dataset_id AS destination_table_dataset_id,
  destination_table.table_id AS destination_table_id,
  creation_time,
  DATE(creation_time) as creation_date,
  job_type,
  statement_type,
  priority,
  query,
  state,
  cache_hit,
  error_result.reason AS error_reason,
  IF(error_result is NULL, FALSE, TRUE) AS error,
  total_bytes_processed,
  total_bytes_billed,
  (total_bytes_processed / POWER(1024, 3)) AS gb_processed,
  (total_bytes_billed / POWER(1024, 3)) AS gb_billed,
  -- As of 2025-01-01 $/TiB is 6.25, https://cloud.google.com/bigquery/pricing
  ((total_bytes_billed / POWER(1024, 4)) * 6.25) AS est_cost_usd,
  ARRAY_TO_STRING( ARRAY(
    SELECT
      FORMAT("%s=%s", label.key, label.value)
    FROM
      UNNEST(labels) AS label
    ORDER BY
      label.key ), ", " ) AS parsed_labels,
  TIMESTAMP_DIFF(end_time, start_time, SECOND) AS duration_seconds,
  COUNT(DISTINCT job_id) OVER (PARTITION BY query) AS recurrence_count,
  FORMAT(
    "https://console.cloud.google.com/bigquery?project=%s&j=bq:%s:%s&page=jobresults",
    project_id, 'US', job_id
  ) AS job_link,
  query_info.query_hashes.normalized_literals as query_hash,
-- NOTE: can reference `region-<REGION (ex. us or eu)>.INFORMATION_SCHEMA.JOBS<_BY_PROJECT or _BY_ORGANIZATION>`
-- ex: `region-us.INFORMATION_SCHEMA.JOBS_BY_PROJECT`, `region-eu.INFORMATION_SCHEMA.JOBS_BY_ORGANIZATION`, etc.
FROM `INFO_SCHEMA_JOBS_REF`
WHERE
  creation_time > '1990-01-01'