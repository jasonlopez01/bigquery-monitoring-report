WITH daily_quotas AS (
  -- https://cloud.google.com/bigquery/quotas
  SELECT
    1500 AS load_job_per_table,
    100000 AS load_job_per_project,
    100000 AS copy_job_per_project,
    100000 AS export_job_per_project,
),
table_load_job_quotas AS (
  SELECT
    DATE(creation_time) as creation_date,
    destination_table.project_id as project_id,
    destination_table.dataset_id as dataset_id,
    destination_table.table_id as table_id,
    CONCAT(destination_table.project_id, '.', destination_table.dataset_id, '.', destination_table.table_id) as source_ref,
    COUNT(job_id) AS job_count,
    ROUND(100 * (COUNT(job_id) / daily_quotas.load_job_per_table), 2) as quota_utilization_percent,
    'table' as quota_scope,
    job_type
  -- NOTE: can reference `region-<REGION, ex. us or eu>.INFORMATION_SCHEMA.JOBS<_BY_PROJECT or _BY_ORGANIZATION>`
  -- ex: `region-us.INFORMATION_SCHEMA.JOBS_BY_PROJECT`, `region-eu.INFORMATION_SCHEMA.JOBS_BY_ORGANIZATION`, etc.
  FROM `INFO_SCHEMA_JOBS_REF`
  LEFT JOIN daily_quotas
    ON 1 = 1
  WHERE
    creation_time > '1990-01-01'
    AND job_type = "LOAD"
  GROUP BY
    creation_date,
    project_id,
    dataset_id,
    table_id,
    job_type,
    daily_quotas.load_job_per_table
),
project_load_job_quotas AS (
  SELECT
    DATE(creation_time) as creation_date,
    project_id,
    CAST(NULL AS STRING) as dataset_id,
    CAST(NULL AS STRING) as table_id,
    project_id as source_ref,
    COUNT(job_id) AS job_count,
    ROUND(100 * (COUNT(job_id) / daily_quotas.load_job_per_project), 2) as quota_utilization_percent,
    'project' as quota_scope,
    job_type
    -- NOTE: can reference `region-<REGION, ex. us or eu>.INFORMATION_SCHEMA.JOBS<_BY_PROJECT or _BY_ORGANIZATION>`
    -- ex: `region-us.INFORMATION_SCHEMA.JOBS_BY_PROJECT`, `region-eu.INFORMATION_SCHEMA.JOBS_BY_ORGANIZATION`, etc.
    FROM `INFO_SCHEMA_JOBS_REF`
  LEFT JOIN daily_quotas
    ON 1 = 1
  WHERE
    creation_time > '1990-01-01'
    AND job_type = "LOAD"
  GROUP BY
    creation_date,
    project_id,
    job_type,
    daily_quotas.load_job_per_project
),
project_copy_job_quotas AS (
  SELECT
    DATE(creation_time) as creation_date,
    project_id,
    CAST(NULL AS STRING) as dataset_id,
    CAST(NULL AS STRING) as table_id,
    project_id as source_ref,
    COUNT(job_id) AS job_count,
    ROUND(100 * (COUNT(job_id) / daily_quotas.load_job_per_project), 2) as quota_utilization_percent,
    'project' as quota_scope,
    job_type
  -- NOTE: can reference `region-<REGION, ex. us or eu>.INFORMATION_SCHEMA.JOBS<_BY_PROJECT or _BY_ORGANIZATION>`
  -- ex: `region-us.INFORMATION_SCHEMA.JOBS_BY_PROJECT`, `region-eu.INFORMATION_SCHEMA.JOBS_BY_ORGANIZATION`, etc.
  FROM `INFO_SCHEMA_JOBS_REF`
  LEFT JOIN daily_quotas
    ON 1 = 1
  WHERE
    creation_time > '1990-01-01'
    AND job_type = "COPY"
  GROUP BY
    creation_date,
    project_id,
    job_type,
    daily_quotas.load_job_per_project
),
project_export_job_quotas AS (
  SELECT
    DATE(creation_time) as creation_date,
    project_id,
    CAST(NULL AS STRING) as dataset_id,
    CAST(NULL AS STRING) as table_id,
    project_id as source_ref,
    COUNT(job_id) AS job_count,
    ROUND(100 * (COUNT(job_id) / daily_quotas.load_job_per_project), 2) as quota_utilization_percent,
    'project' as quota_scope,
    job_type
   -- NOTE: can reference `region-<REGION, ex. us or eu>.INFORMATION_SCHEMA.JOBS<_BY_PROJECT or _BY_ORGANIZATION>`
   -- ex: `region-us.INFORMATION_SCHEMA.JOBS_BY_PROJECT`, `region-eu.INFORMATION_SCHEMA.JOBS_BY_ORGANIZATION`, etc.
  FROM `INFO_SCHEMA_JOBS_REF`
  LEFT JOIN daily_quotas
    ON 1 = 1
  WHERE
    creation_time > '1990-01-01'
    AND job_type = "EXPORT"
  GROUP BY
    creation_date,
    project_id,
    job_type,
    daily_quotas.load_job_per_project
)

SELECT
  *
FROM table_load_job_quotas
UNION ALL
SELECT
  *
FROM project_load_job_quotas
UNION ALL
SELECT
  *
FROM project_copy_job_quotas
UNION ALL
SELECT
  *
FROM project_export_job_quotas