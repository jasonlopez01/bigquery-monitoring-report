WITH constants AS (
  SELECT
    -- https://cloud.google.com/bigquery/pricing
    0.02 AS active_logical_gib_price,
    0.01 AS long_term_logical_gib_price,
    0.04 AS active_physical_gib_price,
    0.02 AS long_term_physical_gib_price,
    -- https://cloud.google.com/bigquery/quotas#partitioned_tables
    9900 AS partition_warning_threshold,
    180 AS clean_up_table_warning_threshold_days
),
referenced_tables AS (
  SELECT
    job_id,
    creation_time,
    referenced_table.project_id as referenced_project_id,
    referenced_table.dataset_id as referenced_dataset_id,
    referenced_table.table_id as referenced_table_id,
  FROM
    -- NOTE: can reference `region-<REGION (ex. us or eu)>.INFORMATION_SCHEMA.JOBS<_BY_PROJECT or _BY_ORGANIZATION>`
    -- ex: `region-us.INFORMATION_SCHEMA.JOBS_BY_PROJECT`, `region-eu.INFORMATION_SCHEMA.JOBS_BY_ORGANIZATION`, etc
    `INFO_SCHEMA_JOBS_REF`,
    UNNEST(referenced_tables) as referenced_table
  WHERE
    creation_time > '1990-01-01'
    AND job_type = 'QUERY'
    AND state = 'DONE'
    AND UPPER(query) NOT LIKE '%INFORMATION_SCHEMA%'
    AND error_result is NULL
),
latest_table_references AS (
  SELECT
    referenced_project_id,
    referenced_dataset_id,
    referenced_table_id,
    MAX(creation_time) as last_referenced_at
  FROM referenced_tables
  GROUP BY ALL
),
dataset_pricing_model AS (
  SELECT
    catalog_name as project_id,
    schema_name as dataset_id,
    option_value as pricing_model
  FROM `INFO_SCHEMA_SCHEMATA_OPTS_REF` AS dataset_options
  WHERE option_name = 'storage_billing_model'
),
table_storage_gibs AS (
  SELECT
    table_storage.project_id,
    table_schema as dataset_id,
    table_name as table_id,
    creation_time,
    storage_last_modified_time,
    table_type,
    total_rows,
    total_partitions,
    COALESCE(dataset_pricing_model.pricing_model, 'LOGICAL') AS dataset_billing_model,
    -- Convert storage bytes to Gibs
    (total_physical_bytes / POWER(1024,3)) AS total_physical_gibs,
    (total_logical_bytes / POWER(1024,3)) AS total_logical_gibs,
    (active_physical_bytes / POWER(1024,3)) AS active_physical_gibs,
    (active_logical_bytes / POWER(1024,3)) AS active_logical_gibs,
    (long_term_physical_bytes / POWER(1024,3)) AS long_term_physical_gibs,
    (long_term_logical_bytes / POWER(1024,3)) AS long_term_logical_gibs,
    (fail_safe_physical_bytes / POWER(1024,3)) AS fail_safe_physical_gibs
  -- NOTE: can reference `region-<REGION (ex. us or eu)>.INFORMATION_SCHEMA.TABLE_STORAGE<_BY_PROJECT or _BY_ORGANIZATION>`
  -- ex: `region-us.INFORMATION_SCHEMA.TABLE_STORAGE_BY_PROJECT`, `region-eu.INFORMATION_SCHEMA.TABLE_STORAGE_BY_ORGANIZATION`, etc
  FROM `INFO_SCHEMA_TABLE_STORAGE_REF` AS table_storage
  LEFT JOIN dataset_pricing_model
  ON
    table_storage.project_id = dataset_pricing_model.project_id
    AND
    table_storage.table_schema = dataset_pricing_model.dataset_id
  WHERE
    deleted is FALSE
)

SELECT
  project_id,
  dataset_id,
  table_id,
  creation_time,
  storage_last_modified_time,
  table_type,
  total_rows,
  total_partitions,
  dataset_billing_model,
  CASE WHEN dataset_billing_model = 'PHYSICAL'
    THEN total_physical_gibs
    ELSE total_logical_gibs
  END as total_gibs,
  CASE WHEN dataset_billing_model = 'PHYSICAL'
    THEN active_physical_gibs
    ELSE active_logical_gibs
  END AS active_gibs,
  CASE WHEN dataset_billing_model = 'PHYSICAL'
    THEN long_term_physical_gibs
    ELSE long_term_logical_gibs
  END AS long_term_gibs,
  -- https://cloud.google.com/bigquery/docs/information-schema-table-storage#forecast_storage_billing
  CASE WHEN dataset_billing_model = 'PHYSICAL' AND table_type = 'BASE TABLE'
    THEN ROUND(((active_physical_gibs + fail_safe_physical_gibs) * active_physical_gib_price) + (long_term_physical_gibs * long_term_physical_gib_price),2)
    ELSE ROUND((active_logical_gibs * active_logical_gib_price) + (long_term_logical_gibs * long_term_logical_gib_price),2)
  END AS est_monthly_cost_usd,
  -- Warnings / recommendations
  IF(total_partitions > partition_warning_threshold, TRUE, FALSE) partition_limit_warning,
  ROUND(
    -- Est. logical monthly cost
    (active_logical_gibs * active_logical_gib_price) + (long_term_logical_gibs * long_term_logical_gib_price) -
    -- Est. physical monthly cost
    ((active_physical_gibs + fail_safe_physical_gibs) * active_physical_gib_price) + (long_term_physical_gibs * long_term_physical_gib_price),
  2) AS est_total_cost_diff_physical_pricing,
    FORMAT(
    "https://console.cloud.google.com/bigquery?project=%s&ws=!1m5!1m4!4m3!1s%s!2s%s!3s%s",
    project_id, project_id, dataset_id, table_id
  ) AS table_link,
    FORMAT(
    "https://console.cloud.google.com/bigquery?project=%s&ws=!1m2!1s%s!2s%s",
    project_id, project_id, dataset_id
  ) AS dataset_link,
  DATE_DIFF(CURRENT_TIMESTAMP(), storage_last_modified_time, DAY) as days_since_last_update,
  DATE_DIFF(CURRENT_TIMESTAMP(), creation_time, DAY) as age_days,
  total_logical_gibs >= 10 AND total_partitions < 1 AS partitioning_recommended,
  latest_table_references.last_referenced_at,
  DATE_DIFF(CURRENT_TIMESTAMP(), latest_table_references.last_referenced_at, DAY) AS days_since_last_referenced,
  (
    -- Table not referenced in X days
    DATE_DIFF(CURRENT_TIMESTAMP(), storage_last_modified_time, DAY) >= constants.clean_up_table_warning_threshold_days
    OR
    -- Table not updated in X days
    DATE_DIFF(CURRENT_TIMESTAMP(), latest_table_references.last_referenced_at, DAY) >= constants.clean_up_table_warning_threshold_days
  ) AS table_cleanup_warning,
  CONCAT(project_id, '.', dataset_id, '.', table_id) as table_ref
FROM table_storage_gibs
LEFT JOIN constants
ON
  1=1
LEFT JOIN latest_table_references
ON
  table_storage_gibs.project_id = latest_table_references.referenced_project_id
  AND table_storage_gibs.dataset_id = latest_table_references.referenced_dataset_id
  AND table_storage_gibs.table_id = latest_table_references.referenced_table_id