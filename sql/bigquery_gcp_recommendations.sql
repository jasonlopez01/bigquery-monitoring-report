SELECT
  project_id,
  description,
  target_resources,
  primary_impact.category as primary_category,
  additional_details.overview as overview,
  state,
  priority
-- NOTE: can reference `region-<REGION, ex. us or eu>.INFORMATION_SCHEMA.RECOMMENDATIONS<_BY_PROJECT or _BY_ORGANIZATION>`
-- ex: `region-us.INFORMATION_SCHEMA.RECOMMENDATIONS_BY_PROJECT`, `region-eu.INFORMATION_SCHEMA.RECOMMENDATIONS_BY_PROJECT`, etc.
FROM `INFO_SCHEMA_RECOMMENDATIONS_REF`
WHERE state = 'ACTIVE'