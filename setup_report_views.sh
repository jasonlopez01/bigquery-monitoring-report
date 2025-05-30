#!/usr/bin/env bash

# Exit when any command fails, or any variable is unassigned
set -eu

cat ./static/title.txt

echo ""
echo ""
echo "==========================================================================="
echo "This script will walkthrough getting some configuration inputs, \
and then creating the necessary BigQuery Views for a BigQuery Cost and Performance Looker Studio template Dashboard."
echo "==========================================================================="

#########################################################################################################
# Helper functions
#########################################################################################################
url_encode() {
    local text="$1"
    printf '%s' "$text" | xxd -p -c 1 | while read -r hex; do
        printf '%%%s' "$hex"
    done
    printf '\n'
}

#########################################################################################################
# Get Project ID
#########################################################################################################

# Prompt for project id
echo ""
read -r -p "Specify the GCP Project ID to create reporting Views in: " PROJECT_ID

# Check if project exists
if gcloud projects describe "${PROJECT_ID}" --format="value(projectId)"; then
    echo "✅ Project '${PROJECT_ID}' exists and you have access to it."
    gcloud config set project "${PROJECT_ID}"
else
    echo "❌ Project '${PROJECT_ID}' does not exist or you do not have access to it."
    exit 1
fi


#########################################################################################################
# Get Region
#########################################################################################################
echo ""
echo "Select a region to scope reports to: "
select REGION in "us" "eu"; do
    case ${REGION} in
        "us")
            echo "✅ You selected the '${REGION}' region."
            break
            ;;
        "eu")
            echo "✅ You selected the '${REGION}' region."
            break
            ;;
        *)
            echo "❌ Invalid selection. Please enter 1 or 2."
            ;;
    esac
done


#########################################################################################################
# Get Scope
#########################################################################################################
echo ""
echo "Scope reports to either an Organization or Project: "
select SCOPE in "PROJECT" "ORGANIZATION"; do
    case ${SCOPE} in
        "PROJECT")
            echo "✅ You selected the '${SCOPE}' scope."
            break
            ;;
        "ORGANIZATION")
            echo "✅ You selected the '${SCOPE}' scope."
            break
            ;;
        *)
            echo "❌ Invalid selection. Please enter 1 or 2."
            ;;
    esac
done


#########################################################################################################
# Get Dataset ID
#########################################################################################################
echo ""
# Prompt for dataset name
read -r -p "Specify the Dataset to create reporting Views in Project ID ${PROJECT_ID} [bigquery_reporting]: " DATASET_ID
DATASET_ID=${DATASET_ID:-bigquery_reporting}

# Check if dataset exists, create if not
if bq show --format=none  --project_id="${PROJECT_ID}" "${DATASET_ID}" ; then
    echo "✅ Dataset '${DATASET_ID}' exists in project '${PROJECT_ID}'."
else
    echo "⚠️ Dataset '${DATASET_ID}' does NOT exist in project '${PROJECT_ID}'."
    read -r -p "Do you want to create the dataset '${DATASET_ID}'? (y/n): " confirm
    if [[ "$confirm" =~ ^[Yy]$ ]]; then
        if bq --project_id="${PROJECT_ID}" --location="${REGION}" mk "${DATASET_ID}"; then
            echo "✅ Dataset '${DATASET_ID}' created successfully."
        else
            echo "❌ Failed to create dataset '${DATASET_ID}'."
            exit 1
        fi
    else
        echo "❌ Dataset creation aborted by user."
        exit 1
    fi
fi


#########################################################################################################
# Create GBQ Views
#########################################################################################################
# Set INFORMATION_SCHEMA source view references based on user input
INFO_SCHEMA_JOBS_REF="region-${REGION}.INFORMATION_SCHEMA.JOBS_BY_${SCOPE}"
INFO_SCHEMA_TABLE_STORAGE_REF="region-${REGION}.INFORMATION_SCHEMA.TABLE_STORAGE_BY_${SCOPE}"
INFO_SCHEMA_RECOMMENDATIONS_REF="region-${REGION}.INFORMATION_SCHEMA.RECOMMENDATIONS_BY_${SCOPE}"

#########################################################################################################
# Job Quota View
#########################################################################################################
JOB_QUOTA_TABLE_ID="bigquery_job_quota_utilization_region_${REGION}_by_${SCOPE}"
JOB_QUOTA_TABLE_REF="${DATASET_ID}.${JOB_QUOTA_TABLE_ID}"
echo ""
echo "⏳ Creating View '${JOB_QUOTA_TABLE_REF}' ..."

# Render SQL
JOB_QUOTA_SQL=$(cat ./sql/bigquery_job_quota_utilization.sql)
JOB_QUOTA_SQL="${JOB_QUOTA_SQL//INFO_SCHEMA_JOBS_REF/$INFO_SCHEMA_JOBS_REF}"
JOB_QUOTA_SQL="${JOB_QUOTA_SQL//INFO_SCHEMA_TABLE_STORAGE_REF/$INFO_SCHEMA_TABLE_STORAGE_REF}"

# Create View
if bq mk --use_legacy_sql=false --view="${JOB_QUOTA_SQL}" --project_id="${PROJECT_ID}" "${JOB_QUOTA_TABLE_REF}" ; then
  echo "✅ Created View '${JOB_QUOTA_TABLE_REF}'"
else
  echo "View already exists, updating View SQL..."
  bq update --use_legacy_sql=false --view="${JOB_QUOTA_SQL}" --project_id="${PROJECT_ID}" "${JOB_QUOTA_TABLE_REF}"
fi

#########################################################################################################
# Jobs View
#########################################################################################################
JOBS_TABLE_ID="bigquery_jobs_region_${REGION}_by_${SCOPE}"
JOBS_TABLE_REF="${DATASET_ID}.${JOBS_TABLE_ID}"
echo ""
echo "⏳ Creating View '${JOBS_TABLE_REF}' ..."

# Render SQL
JOBS_SQL=$(cat ./sql/bigquery_jobs.sql)
JOBS_SQL="${JOBS_SQL//INFO_SCHEMA_JOBS_REF/$INFO_SCHEMA_JOBS_REF}"
JOBS_SQL="${JOBS_SQL//INFO_SCHEMA_TABLE_STORAGE_REF/$INFO_SCHEMA_TABLE_STORAGE_REF}"

# Create View
if bq mk --use_legacy_sql=false --view="${JOBS_SQL}" --project_id="${PROJECT_ID}" "${JOBS_TABLE_REF}" ; then
  echo "✅ Created View '${JOBS_TABLE_REF}'"
else
  echo "View already exists, updating View SQL..."
  bq update --use_legacy_sql=false --view="${JOBS_SQL}" --project_id="${PROJECT_ID}" "${JOBS_TABLE_REF}"
fi

#########################################################################################################
# BigQuery GCP Recommendations View
#########################################################################################################
RECOMMENDATIONS_TABLE_ID="bigquery_gcp_recommendations_region_${REGION}_by_${SCOPE}"
RECOMMENDATIONS_TABLE_REF="${DATASET_ID}.${RECOMMENDATIONS_TABLE_ID}"
echo ""
echo "⏳ Creating View '${RECOMMENDATIONS_TABLE_REF}' ..."

# Render SQL
RECOMMENDATIONS_SQL=$(cat ./sql/bigquery_gcp_recommendations.sql)
RECOMMENDATIONS_SQL="${RECOMMENDATIONS_SQL//INFO_SCHEMA_RECOMMENDATIONS_REF/$INFO_SCHEMA_RECOMMENDATIONS_REF}"

# Create View
if bq mk --use_legacy_sql=false --view="${RECOMMENDATIONS_SQL}" --project_id="${PROJECT_ID}" "${RECOMMENDATIONS_TABLE_REF}" ; then
  echo "✅ Created View '${RECOMMENDATIONS_TABLE_REF}'"
else
  echo "View already exists, updating View SQL..."
  bq update --use_legacy_sql=false --view="${RECOMMENDATIONS_SQL}" --project_id="${PROJECT_ID}" "${RECOMMENDATIONS_TABLE_REF}"
fi


#########################################################################################################
# Storage View
#########################################################################################################
STORAGE_TABLE_ID="bigquery_storage_region_${REGION}_by_${SCOPE}"
STORAGE_TABLE_REF="${DATASET_ID}.${STORAGE_TABLE_ID}"
echo ""
echo "⏳ Creating View '${STORAGE_TABLE_REF}' ..."

# Need to determine if org or project scoped,
# for org must build a dynamic dataset billing model view, as there is no org-level, only project
if [[ "${SCOPE}" == "PROJECT" ]] ; then
  INFO_SCHEMA_SCHEMATA_OPTS_REF="region-${REGION}.INFORMATION_SCHEMA.SCHEMATA_OPTIONS"
elif [[ "${SCOPE}" == "ORGANIZATION" ]] ; then
  echo "⏳ Creating stored proc to build Org-level Dataset metadata View. "
  CREATE_ORG_SCHEMATA_OPTIONS_VIEW_PROC_REF="${PROJECT_ID}.${DATASET_ID}.create_update_org_schemata_options_view"
  INFO_SCHEMA_SCHEMATA_OPTS_REF="${PROJECT_ID}.${DATASET_ID}.bigquery_org_schemata_options_region_${REGION}"

  # Render stored proc sql
  CREATE_ORG_SCHEMATA_OPTIONS_VIEW_PROC_SQL=$(cat ./sql/create_update_org_schemata_options_view.sql)
  CREATE_ORG_SCHEMATA_OPTIONS_VIEW_PROC_SQL="${CREATE_ORG_SCHEMATA_OPTIONS_VIEW_PROC_SQL//CREATE_ORG_SCHEMATA_OPTIONS_VIEW_PROC_REF/$CREATE_ORG_SCHEMATA_OPTIONS_VIEW_PROC_REF}"
  CREATE_ORG_SCHEMATA_OPTIONS_VIEW_PROC_SQL="${CREATE_ORG_SCHEMATA_OPTIONS_VIEW_PROC_SQL//INFO_SCHEMA_SCHEMATA_OPTS_REF/$INFO_SCHEMA_SCHEMATA_OPTS_REF}"
  CREATE_ORG_SCHEMATA_OPTIONS_VIEW_PROC_SQL="${CREATE_ORG_SCHEMATA_OPTIONS_VIEW_PROC_SQL//INFO_SCHEMA_TABLE_STORAGE_REF/$INFO_SCHEMA_TABLE_STORAGE_REF}"

  # Created Stored proc and run it to create the custom org-level schemata options view
  bq query --project_id="${PROJECT_ID}" --use_legacy_sql=false "${CREATE_ORG_SCHEMATA_OPTIONS_VIEW_PROC_SQL}"
  bq query --project_id="${PROJECT_ID}" --use_legacy_sql=false "CALL ${CREATE_ORG_SCHEMATA_OPTIONS_VIEW_PROC_REF}()"

else
    echo "❌ Invalid SCOPE option '${SCOPE}'"
    exit 1
fi

# Render SQL for storage view
STORAGE_SQL=$(cat ./sql/bigquery_storage.sql)
STORAGE_SQL="${STORAGE_SQL//INFO_SCHEMA_JOBS_REF/$INFO_SCHEMA_JOBS_REF}"
STORAGE_SQL="${STORAGE_SQL//INFO_SCHEMA_TABLE_STORAGE_REF/$INFO_SCHEMA_TABLE_STORAGE_REF}"
STORAGE_SQL="${STORAGE_SQL//INFO_SCHEMA_SCHEMATA_OPTS_REF/$INFO_SCHEMA_SCHEMATA_OPTS_REF}"

# Create View
if bq mk --use_legacy_sql=false --view="${STORAGE_SQL}" --project_id="${PROJECT_ID}" "${STORAGE_TABLE_REF}" ; then
  echo "✅ Created View '${STORAGE_TABLE_REF}'"
else
  echo "View already exists, updating View SQL..."
  bq update --use_legacy_sql=false --view="${STORAGE_SQL}" --project_id="${PROJECT_ID}" "${STORAGE_TABLE_REF}"
fi


#########################################################################################################
# Create Looker Dashboard Copy/Create Link
#########################################################################################################
# https://developers.google.com/looker-studio/integrate
LOOKER_TEMPLATE_REPORT_ID="da83c07a-5d81-47cd-9f2e-3b11d093358b"

CREATE_LOOKER_DASHBOARD_LINK="https://lookerstudio.google.com/reporting/create?\
c.reportId=${LOOKER_TEMPLATE_REPORT_ID}\
&c.mode=view\
&c.explain=true\
&r.reportName=BigQueryMonitoringDashboardCOPY\
&ds.%2A.connector=bigQuery\
&ds.%2A.keepDatasourceName=true\
&ds.%2A.type=TABLE\
&ds.%2A.projectId=${PROJECT_ID}\
&ds.%2A.datasetId=${DATASET_ID}\
&ds.storage_source.tableId=${STORAGE_TABLE_ID}\
&ds.jobs_source.tableId=${JOBS_TABLE_ID}\
&ds.quotas_source.tableId=${JOB_QUOTA_TABLE_ID}"

echo ""
echo "==========================================================================="
echo "✅ All setup tasks are complete! Click below link to create your Looker Studio Dashboard!"
echo "${CREATE_LOOKER_DASHBOARD_LINK}"
echo "==========================================================================="