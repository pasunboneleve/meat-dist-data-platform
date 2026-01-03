#!/bin/env bash

set -euox pipefail

REPORT_DATE=${1:-$(date +"%F")}
DATA="{\"target_date\":\"$REPORT_DATE\"}"

#
# This scripts triggers the synthetic meat ingestor Cloud Run service
#

set +x
token=$(gcloud auth print-identity-token)
url=$(gcloud run services describe synthetic-meat-ingestor \
              --region australia-southeast1 --format 'value(status.address.url)')
curl ${url} \
     -H "Authorization: Bearer ${token}" \
     -H "Content-Type: application/json" \
     -d ${DATA}
