#!/bin/env bash

set -euox pipefail

#
# This scripts triggers the synthetic meat ingestor Cloud Run service
#

token=$(gcloud auth print-identity-token)
url=$(gcloud run services describe synthetic-meat-ingestor \
              --region australia-southeast2 --format 'value(status.address.url)')
curl -H "Authorization: Bearer ${token}" ${url}
