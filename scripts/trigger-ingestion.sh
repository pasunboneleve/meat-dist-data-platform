#!/bin/env bash

set -euox --pipefail

#
# This scripts triggers the synthetic meat ingestor Cloud Run service
#


curl -H "Authorization: Bearer $(gcloud auth print-identity-token)" \
     $(gcloud run services describe synthetic-meat-ingestor \
              --region australia-southeast2 --format 'value(status.address.url)')
