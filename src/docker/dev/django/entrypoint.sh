#!/bin/bash

set -o errexit 
set -o pipefail 
set -o nounset 

export CELERY_BROKER_URL="${CELERY_BROKER_URL}"

exec "$@"