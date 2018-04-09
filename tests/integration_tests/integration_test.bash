#!/usr/bin/env bash

set -euxo pipefail

indexarchives.py -da
summarize_jobs.py -d -r 2 -j 972366
aggregate_supremm.sh

count=$(mysql -ss -u root <<EOF
USE modw_supremm;
SELECT count(*) FROM job WHERE local_job_id=972366 AND resource_id=2;
EOF
)

[[ $count -eq 1 ]]
