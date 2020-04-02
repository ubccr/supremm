#!/usr/bin/env bash

set -euxo pipefail

indexarchives.py -da
summarize_jobs.py -d -r 2 -j 972366 --fail-fast
aggregate_supremm.sh

count=$(mysql -ss -u root <<EOF
USE modw_supremm;
SELECT count(*) FROM job WHERE local_job_id=972366 AND resource_id=2 AND netdrv_gpfs_rx IS NOT NULL;
EOF
)

[[ $count -eq 1 ]]

ingest_jobscripts.py

count=$(mysql -ss -u root modw_supremm <<EOF
SELECT COUNT(*) FROM \`job_scripts\` js , \`modw\`.\`job_tasks\` jt WHERE js.tg_job_id = jt.job_id and jt.resource_id = 3 and jt.local_jobid IN (197155, 197199, 197186, 197182);
EOF
)

[[ $count -eq 4 ]]

count=$(mysql -ss -u root modw_supremm <<EOF
SELECT COUNT(*) FROM \`job_scripts\`;
EOF
)

[[ $count -eq 6 ]]

pytest tests/integration_tests/integration_plugin_api.py
