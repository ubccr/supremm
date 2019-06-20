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
SELECT COUNT(*) FROM \`batchscripts\` WHERE resource_id = 3 and local_job_id IN (1234234, 197155, 197199, 123424, 197186, 197182);
EOF
)

[[ $count -eq 6 ]]

pytest tests/integration_tests/integration_plugin_api.py
python tests/hardware_info_tests/testHardwareInfoFetching.py
