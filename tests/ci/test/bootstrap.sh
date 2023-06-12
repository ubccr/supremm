#!/usr/bin/env bash
set -euxo pipefail
shopt -s extglob

tests/ci/setup.sh test

INSTALL_TYPE=$1
case $INSTALL_TYPE in
  "rpm")
    dnf install -y dist/supremm-+([0-9.])*.x86_64.rpm
    ;;
  "wheel")
    pip3 install -y dist/supremm-+([0-9.])*.whl
    ;;
  "src")
    python3 setup.py install
    ;;
esac

# Prepare archive and jobscript directories
mkdir -p /data/pcp_cluster/{pcp-logs,jobscripts}
mkdir -p "/data/pcp_cluster/pcp-logs/hostname/2016/12/30"
mkdir -p "/data/prom_cluster/jobscripts"

# Run setup script
supremm_setup_expect.py

# Copy node-level archives
cp tests/integration_tests/pcp_logs_extracted/* /data/pcp_cluster/pcp-logs/hostname/2016/12/30

# Create files containing job scripts
jspath=/data/pcp_cluster/jobscripts/20161230
mkdir $jspath
for jobid in 972366
do
    echo "Job script for job $jobid" > $jspath/$jobid.savescript
done

# Create job scripts for a submit jobs
jspath=/data/prom_cluster/jobscripts/20230602
mkdir $jspath
for jobid in 123456 789012 345678 901234
do
    echo "Job script for job $jobid" > $jspath/$jobid.savescript
done
