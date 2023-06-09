#!/usr/bin/env bash
set -euxo pipefail
shopt -s extglob

INSTALL_TYPE=$1
case $INSTALL_TYPE in
  "rpm")
    dnf install -y /tmp/dist/supremm-+([0-9.])*.x86_64.rpm
    ;;
  "wheel")
    pip3 install -y /tmp/dist/supremm-+([0-9.])*.whl
    ;;
  "src")
    dnf install -y \
        python36 \
        python3-pymongo \
        python3-numpy \
        python3-scipy \
        python3-PyMySQL \
        python3-pcp \
        python3-pcp \
        pcp-libs \
        python3-Cython \
        python3-pytz \
        python3-requests
    python3 setup.py install
    ;;
esac

# Prepare archive and jobscript directories
mkdir -p /data/pcp_cluster/{pcp-logs,jobscripts}
mkdir -p "/data/pcp_cluster/pcp-logs/hostname/2016/12/30"
mkdir -p "/data/prom_cluster/jobscripts"

# Run setup script
dnf install -y python3-pexpect

export TERMINFO=/bin/bash
export TERM=linux
/usr/bin/supremm_setup_expect.py

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
