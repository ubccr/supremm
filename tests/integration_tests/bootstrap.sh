#!/usr/bin/env bash
set -euxo pipefail
shopt -s extglob

python setup.py bdist_rpm
yum install -y dist/supremm-+([0-9.])*.x86_64.rpm
~/bin/services start
rm -rf /var/lib/mongodb/*
mongod -f /etc/mongod.conf
~/bin/importmongo.sh

mkdir -p /data/{phillips,pozidriv,frearson,mortorq,robertson}/{pcp-logs,jobscripts}
mkdir -p "/data/mortorq/pcp-logs/hostname/2016/12/30"
mkdir -p "/data/pcp-logs/hardware-info-test/hostname/2019/06/17"

python tests/integration_tests/supremm_setup_expect.py

cp tests/integration_tests/pcp_logs_extracted/* /data/mortorq/pcp-logs/hostname/2016/12/30
cp tests/hardware_info_tests/pcp_logs/* /data/pcp-logs/hardware-info-test/hostname/2019/06/17

# Create files containing 'job scripts'
jspath=/data/phillips/jobscripts/20170101
mkdir $jspath
for jobid in 197155 197182 197186 197199 1234234[21] 123424[]
do
    echo "Job script for job $jobid" > $jspath/$jobid.savescript
done
