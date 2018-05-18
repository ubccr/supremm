#!/usr/bin/env bash
set -euxo pipefail
shopt -s extglob

python setup.py bdist_rpm
yum install -y dist/supremm-+([0-9.])*.x86_64.rpm
~/bin/services start
rm -rf /var/lib/mongodb/*
mongod -f /etc/mongod.conf
~/bin/importmongo.sh

mkdir -p /data/{phillips,pozidriv,frearson,mortorq,robertson}/pcp-logs
mkdir -p "/data/mortorq/pcp-logs/hostname/2016/12/30"

python tests/integration_tests/supremm_setup_expect.py

cp tests/integration_tests/pcp_logs_extracted/* /data/mortorq/pcp-logs/hostname/2016/12/30

