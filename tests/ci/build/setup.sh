#!/bin/bash

dnf install -y epel-release

# enable powertools repo for Cython
sed -i 's/enabled=0/enabled=1/' /etc/yum.repos.d/Rocky-PowerTools.repo

# install development dependencies
dnf install -y \
    gcc \
    python3-numpy \
    python3-scipy \
    python36-devel \
    python3-Cython \
    python3-pymongo \
    python3-PyMySQL \
    python3-pytest \
    python3-pytest-cov \
    python3-mock \
    python3-pexpect \
    python3-pylint \
    python3-pcp \
    python3-requests \
    pcp-devel
