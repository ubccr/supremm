#!/bin/bash

dnf install -y epel-release

# enable powertools repo for Cython
dnf config-manager --set-enabled powertools

SETUP=$1
case $SETUP in
  "build")
    dnf install -y \
        gcc \
        pcp-devel \
        rpm-build

    # Install development dependencies
    dnf install -y \
        python3-numpy \
        python3-scipy \
        python36-devel \
        python3-Cython \
        python3-pymongo \
        python3-PyMySQL \
        python3-pcp \
        python3-requests \
        python3-wheel
   ;;
  "test")
    # Install dependencies
    dnf install -y \
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
        python3-pytz \
        python3-requests \
        pcp-devel \
    ;;
esac

