FROM       tas-tools-ext-01.ccr.xdmod.org/centos8-xdmod-bootstrap:latest
MAINTAINER Joseph P. White <jpwhite4@buffalo.edu>

RUN yum install -y \
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
    pcp-devel

ADD . /root/supremm

WORKDIR /root/supremm

