FROM       tas-tools-ext-01.ccr.xdmod.org/xdmod-centos7:open7.5-supremm7.5-v3
MAINTAINER Joseph P. White <jpwhite4@buffalo.edu>

RUN yum install -y \
    gcc \
    rsync \
    sudo \
    numpy \
    scipy \
    python-devel \
    python2-pip \
    python2-mock \
    python-ctypes \
    python-psutil \
    python-pcp \
    python-pymongo \
    MySQL-python \
    Cython \
    jq \
    pcp-devel

RUN pip install pylint==1.8.3 coverage pytest pytest-cov psutil setuptools==36.4.0 pexpect==4.4.0

RUN pip install --ignore-installed six>=1.10.0

ADD . /root/supremm

WORKDIR /root/supremm

