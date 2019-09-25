FROM       tas-tools-ext-01.ccr.xdmod.org/centos7_6-open-job_performance-8.1.0:latest
MAINTAINER Joseph P. White <jpwhite4@buffalo.edu>

RUN yum install -y \
    gcc \
    numpy \
    scipy \
    python-devel \
    python2-pip \
    python2-mock \
    python-ctypes \
    python-pymongo \
    MySQL-python \
    Cython \
    python-pcp \
    pcp-devel

RUN pip install pylint==1.8.3 coverage pytest==4.6.3 pytest-cov==2.7.1 setuptools==36.4.0 pexpect==4.4.0

RUN pip install --ignore-installed six>=1.10.0

ADD . /root/supremm

WORKDIR /root/supremm

