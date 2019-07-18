FROM       tas-tools-ext-01.ccr.xdmod.org/xdmod-centos7:open7.5.1-supremm7.5.1-v1
MAINTAINER Joseph P. White <jpwhite4@buffalo.edu>

RUN wget https://bintray.com/pcp/el7/rpm -O /etc/yum.repos.d/bintray-pcp-el7.repo

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
    python-pcp-3.12.2 \
    pcp-devel-3.12.2

RUN pip install pylint==1.8.3 coverage pytest==4.6.3 pytest-cov==2.7.1 setuptools==36.4.0 pexpect==4.4.0

RUN pip install --ignore-installed six>=1.10.0

ADD . /root/supremm

WORKDIR /root/supremm

