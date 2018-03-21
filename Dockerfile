FROM       centos:centos7
MAINTAINER Joseph P. White <jpwhite4@buffalo.edu>

RUN yum -y install epel-release && yum -y update
RUN yum -y install wget && wget https://centos7.iuscommunity.org/ius-release.rpm && rpm -i ius-release.rpm 

RUN yum install -y \
    gcc \
    rsync \
    vim \
    sudo \
    git2u \
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
    python-setuptools \
    Cython \
    jq \
    pcp-devel

RUN pip install pylint coverage pytest

ADD . /root

WORKDIR /root

