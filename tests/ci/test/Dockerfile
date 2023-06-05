FROM rockylinux:8
VOLUME /tmp/supremm
WORKDIR /tmp/supremm
RUN dnf install -y epel-release && \
    sed -i 's/enabled=0/enabled=1/' /etc/yum.repos.d/Rocky-PowerTools.repo
COPY bootstrap.sh /usr/bin/
COPY supremm_setup_expect.py /usr/bin/
COPY entrypoint.sh /usr/local/bin/
RUN chmod u+x /usr/bin/supremm_setup_expect.py
RUN chmod u+x /usr/local/bin/entrypoint.sh
ENTRYPOINT ["entrypoint.sh"]
