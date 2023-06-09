FROM rockylinux:8
VOLUME /tmp/supremm
COPY setup.sh /sbin/setup.sh
COPY build.sh /sbin/build.sh
RUN setup.sh
WORKDIR /tmp/supremm
entrypoint ["build.sh"]
