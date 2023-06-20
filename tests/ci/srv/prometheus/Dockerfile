FROM prom/prometheus:latest
RUN touch /prometheus/queries.active
COPY ./prometheus.yml /etc/prometheus/prometheus.yml
ADD ./promdata.tar.gz /prometheus
