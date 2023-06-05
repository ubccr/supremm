FROM prom/prometheus:latest
RUN touch /prometheus/queries.active
COPY ./prometheus.yml /etc/prometheus/prometheus.yml
ADD ./mockdata.tar.gz /prometheus
RUN promtool tsdb create-blocks-from openmetrics /prometheus/mockdata.txt /prometheus
