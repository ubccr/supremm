#!/usr/bin/env python
"""https://github.com/treydock/gpfs_exporter"""

from supremm.plugin import PrometheusTimeseriesNamePlugin

class GpfsTimeseriesPrometheus(PrometheusTimeseriesNamePlugin):
    """ Collect GPFS metrics from Prometheus """

    name = property(lambda x: "lnet")
    metric_system = property(lambda x: "prometheus")
    requiredMetrics = property(lambda x: {
        "read_bytes": {
            'metric': 'rate(gpfs_perf_read_bytes{{instance=~"^{node}.+"}}[{rate}]) / 1024^2',
            'timeseries_name': '{fs}-read',
        },
        "write_bytes": {
            'metric': 'rate(gpfs_perf_write_bytes{{instance=~"^{node}.+"}}[{rate}]) / 1024^2',
            'timeseries_name': '{fs}-write',
        }
    })
    optionalMetrics = property(lambda x: {})
    derivedMetrics = property(lambda x: {})
