#!/usr/bin/env python
"""https://github.com/treydock/gpfs_exporter"""

from supremm.plugin import PrometheusPlugin

class GpfsPrometheus(PrometheusPlugin):
    """ Collect GPFS metrics from Prometheus """

    name = property(lambda x: "gpfs")
    metric_system = property(lambda x: "prometheus")
    requiredMetrics = property(lambda x: {
        "read": {
            'metric': 'rate(gpfs_perf_operations{{instance=~"^{node}.+",operation="reads"}}[{rate}])',
            'indom': 'fs',
        },
        "read_bytes": {
            'metric': 'rate(gpfs_perf_read_bytes{{instance=~"^{node}.+"}}[{rate}])',
            'indom': 'fs',
        },
        "write": {
            'metric': 'rate(gpfs_perf_operations{{instance=~"^{node}.+",operation="writes"}}[{rate}])',
            'indom': 'fs',
        },
        "write_bytes": {
            'metric': 'rate(gpfs_perf_write_bytes{{instance=~"^{node}.+"}}[{rate}])',
            'indom': 'fs',
        }
    })
    optionalMetrics = property(lambda x: {})
    derivedMetrics = property(lambda x: {})


