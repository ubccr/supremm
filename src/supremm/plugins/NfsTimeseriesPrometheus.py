#!/usr/bin/env python
""" Timeseries generator module """

from supremm.plugin import PrometheusTimeseriesNamePlugin

class NfsTimeseriesPrometheus(PrometheusTimeseriesNamePlugin):
    """ Generate timeseries summary for NFS usage data """

    name = property(lambda x: "nfs")
    metric_system = property(lambda x: "prometheus")
    requiredMetrics = property(lambda x: {
        "read": {
            'metric': 'sum(rate(node_mountstats_nfs_total_read_bytes_total{{instance=~"^{node}.+"}}[{rate}])) / 1024^2',
            'timeseries_name': 'read',
        },
        "write": {
            'metric': 'sum(rate(node_mountstats_nfs_total_write_bytes_total{{instance=~"^{node}.+"}}[{rate}])) / 1024^2',
            'timeseries_name': 'write',
        },
    })
    optionalMetrics = property(lambda x: {})
    derivedMetrics = property(lambda x: {})
