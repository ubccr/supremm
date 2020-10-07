#!/usr/bin/env python
"""https://github.com/prometheus/node_exporter"""

from supremm.plugin import PrometheusPlugin

class NfsPrometheus(PrometheusPlugin):
    """ Generate usage statistics for NFS clients """

    name = property(lambda x: "nfs")
    metric_system = property(lambda x: "prometheus")
    requiredMetrics = property(lambda x: {
        "read": {
            'metric': 'rate(node_mountstats_nfs_total_read_bytes_total{{instance=~"^{node}.+"}}[{rate}])',
            'indom': '{export}',
        },
        "write": {
            'metric': 'rate(node_mountstats_nfs_total_write_bytes_total{{instance=~"^{node}.+"}}[{rate}])',
            'indom': '{export}',
        },
    })
    optionalMetrics = property(lambda x: {})
    derivedMetrics = property(lambda x: {})
