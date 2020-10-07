#!/usr/bin/env python
""" Block usage timerseries plugin - https://github.com/prometheus/node_exporter"""

from supremm.plugin import PrometheusTimeseriesNamePlugin

class BlockTimeseriesPrometheus(PrometheusTimeseriesNamePlugin):
    """ This plugin processes lots of metric that are all interested in the difference over the process """

    name = property(lambda x: "block")
    metric_system = property(lambda x: "prometheus")
    requiredMetrics = property(lambda x: {
        "read_bytes": {
            'metric': 'rate(node_disk_read_bytes_total{{instance=~"^{node}.+"}}[{rate}])',
            'timeseries_name': 'read'
        },
        "write_bytes": {
            'metric': 'rate(node_disk_written_bytes_total{{instance=~"^{node}.+"}}[{rate}])',
            'timeseries_name': 'write',
        }
    })
    optionalMetrics = property(lambda x: {})
    derivedMetrics = property(lambda x: {})

