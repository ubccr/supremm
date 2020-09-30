#!/usr/bin/env python
""" Block usage timerseries plugin - https://github.com/prometheus/node_exporter"""

from supremm.plugin import PrometheusTimeseriesPlugin

class BlockTimeseriesPrometheus(PrometheusTimeseriesPlugin):
    """ This plugin processes lots of metric that are all interested in the difference over the process """

    name = property(lambda x: "block")
    metric_system = property(lambda x: "prometheus")
    timeseries = property(lambda x: True)
    requiredMetrics = property(lambda x: {
        "read_bytes": {
            'metric': 'rate(node_disk_read_bytes_total{{instance=~"^{node}.+"}}[{rate}])',
            'indom': 'device',
        },
        "write_bytes": {
            'metric': 'rate(node_disk_written_bytes_total{{instance=~"^{node}.+"}}[{rate}])',
            'indom': 'device',
        }
    })
    optionalMetrics = property(lambda x: {})
    derivedMetrics = property(lambda x: {})

