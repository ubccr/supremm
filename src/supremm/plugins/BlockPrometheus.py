#!/usr/bin/env python
""" Block usage plugin - https://github.com/prometheus/node_exporter"""


from supremm.plugin import PrometheusPlugin

class BlockPrometheus(PrometheusPlugin):
    """ This plugin processes lots of metric that are all interested in the difference over the process """

    name = property(lambda x: "block")
    metric_system = property(lambda x: "prometheus")
    requiredMetrics = property(lambda x: {
        "read": {
            'metric': 'rate(node_disk_reads_completed_total{{instance=~"^{node}.+"}}[{rate}])',
            'indom': '{device}',
        },
        "read_bytes": {
            'metric': 'rate(node_disk_read_bytes_total{{instance=~"^{node}.+"}}[{rate}])',
            'indom': '{device}',
        },
        "write": {
            'metric': 'rate(node_disk_writes_completed_total{{instance=~"^{node}.+"}}[{rate}])',
            'indom': '{device}',
        },
        "write_bytes": {
            'metric': 'rate(node_disk_written_bytes_total{{instance=~"^{node}.+"}}[{rate}])',
            'indom': '{device}',
        }
    })
    optionalMetrics = property(lambda x: {})
    derivedMetrics = property(lambda x: {})

