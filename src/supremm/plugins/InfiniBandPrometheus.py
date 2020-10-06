#!/usr/bin/env python
"""https://github.com/prometheus/node_exporter"""

from supremm.plugin import PrometheusPlugin

class InfiniBandPrometheus(PrometheusPlugin):
    """ This plugin processes lots of metric that are all interested in the difference over the process """

    name = property(lambda x: "infiniband")
    metric_system = property(lambda x: "prometheus")
    requiredMetrics = property(lambda x: {
        "switch-in-bytes": {
            'metric': 'rate(node_infiniband_port_data_received_bytes_total{{instance=~"^{node}.+"}}[{rate}])',
            'indom': '{device}:{port}',
        },
        "switch-out-bytes": {
            'metric': 'rate(node_infiniband_port_data_transmitted_bytes_total{{instance=~"^{node}.+"}}[{rate}])',
            'indom': '{device}:{port}',
        },
        "switch-in-packets": {
            'metric': 'rate(node_infiniband_port_packets_received_total{{instance=~"^{node}.+"}}[{rate}])',
            'indom': '{device}:{port}',
        },
        "switch-out-packets": {
            'metric': 'rate(node_infiniband_port_packets_transmitted_total{{instance=~"^{node}.+"}}[{rate}])',
            'indom': '{device}:{port}',
        }
    })
    optionalMetrics = property(lambda x: {})
    derivedMetrics = property(lambda x: {})
