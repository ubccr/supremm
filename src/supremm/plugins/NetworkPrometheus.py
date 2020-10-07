#!/usr/bin/env python
"""https://github.com/prometheus/node_exporter"""

from supremm.plugin import PrometheusPlugin

class NetworkPrometheus(PrometheusPlugin):
    """ This plugin processes lots of metric that are all interested in the difference over the process """

    name = property(lambda x: "network")
    metric_system = property(lambda x: "prometheus")
    requiredMetrics = property(lambda x: {
        "in-bytes": {
            'metric': 'rate(node_network_receive_bytes_total{{instance=~"^{node}.+",device!="lo"}}[{rate}])',
            'indom': '{device}',
        },
        "out-bytes": {
            'metric': 'rate(node_network_transmit_bytes_total{{instance=~"^{node}.+",device!="lo"}}[{rate}])',
            'indom': '{device}',
        },
        "in-packets": {
            'metric': 'rate(node_network_receive_packets_total{{instance=~"^{node}.+",device!="lo"}}[{rate}])',
            'indom': '{device}',
        },
        "out-packets": {
            'metric': 'rate(node_network_transmit_packets_total{{instance=~"^{node}.+",device!="lo"}}[{rate}])',
            'indom': '{device}',
        }
    })
    optionalMetrics = property(lambda x: {})
    derivedMetrics = property(lambda x: {})
