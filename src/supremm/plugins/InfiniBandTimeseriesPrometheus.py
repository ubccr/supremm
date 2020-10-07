#!/usr/bin/env python
"""https://github.com/prometheus/node_exporter"""

from supremm.plugin import PrometheusTimeseriesNamePlugin

class InfiniBandTimeseriesPrometheus(PrometheusTimeseriesNamePlugin):
    """ This plugin processes lots of metric that are all interested in the difference over the process """

    name = property(lambda x: "infiniband")
    metric_system = property(lambda x: "prometheus")
    requiredMetrics = property(lambda x: {
        "switch-in-bytes": {
            'metric': 'sum(rate(node_infiniband_port_data_received_bytes_total{{instance=~"^{node}.+"}}[{rate}]))',
            'timeseries_name': 'receive'
        },
        "switch-out-bytes": {
            'metric': 'sum(rate(node_infiniband_port_data_transmitted_bytes_total{{instance=~"^{node}.+"}}[{rate}]))',
            'timeseries_name': 'transmit'
        },
    })
    optionalMetrics = property(lambda x: {})
    derivedMetrics = property(lambda x: {})
