#!/usr/bin/env python
""" Timeseries generator module - https://github.com/prometheus/node_exporter"""

from supremm.plugin import PrometheusTimeseriesPlugin

class MemUsageTimeseriesPrometheus(PrometheusTimeseriesPlugin):
    """ Generate the CPU usage as a timeseries data """

    name = property(lambda x: "memused_minus_diskcache")
    metric_system = property(lambda x: "prometheus")
    requiredMetrics = property(lambda x: {
        'used_minus_cache': {
            'metric': """node_memory_MemTotal_bytes{{instance=~"^{node}.+"}}
                - node_memory_MemFree_bytes{{instance=~"^{node}.+"}}
                - node_memory_Cached_bytes{{instance=~"^{node}.+"}}
                - node_memory_Slab_bytes{{instance=~"^{node}.+"}}""",
        }
    })
    optionalMetrics = property(lambda x: {})
    derivedMetrics = property(lambda x: {})
