#!/usr/bin/env python
""" Timeseries generator module - https://github.com/soundcloud/ipmi_exporter"""

from supremm.plugin import PrometheusTimeseriesPlugin

class PowerUsageTimeseriesPrometheus(PrometheusTimeseriesPlugin):
    """ Generate the Power usage as a timeseries data """

    name = property(lambda x: "power")
    metric_system = property(lambda x: "prometheus")
    requiredMetrics = property(lambda x: {
        'power': {
            'metric': 'ipmi_dcmi_power_consumption_watts{{instance=~"^{node}.+"}}',
        }
    })
    optionalMetrics = property(lambda x: {})
    derivedMetrics = property(lambda x: {})
