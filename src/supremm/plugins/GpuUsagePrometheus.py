#!/usr/bin/env python
""" Energy usage plugin - https://github.com/NVIDIA/gpu-monitoring-tools"""

from supremm.plugin import PrometheusPlugin

class GpuUsagePrometheus(PrometheusPlugin):
    """ Compute the power statistics for a job """

    name = property(lambda x: "gpu")
    metric_system = property(lambda x: "prometheus")
    requiredMetrics = property(lambda x: {
        'memused': {
            'metric': 'DCGM_FI_DEV_FB_USED{{instance=~"^{node}.+"}} * 1024^2',
            'indom': 'gpu{gpu}',
        },
        'util': {
            'metric': 'DCGM_FI_DEV_GPU_UTIL{{instance=~"^{node}.+"}}',
            'indom': 'gpu{gpu}',
        }
    })
    optionalMetrics = property(lambda x: {})
    derivedMetrics = property(lambda x: {})
