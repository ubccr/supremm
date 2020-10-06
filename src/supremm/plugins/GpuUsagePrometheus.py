#!/usr/bin/env python
""" Energy usage plugin - https://github.com/NVIDIA/gpu-monitoring-tools"""

from supremm.plugin import PrometheusPlugin

class GpuUsagePrometheus(PrometheusPlugin):
    """ Compute the power statistics for a job """

    name = property(lambda x: "gpu")
    metric_system = property(lambda x: "prometheus")
    requiredMetrics = property(lambda x: {
        'memutil': {
            'metric': 'DCGM_FI_DEV_MEM_COPY_UTIL{{instance=~"^{node}.+"}}',
            'indom': 'gpu{gpu}',
        },
        'util': {
            'metric': 'DCGM_FI_DEV_GPU_UTIL{{instance=~"^{node}.+"}}',
            'indom': 'gpu{gpu}',
        }
    })
    optionalMetrics = property(lambda x: {})
    derivedMetrics = property(lambda x: {})
