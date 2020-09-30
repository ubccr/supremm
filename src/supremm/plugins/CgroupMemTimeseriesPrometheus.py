#!/usr/bin/env python
""" Memory usage plugin - https://github.com/treydock/cgroup_exporter"""

from supremm.plugin import PrometheusTimeseriesPlugin

class CgroupMemTimeseriesPrometheus(PrometheusTimeseriesPlugin):
    """ Cgroup memory statistics for the job """

    name = property(lambda x: "process_mem_usage")
    metric_system = property(lambda x: "prometheus")
    requiredMetrics = property(lambda x: {
        "usage": {
            'metric': '(cgroup_memory_used_bytes{{instance=~"^{node}.+"}} * on(cgroup, instance) group_left(jobid) cgroup_info{{instance=~"^{node}.+",jobid="{jobid}"}}) / 1024^3',
        }
    })

    optionalMetrics = property(lambda x: {})
    derivedMetrics = property(lambda x: {})
