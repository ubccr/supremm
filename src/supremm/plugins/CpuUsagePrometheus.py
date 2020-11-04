#!/usr/bin/env python
""" CPU Usage metrics

https://github.com/prometheus/node_exporter
https://github.com/treydock/cgroup_exporter
"""

from supremm.plugin import PrometheusPlugin
from supremm.statistics import calculate_stats
from supremm.errors import ProcessingError

class CpuUsagePrometheus(PrometheusPlugin):
    """ Compute the overall cpu usage for a job """

    name = property(lambda x: "cpu")
    metric_system = property(lambda x: "prometheus")
    requiredMetrics = property(lambda x: {
        'cpu': {
            'metric': 'rate(node_cpu_seconds_total{{instance=~"^{node}.+"}}[{rate}])'
        },
        'user': {
            'metric': '(rate(cgroup_cpu_user_seconds{{instance=~"^{node}.+"}}[{rate}]) / cgroup_cpus{{instance=~"^{node}.+"}}) * on(cgroup, instance) group_left(jobid) cgroup_info{{instance=~"^{node}.+",jobid="{jobid}"}}'
        },
        'system': {
            'metric': '(rate(cgroup_cpu_system_seconds{{instance=~"^{node}.+"}}[{rate}]) / cgroup_cpus{{instance=~"^{node}.+"}}) * on(cgroup, instance) group_left(jobid) cgroup_info{{instance=~"^{node}.+",jobid="{jobid}"}}'
        },
        'idle': {
            'metric': '1.0 - (rate(cgroup_cpu_total_seconds{{instance=~"^{node}.+"}}[{rate}]) / cgroup_cpus{{instance=~"^{node}.+"}}) * on(cgroup, instance) group_left(jobid) cgroup_info{{instance=~"^{node}.+",jobid="{jobid}"}}'
        }
    })

    optionalMetrics = property(lambda x: {})
    derivedMetrics = property(lambda x: {})

    def __init__(self, job, config):
        super(CpuUsagePrometheus, self).__init__(job, config)
        self._cgroupdata = {}

    def process(self, mdata):
        self._data[mdata.nodename] = {}
        self._cgroupdata[mdata.nodename] = {}
        for metricname, metric in self.allmetrics.items():
            query = metric['metric'].format(node=mdata.nodename, jobid=self._job.job_id, rate=self.rate)
            data = self.query_range(query, mdata.start, mdata.end)
            if data is None:
                self._error = ProcessingError.PROMETHEUS_QUERY_ERROR
                return None
            for r in data.get('data', {}).get('result', []):
                m = r.get('metric', {})
                mode = m.get('mode', None)
                cpu = m.get('cpu', None)
                if metricname != 'cpu':
                    mode = metricname
                    cpu = 'all'
                if mode is None or cpu is None:
                    continue
                if metricname == 'cpu':
                    self._data[mdata.nodename][mode] = {}
                    self._data[mdata.nodename][mode][cpu] = []
                else:
                    self._cgroupdata[mdata.nodename][mode] = []
                values = r.get('values', [])
                for v in values:
                    ts = int(v[0])
                    if ts <= (mdata.start + self.start_trim) or ts >= (mdata.end - self.end_trim):
                        continue
                    value = float(v[1])
                    if metricname == 'cpu':
                        self._data[mdata.nodename][mode][cpu].append(value)
                    else:
                        self._cgroupdata[mdata.nodename][mode].append(value)
        return True

    def results(self):
        if self._error != None:
            return {"error": self._error}
        if len(self._data) != self._job.nodecount:
            return {"error": ProcessingError.INSUFFICIENT_HOSTDATA}
        if len(self._cgroupdata) != self._job.nodecount:
            return {"error": ProcessingError.INSUFFICIENT_HOSTDATA}

        cpusallowed = self._job.getdata('proc')['cpusallowed']
        hinv = self._job.getdata('hinv')

        stats = {'jobcpus': {}, 'nodecpus': {}, 'cgroup': {}}
        results = {'nodecpus': {}, 'jobcpus': {}, 'cgroup': {}}

        for host, modes in self._data.items():
            usercpus = cpusallowed[host]
            if 'error' in usercpus:
                results['jobcpus'] = usercpus
            else:
                results['jobcpus']['all'] = {'cnt': 0}
                results['jobcpus']['all']['cnt'] += len(usercpus)
            if 'error' in hinv[host]:
                results['nodecpus'] = hinv[host]
            else:
                results['nodecpus']['all'] = {'cnt': 0}
                results['nodecpus']['all']['cnt'] += hinv[host]['cores']
            for mode, cpus in modes.items():
                if mode not in stats['jobcpus']:
                    stats['jobcpus'][mode] = []
                if mode not in stats['nodecpus']:
                    stats['nodecpus'][mode] = []
                for cpu, values in cpus.items():
                    stats['nodecpus'][mode] = stats['nodecpus'][mode] + values
                    if cpu not in usercpus:
                        continue
                    stats['jobcpus'][mode] = stats['jobcpus'][mode] + values

        for host, modes in self._cgroupdata.items():
            for mode, values in modes.items():
                if mode not in stats['cgroup']:
                    stats['cgroup'][mode] = []
                stats['cgroup'][mode] = stats['cgroup'][mode] + values

        if 'error' in results['nodecpus'] or 'error' in results['jobcpus']:
            return results

        for _type, modes in stats.items():
            for mode, values in modes.items():
                results[_type][mode] = calculate_stats(values)

        return results

