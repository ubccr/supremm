#!/usr/bin/env python
""" CPU Usage metrics - https://github.com/prometheus/node_exporter"""

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
        }
    })

    optionalMetrics = property(lambda x: {})
    derivedMetrics = property(lambda x: {})

    def process(self, mdata):
        self._data[mdata.nodename] = {}
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
                if mode is None or cpu is None:
                    continue
                self._data[mdata.nodename][mode] = {}
                self._data[mdata.nodename][mode][cpu] = []
                values = r.get('values', [])
                for v in values:
                    value = float(v[1])
                    self._data[mdata.nodename][mode][cpu].append(value)
        return True

    def results(self):
        error = False
        if self._error != None:
            return {"error": self._error}
        if len(self._data) != self._job.nodecount:
            return {"error": ProcessingError.INSUFFICIENT_HOSTDATA}

        cpusallowed = self._job.getdata('proc')['cpusallowed']
        hinv = self._job.getdata('hinv')

        stats = {'jobcpus': {'all': {'cnt': 0}}, 'nodecpus': {'all': {'cnt': 0}}}
        results = {'nodecpus': {}, 'jobcpus': {}}

        for host, modes in self._data.items():
            usercpus = cpusallowed[host]
            if 'error' in usercpus:
                results['jobcpus'] = usercpus
                error = True
            if 'error' in hinv[host]:
                results['nodecpus'] = hinv[host]
                error = True
            if error:
                continue
            stats['jobcpus']['all']['cnt'] += len(usercpus)
            stats['nodecpus']['all']['cnt'] += hinv[host]['cores']
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

        if error:
            return results

        for _type, modes in stats.items():
            for mode, values in modes.items():
                if mode == 'all':
                    results[_type][mode] = values
                    continue
                results[_type][mode] = calculate_stats(values)

        return results

