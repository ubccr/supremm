#!/usr/bin/env python
""" Proc information pre-processor 
https://github.com/treydock/cgroup_exporter
https://github.com/prometheus/node_exporter
"""

from supremm.plugin import PrometheusPlugin
from supremm.errors import ProcessingError

class ProcPrometheus(PrometheusPlugin):
    """ Parse and analyse the proc information for a job. Supports parsing the cgroup information
        from SLRUM and PBS/Torque (if available).
    """

    name = property(lambda x: "proc")
    metric_system = property(lambda x: "prometheus")
    mode = property(lambda x: "timeseries")
    requiredMetrics = property(lambda x: {
        'cpusallowed': {
            'metric': 'cgroup_cpu_info{{instance=~"^{node}.+"}} * on(cgroup, instance) group_left(jobid) cgroup_info{{instance=~"^{node}.+",jobid="{jobid}"}}',
        },
        'processes': {
            'metric': 'cgroup_process_exec_count{{instance=~"^{node}.+"}} * on(cgroup, instance) group_left(jobid) cgroup_info{{instance=~"^{node}.+",jobid="{jobid}"}}',
        },
        'hostcpus': {
            'metric': 'count by(instance) (node_cpu_info{{instance=~"{node}.+"}})',
        }
    })

    optionalMetrics = property(lambda x: {})
    derivedMetrics = property(lambda x: {})

    def __init__(self, job, config):
        super(ProcPrometheus, self).__init__(job, config)

        self.cpusallowed = None
        self.hostcpus = None
        self.hostname = None
        self.output = {"procDump": {"constrained": [], "unconstrained": []}, "cpusallowed": {}, "hostcpus": {}}

    def hoststart(self, hostname):
        self.hostname = hostname
        self.output['cpusallowed'][hostname] = {"error": ProcessingError.RAW_COUNTER_UNAVAILABLE}
        self.output['hostcpus'][hostname] = {"error": ProcessingError.RAW_COUNTER_UNAVAILABLE}

    def hostend(self):
        if self.cpusallowed is not None:
            self.output['cpusallowed'][self.hostname] = self.cpusallowed
        if self.hostcpus is not None:
            self.output['hostcpus'][self.hostname] = self.hostcpus

        self.cpusallowed = None
        self.hostcpus = None
        self.hostname = None

        self._job.adddata(self.name, self.output)

    def process(self, mdata):
        for metricname, metric in self.allmetrics.items():
            query = metric['metric'].format(node=mdata.nodename, jobid=self._job.job_id, rate=self.rate)
            if metricname == 'hostcpus':
                data = self.query(query, mdata.start)
            else:
                data = self.query_range(query, mdata.start, mdata.end)
            if data is None:
                self._error = ProcessingError.PROMETHEUS_QUERY_ERROR
                return None
            for r in data.get('data', {}).get('result', []):
                m = r.get('metric', {})
                if metricname == 'cpusallowed':
                    value = m.get('cpus', "").split(",")
                    if value:
                        self.cpusallowed = value
                        break
                elif metricname == 'hostcpus':
                    value = r.get('value', [None, None])[1]
                    if value is not None:
                        self.hostcpus = float(value)
                        break
                elif metricname == 'processes':
                    execname = m.get('exec', None)
                    if execname is None:
                        self.output['procDump']['constrained'] = {"error": ProcessingError.RAW_COUNTER_UNAVAILABLE}
                        continue
                    if execname not in self.output['procDump']['constrained']:
                        self.output['procDump']['constrained'].append(execname)

        return True

    def results(self):
        if self._error != None:
            return {"error": self._error}

        result = {
            "constrained": [],
            "unconstrained": [],
            "cpusallowed": {},
            "hostcpus": {},
        }

        for hostname, value in self.output['cpusallowed'].items():
            result['cpusallowed'][hostname] = ','.join(value)
        for hostname, value in self.output['hostcpus'].items():
            result['hostcpus'][hostname] = value
        result['constrained'] = self.output['procDump']['constrained']
            
        return {'procDump': result}
