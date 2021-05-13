#!/usr/bin/env python
""" Proc information pre-processor 
https://github.com/treydock/cgroup_exporter
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
        'gpusallowed': {
            'metric': '{job_gpu_info}{{instance=~"^{node}.+",jobid="{jobid}"}}',
        },
        'processes': {
            'metric': 'cgroup_process_exec_count{{instance=~"^{node}.+"}} * on(cgroup, instance) group_left(jobid) cgroup_info{{instance=~"^{node}.+",jobid="{jobid}"}}',
        },
    })

    optionalMetrics = property(lambda x: {})
    derivedMetrics = property(lambda x: {})

    def __init__(self, job, config):
        super(ProcPrometheus, self).__init__(job, config)

        self.cpusallowed = None
        self.gpusallowed = None
        self.hostname = None
        self.output = {"procDump": {"constrained": [], "unconstrained": []}, "cpusallowed": {}, "gpusallowed": {}}

    def hoststart(self, hostname):
        self.hostname = hostname
        self.output['cpusallowed'][hostname] = {"error": ProcessingError.RAW_COUNTER_UNAVAILABLE}
        self.output['gpusallowed'][hostname] = {"error": ProcessingError.RAW_COUNTER_UNAVAILABLE}

    def hostend(self):
        if self.cpusallowed is not None:
            self.output['cpusallowed'][self.hostname] = self.cpusallowed
        if self.gpusallowed is not None:
            self.output['gpusallowed'][self.hostname] = self.gpusallowed

        self.cpusallowed = None
        self.gpusallowed = None
        self.hostname = None

        self._job.adddata(self.name, self.output)

    def process(self, mdata):
        gpusallowed = []
        for metricname, metric in self.allmetrics.items():
            query = metric['metric'].format(node=mdata.nodename, jobid=self._job.job_id, rate=self.rate, job_gpu_info=self.job_gpu_info)
            data = self.query_range(query, mdata.start, mdata.end)
            if data is None:
                continue
            for r in data.get('data', {}).get('result', []):
                m = r.get('metric', {})
                if metricname == 'cpusallowed':
                    value = m.get('cpus', "").split(",")
                    if value:
                        self.cpusallowed = value
                        break
                elif metricname == 'gpusallowed':
                    value = m.get('gpu', "")
                    if value:
                        gpusallowed.append(value)
                elif metricname == 'processes':
                    execname = m.get('exec', None)
                    if execname is None:
                        continue
                    if execname not in self.output['procDump']['constrained']:
                        self.output['procDump']['constrained'].append(execname)
        self.gpusallowed = gpusallowed
        return True

    def results(self):
        result = {
            "constrained": [],
            "unconstrained": [],
            "cpusallowed": {},
            "gpusallowed": {},
        }

        for hostname, value in self.output['cpusallowed'].items():
            if 'error' in value:
                result['cpusallowed'][hostname] = value
            else:
                result['cpusallowed'][hostname] = ','.join(value)
        for hostname, value in self.output['gpusallowed'].items():
            if 'error' in value:
                result['gpusallowed'][hostname] = value
            else:
                result['gpusallowed'][hostname] = ','.join(value)
        result['constrained'] = self.output['procDump']['constrained']
            
        return {'procDump': result}
