#!/usr/bin/env python3
""" Proc information pre-processor """

import re
import itertools
from collections import Counter

from supremm.plugin import PreProcessor
from supremm.errors import ProcessingError
from supremm.linuxhelpers import parsecpusallowed

SLURM_CGROUP_RE = re.compile(r"cpuset:/slurm/uid_(\d+)/job_(\d+)/")
TORQUE_CGROUP_RE = re.compile(r"cpuset:/torque/(\d+(?:\[\d+\])?)(?:\.[^\.].*)?")


class ProcPrometheus(PreProcessor):
    """ Parse and analyse the proc information for a job. Supports parsing the cgroup information
        from SLRUM and PBS/Torque (if available).
    """

    name = property(lambda x: "procprom")
    mode = property(lambda x: "timeseries")
    requiredMetrics = property(lambda x: ["prom:cgroup_cpu_info",
                                          "prom:cgroup_process_exec_count"])

    optionalMetrics = property(lambda x: [])
    derivedMetrics = property(lambda x: [])

    def __init__(self, job):
        super(ProcPrometheus, self).__init__(job)

        self.cgrouppath = None
        self.expectedcgroup = None
        self.cgroupparser = None

        if job.acct['resource_manager'] == 'slurm':
            self.cgrouppath = "/slurm/uid_" + str(job.acct['uid']) + "/job_" + job.job_id
            self.expectedcgroup = "cpuset:" + self.cgrouppath
            self.cgroupparser = self.slurmcgroupparser
        elif job.acct['resource_manager'] == 'pbs':
            self.cgrouppath = "/torque/" + job.job_id
            self.expectedcgroup = "cpuset:" + self.cgrouppath
            self.cgroupparser = self.torquecgroupparser

        self.jobusername = job.acct['user']

        self.cpusallowed = None
        self.cgroupcpuset = None
        self.hostname = None

        self.output = {"procDump": {"constrained": Counter(), "unconstrained": Counter()}, "cpusallowed": {}}

    @staticmethod
    def torquecgroupparser(s):
        """ Parse linux cgroup string for slurm-specific settings and extract
            the jobid of each job
        """
        m = TORQUE_CGROUP_RE.search(s)
        if m:
            return None, m.group(1)

        return None, None

    @staticmethod
    def slurmcgroupparser(s):
        """ Parse linux cgroup string for slurm-specific settings and extract
            the UID and jobid of each job
        """

        m = SLURM_CGROUP_RE.search(s)
        if m:
            return m.group(1), m.group(2)
        else:
            return None, None

    def hoststart(self, hostname):
        self.hostname = hostname
        self.output['cpusallowed'][hostname] = {"error": ProcessingError.RAW_COUNTER_UNAVAILABLE}

    def logerror(self, info):
        """ record error information """
        if 'errors' not in self.output:
            self.output['errors'] = {}
        if self.hostname not in self.output['errors']:
            self.output['errors'][self.hostname] = set()
        self.output['errors'][self.hostname].add(info)

    def process(self, timestamp, data, description):
        # Set self.cgroupcpuset here using parsecpusallowed
        # The cgroupcpuset is returned as part of the description query
        # label: "cpus" 
        if self.cpusallowed is None:
            allcores = set()
            try:
                for cpuset in description[0].values():
                    allcores |= parsecpusallowed(cpuset)
                if len(allcores) > 0:
                    self.cpusallowed = allcores
            except ValueError:
                # Empty cpuset info seen in the wild - should get populated at
                # next timestep
                pass

        # All procs from the exporter are contained (constrained)
        for procname in description[1].values():
            self.output['procDump']['constrained'][procname] += 1
        
        # No unconstrained proc data from the exporter
        #for procname in unconstrainedprocs.values():
        #    self.output['procDump']['unconstrained'][procname] += 1

        return True

    def hostend(self):

        if self.cgroupcpuset is not None:
            self.output['cpusallowed'][self.hostname] = list(self.cgroupcpuset)
        elif self.cpusallowed is not None:
            self.output['cpusallowed'][self.hostname] = list(self.cpusallowed)

        self.cgroupcpuset = None
        self.cpusallowed = None
        self.hostname = None

        self._job.adddata(self.name, self.output)

    def results(self):

        constrained = [x[0] for x in self.output['procDump']['constrained'].most_common()]

        result = {"constrained": constrained,
                  "cpusallowed": {}}

        sizelimit = 150
        if len(result["constrained"]) > sizelimit:
            result["constrained"] = result["constrained"][0:sizelimit]
            result["error"] = "process list limited to {0} procs".format(sizelimit)

        i = 0
        for nodename, cpulist in self.output['cpusallowed'].items():
            if 'error' in cpulist:
                result['cpusallowed']['node{0}'.format(i)] = {'node': nodename, 'error': cpulist['error']}
            else:
                result['cpusallowed']['node{0}'.format(i)] = {'node': nodename, 'cpu_list': ','.join(str(cpu) for cpu in cpulist)}
            i += 1

        return {'procDump': result}
