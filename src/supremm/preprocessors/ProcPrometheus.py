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


class Proc(PreProcessor):
    """ Parse and analyse the proc information for a job. Supports parsing the cgroup information
        from SLRUM and PBS/Torque (if available).
    """

    name = property(lambda x: "proc")
    mode = property(lambda x: "timeseries")
    requiredMetrics = property(lambda x: [])#TODO how to handle with reqMetrics? 

    optionalMetrics = property(lambda x: [])
    derivedMetrics = property(lambda x: [])

    def __init__(self, job):
        super(Proc, self).__init__(job)

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

        if len(data[0]) != len(data[1]) or len(data[0]) != len(data[2]):
            # There is a known race condition in the proc pmda that means that
            # processes are not always recorded in the Indom
            return True

        #currentpids = {}
        #cgroupedprocs = []

        containedprocs = {}





        for pid, idx in currentpids.items():
            if pid not in description[1]:
                self.logerror("missing process name")
                continue

            s = str(description[1][pid], errors='replace')
            command = s[s.find(" ") + 1:]

            if self.cgroupparser is not None:
                if self.expectedcgroup in data[2][idx][0]:
                    containedprocs[pid] = command
                    cgroupedprocs.append(idx)
                else:
                    _, otherjobid = self.cgroupparser(data[2][idx][0])
                    if otherjobid is not None:
                        otherjobs[pid] = command
                    else:
                        unconstrainedprocs[pid] = command
            else:
                unconstrainedprocs[pid] = command

        if len(data) > 3 and self.cgrouppath is not None and self.cgroupcpuset is None:
            for cpuset in filter(lambda x: x[1] == self.cgrouppath, iter(description[3].items())):
                for content in filter(lambda x: int(x[1]) == cpuset[0], data[3]):
                    self.cgroupcpuset = parsecpusallowed(content[0])
                    break

        if self.cpusallowed is None:
            allcores = set()
            try:
                for idx in cgroupedprocs:
                    allcores |= parsecpusallowed(data[0][idx][0])
                if len(allcores) > 0:
                    self.cpusallowed = allcores
            except ValueError:
                # Empty cpuset info seen in the wild - should get populated at
                # next timestep
                pass

        for procname in containedprocs.values():
            self.output['procDump']['constrained'][procname] += 1

        for procname in unconstrainedprocs.values():
            self.output['procDump']['unconstrained'][procname] += 1

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
        unconstrained = [x[0] for x in self.output['procDump']['unconstrained'].most_common()]

        result = {"constrained": constrained,
                  "unconstrained": unconstrained,
                  "cpusallowed": {}}

        sizelimit = 150
        if len(result["constrained"]) > sizelimit:
            result["constrained"] = result["constrained"][0:sizelimit]
            result["error"] = "process list limited to {0} procs".format(sizelimit)
        if len(result["unconstrained"]) > sizelimit:
            result["unconstrained"] = result["unconstrained"][0:sizelimit]
            result["error"] = "process list limited to {0} procs".format(sizelimit)

        i = 0
        for nodename, cpulist in self.output['cpusallowed'].items():
            if 'error' in cpulist:
                result['cpusallowed']['node{0}'.format(i)] = {'node': nodename, 'error': cpulist['error']}
            else:
                result['cpusallowed']['node{0}'.format(i)] = {'node': nodename, 'cpu_list': ','.join(str(cpu) for cpu in cpulist)}
            i += 1

        return {'procDump': result}
