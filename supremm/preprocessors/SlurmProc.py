#!/usr/bin/env python
""" Proc information pre-processor """

from supremm.plugin import PreProcessor
from supremm.errors import ProcessingError
from supremm.linuxhelpers import parsecpusallowed
import re
import itertools

class SlurmProc(PreProcessor):
    """ Parse and analyse the proc information for a job that ran under slurm
        where the slurm cgroups plugin was enabled. 
    """

    name = property(lambda x: "proc")
    mode = property(lambda x: "timeseries")
    requiredMetrics = property(lambda x: [
        [ "proc.psinfo.cpusallowed",
        "proc.id.uid_nm",
        "proc.psinfo.cgroups" ],
        [ "hotproc.psinfo.cpusallowed",
        "hotproc.id.uid_nm",
        "hotproc.psinfo.cgroups" ] 
        ])

    optionalMetrics = property(lambda x: ["cgroup.cpuset.cpus"])
    derivedMetrics = property(lambda x: [])

    def __init__(self, job):
        super(SlurmProc, self).__init__(job)

        self.expectedslurmscript = "/var/spool/slurmd/job" + job.job_id + "/slurm_script"
        self.cgrouppath = "/slurm/uid_" + str(job.acct['uid']) + "/job_" + job.job_id
        self.expectedcgroup = "cpuset:" + self.cgrouppath
        self.jobusername = job.acct['user']

        self.cpusallowed = None
        self.cgroupcpuset = None
        self.hostname = None

        self.output = {"procDump": {"constrained": set(), "unconstrained": set()}, "cpusallowed": {}}

    @staticmethod
    def slurmcgroupparser(s):
        """ Parse linux cgroup string for slurm-specific settings and extract
            the UID and jobid of each job
        """

        groups = s.split(";")
        groupre = re.compile(r"^cpuset:/slurm/uid_(\d+)/job_(\d+)/")

        for group in groups:
            m = groupre.match(group)
            if m:
                return m.group(1), m.group(2)

        return None, None


    @staticmethod
    def instanceparser(s):
        tokens = s.split(" ")

        pid = tokens[0]
        cmd = tokens[1:]

        return pid,cmd

    def hoststart(self, hostname):
        self.hostname = hostname
        self.output['cpusallowed'][hostname] = {"error": ProcessingError.RAW_COUNTER_UNAVAILABLE}

    def logerror(self, info):
        if 'errors' not in self.output:
            self.output['errors'] = {}
        if self.hostname not in self.output['errors']:
            self.output['errors'][self.hostname] = []
        self.output['errors'][self.hostname].append(info)

    def process(self, timestamp, data, description):

        if len(data[0]) != len(data[1]) or len(data[0]) != len(data[2]):
            # There is a known race condition in the proc pmda that means that
            # processes are not always recorded in the Indom
            return True

        currentpids = {}
        cgroupedprocs = []

        containedprocs = {}
        otherjobs = {}
        unconstrainedprocs = {}

        # Find all procs running under job user account
        for idx, unamepid in enumerate(data[1]):
            if unamepid[0] == self.jobusername:
                pid = int(unamepid[1])
                currentpids[pid] = idx

        for pid, idx in currentpids.iteritems():
            if pid not in description[1]:
                self.logerror("missing process name for pid {0}".format(pid))
                continue

            command = " ".join(description[1][pid].split(" ")[1:])

            if data[2][idx][0].find(self.expectedcgroup) != -1:
                containedprocs[pid] = command
                cgroupedprocs.append(idx)
            else:
                otheruid, otherjobid = self.slurmcgroupparser(data[2][idx][0])
                if otherjobid != None:
                    otherjobs[pid] = command
                else:
                    unconstrainedprocs[pid] = command

        if len(data) > 3 and self.cgroupcpuset == None:
            for cpuset in itertools.ifilter(lambda x: x[1] == self.cgrouppath, description[3].iteritems()):
                for content in itertools.ifilter(lambda x: int(x[1]) == cpuset[0], data[3]):
                    self.cgroupcpuset = parsecpusallowed(content[0])
                    break

        if self.cpusallowed == None:
            allcores = set()
            for idx in cgroupedprocs:
                allcores |= parsecpusallowed(data[0][idx][0])
            if len(allcores) > 0:
                self.cpusallowed = allcores

        self.output['procDump']['constrained'] |= set(containedprocs.values())
        self.output['procDump']['unconstrained'] |= set(unconstrainedprocs.values())

        return True

    def hostend(self):

        if self.cgroupcpuset != None:
            self.output['cpusallowed'][self.hostname] = list(self.cgroupcpuset)
        elif self.cpusallowed != None:
            self.output['cpusallowed'][self.hostname] = list(self.cpusallowed)

        self.cgroupcpuset = None
        self.cpusallowed = None
        self.hostname = None

        self._job.adddata(self.name, self.output)

    def results(self):

        result = {"constrained": list(self.output['procDump']['constrained']),
                  "unconstrained": list(self.output['procDump']['unconstrained']),
                  "cpusallowed": {}}

        sizelimit = 150
        if len(result["constrained"]) > sizelimit:
            result["constrained"] = result["constrained"][0:sizelimit]
            result["error"] = "process list limited to {0} procs".format(sizelimit)
        if len(result["unconstrained"]) > sizelimit:
            result["unconstrained"] = result["unconstrained"][0:sizelimit]
            result["error"] = "process list limited to {0} procs".format(sizelimit)

        i = 0
        for nodename, cpulist in self.output['cpusallowed'].iteritems():
            if 'error' in cpulist:
                result['cpusallowed']['node{0}'.format(i)] = {'node': nodename, 'error': cpulist['error']}
            else:
                result['cpusallowed']['node{0}'.format(i)] = {'node': nodename, 'cpu_list': ','.join(str(cpu) for cpu in cpulist)}
            i += 1

        return {'procDump': result}

