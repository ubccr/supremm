#!/usr/bin/env python3
""" Proc information pre-processor """

import re

from supremm.preprocessors.Proc import Proc
from supremm.linuxhelpers import parsecpusallowed


class ProcPrometheus(Proc):
    """ Parse and analyse the proc information for a job. Supports parsing the cgroup information
        from SLRUM and PBS/Torque (if available).
    """

    requiredMetrics = property(lambda x: ["prom:cgroup_cpu_info",
                                          "prom:cgroup_process_exec_count"])

    optionalMetrics = property(lambda x: [])
    derivedMetrics = property(lambda x: [])

    def __init__(self, job):
        super(ProcPrometheus, self).__init__(job)

    def process(self, timestamp, data, description):
        """ Override Proc process() method """
        # Set self.cgroupcpuset here using parsecpusallowed
        # The cgroupcpuset is returned as part of the description query
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

        # All processes from the exporter are constrained
        for procname in description[1].values():
            self.output['procDump']['constrained'][procname] += 1

        return True
