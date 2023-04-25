#!/usr/bin/env python3
""" Timeseries generator module """

from supremm.plugin import Plugin
from supremm.subsample import TimeseriesAccumulator
from supremm.errors import ProcessingError, NotApplicableError
import numpy
from collections import Counter
import re

class CgroupMemTimeseries(Plugin):
    """ Generate timeseries summary for memory usage viewed from CGroup
        This code is SLURM-specific because of the SLURM cgroup naming convention.
    """

    name = property(lambda x: "process_mem_usage")
    mode = property(lambda x: "timeseries")
    requiredMetrics = property(lambda x: ["cgroup.memory.usage"])
    optionalMetrics = property(lambda x: [])
    derivedMetrics = property(lambda x: [])

    def __init__(self, job):
        super(CgroupMemTimeseries, self).__init__(job)
        self._data = TimeseriesAccumulator(job.nodecount, self._job.walltime)
        self._hostdata = {}
        self._hostcounts = {}
        if job.acct['resource_manager'] == 'pbs':
            self._expectedcgroup = "/torque/{0}".format(job.job_id)
        elif job.acct['resource_manager'] == 'slurm':
            self._expectedcgroup = "/slurm/uid_{0}/job_{1}".format(job.acct['uid'], job.job_id)
        else:
            raise NotApplicableError

    def process(self, nodemeta, timestamp, data, description):

        hostidx = nodemeta.nodeindex

        if len(data[0]) == 0:
            # Skip data point with no data
            return True

        if nodemeta.nodeindex not in self._hostdata:
            self._hostdata[hostidx] = numpy.empty((TimeseriesAccumulator.MAX_DATAPOINTS, 1))
            self._hostcounts[hostidx] = {'missing': 0, 'present': 0}

        try:
            dataidx = None
            for idx, desc in enumerate(description[0][1]):
                if re.match(r"^" + re.escape(self._expectedcgroup) + r"($|\.)", desc):
                    dataidx = idx
                    break
            # No cgroup info at this datapoint
            if dataidx is None:
                return True
            nodemem_gb = data[0][dataidx] / 1073741824.0
            self._hostcounts[hostidx]['present'] += 1
        except ValueError:
            self._hostcounts[hostidx]['missing'] += 1
            # No cgroup info at this datapoint
            return True

        insertat = self._data.adddata(hostidx, timestamp, nodemem_gb)
        if insertat != None:
            self._hostdata[hostidx][insertat] = nodemem_gb

        return True

    def results(self):

        if len(self._hostdata) != self._job.nodecount:
            return {'error': ProcessingError.RAW_COUNTER_UNAVAILABLE}

        for hcount in self._hostcounts.values():
            if hcount['missing'] > hcount['present']:
                return {'error': ProcessingError.CPUSET_UNKNOWN}

        values = self._data.get()

        if len(self._hostdata) > 64:

            # Compute min, max & median data and only save the host data
            # for these hosts

            memdata = values[:, :, 1]
            sortarr = numpy.argsort(memdata.T, axis=1)

            retdata = {
                "min": self.collatedata(sortarr[:, 0], memdata),
                "max": self.collatedata(sortarr[:, -1], memdata),
                "med": self.collatedata(sortarr[:, sortarr.shape[1] // 2], memdata),
                "times": values[0, :, 0].tolist(),
                "hosts": {}
            }

            uniqhosts = Counter(sortarr[:, 0])
            uniqhosts.update(sortarr[:, -1])
            uniqhosts.update(sortarr[:, sortarr.shape[1] // 2])
            includelist = list(uniqhosts.keys())
        else:
            # Save data for all hosts
            retdata = {
                "times": values[0, :, 0].tolist(),
                "hosts": {}
            }
            includelist = list(self._hostdata.keys())


        for hostidx in includelist:
            retdata['hosts'][str(hostidx)] = {}
            retdata['hosts'][str(hostidx)]['all'] = values[hostidx, :, 1].tolist()

        return retdata

    @staticmethod
    def collatedata(args, rates):
        """ build output data """
        result = []
        for timepoint, hostidx in enumerate(args):
            try:
                result.append([rates[hostidx, timepoint], int(hostidx)])
            except IndexError:
                pass

        return result
