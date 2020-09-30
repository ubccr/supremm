#!/usr/bin/env python
""" Timeseries generator module """

from supremm.plugin import Plugin
from supremm.subsample import TimeseriesAccumulator
import numpy
from collections import Counter

class MemUsageTimeseries(Plugin):
    """ Generate the CPU usage as a timeseries data """

    name = property(lambda x: "memused_minus_diskcache")
    metric_system = property(lambda x: "pcp")
    mode = property(lambda x: "timeseries")
    requiredMetrics = property(lambda x: ["mem.numa.util.used", "mem.numa.util.filePages", "mem.numa.util.slab"])
    optionalMetrics = property(lambda x: [])
    derivedMetrics = property(lambda x: [])

    def __init__(self, job, config):
        super(MemUsageTimeseries, self).__init__(job, config)
        self._data = TimeseriesAccumulator(job.nodecount, self._job.walltime)
        self._hostdata = {}
        self._hostdevnames = {}

    def process(self, nodemeta, timestamp, data, description):

        hostidx = nodemeta.nodeindex

        if len(data[0]) == 0:
            # Skip data point with no data
            return True

        if nodemeta.nodeindex not in self._hostdata:
            self._hostdata[hostidx] = numpy.empty((TimeseriesAccumulator.MAX_DATAPOINTS, len(data[0])))
            self._hostdevnames[hostidx] = dict((str(k), "numa " + v) for k, v in zip(description[0][0], description[0][1]))

        nodemem_kb = numpy.sum(data[0]) - numpy.sum(data[1]) - numpy.sum(data[2])
        insertat = self._data.adddata(hostidx, timestamp, nodemem_kb / 1048576.0)
        if insertat != None:
            self._hostdata[hostidx][insertat] = (data[0] - data[1] - data[2]) / 1048576.0

        return True

    def results(self):

        values = self._data.get()

        if len(self._hostdata) > 64:

            # Compute min, max & median data and only save the host data
            # for these hosts

            memdata = values[:, :, 1]
            sortarr = numpy.argsort(memdata.T, axis=1)

            retdata = {
                "min": self.collatedata(sortarr[:, 0], memdata),
                "max": self.collatedata(sortarr[:, -1], memdata),
                "med": self.collatedata(sortarr[:, sortarr.shape[1] / 2], memdata),
                "times": values[0, :, 0].tolist(),
                "hosts": {}
            }

            uniqhosts = Counter(sortarr[:, 0])
            uniqhosts.update(sortarr[:, -1])
            uniqhosts.update(sortarr[:, sortarr.shape[1] / 2])
            includelist = uniqhosts.keys()
        else:
            # Save data for all hosts
            retdata = {
                "times": values[0, :, 0].tolist(),
                "hosts": {}
            }
            includelist = self._hostdata.keys()


        for hostidx in includelist:
            retdata['hosts'][str(hostidx)] = {}
            retdata['hosts'][str(hostidx)]['all'] = values[hostidx, :, 1].tolist()
            retdata['hosts'][str(hostidx)]['dev'] = {}

            for devid in self._hostdevnames[hostidx].iterkeys():
                dpnts = len(values[hostidx, :, 0])
                retdata['hosts'][str(hostidx)]['dev'][devid] = self._hostdata[hostidx][:dpnts, numpy.int(devid)].tolist()

            retdata['hosts'][str(hostidx)]['names'] = self._hostdevnames[hostidx]

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
