#!/usr/bin/env python3
""" Timeseries generator module """
from collections import Counter

from supremm.plugin import Plugin
from supremm.subsample import TimeseriesAccumulator
from supremm.errors import ProcessingError

import numpy

class CpuUserTimeseries(Plugin):
    """ Generate the CPU usage as a timeseries data """

    name = property(lambda x: "cpuuser")
    mode = property(lambda x: "timeseries")
    requiredMetrics = property(lambda x: ["kernel.percpu.cpu.user"])
    optionalMetrics = property(lambda x: [])
    derivedMetrics = property(lambda x: [])

    def __init__(self, job):
        super(CpuUserTimeseries, self).__init__(job)
        self._data = TimeseriesAccumulator(job.nodecount, self._job.walltime)
        self._hostdata = {}
        self._hostdevnames = {}
        self._cpusallowed = None

    def initcpus(self):
        if self._job.getdata('proc'):
            self._cpusallowed = self._job.getdata('proc')['cpusallowed']
        else:
            self._cpusallowed = {}

    def process(self, nodemeta, timestamp, data, description):

        if self._cpusallowed is None:
            self.initcpus()

        if len(data[0]) == 0:
            # Skip datapoints that have no values
            return True

        if nodemeta.nodename in self._cpusallowed and 'error' not in self._cpusallowed[nodemeta.nodename]:
            cpudata = data[0][self._cpusallowed[nodemeta.nodename]]
        else:
            cpudata = data[0]

        hostidx = nodemeta.nodeindex

        if nodemeta.nodeindex not in self._hostdata:
            self._hostdata[hostidx] = numpy.empty((TimeseriesAccumulator.MAX_DATAPOINTS, len(cpudata)))
            if nodemeta.nodename in self._cpusallowed and 'error' not in self._cpusallowed[nodemeta.nodename]:
                self._hostdevnames[hostidx] = {}
                for i, cpuidx in enumerate(self._cpusallowed[nodemeta.nodename]):
                    self._hostdevnames[hostidx][str(i)] = description[0][1][cpuidx]
            else:
                self._hostdevnames[hostidx] = dict((str(k), v) for k, v in zip(description[0][0], description[0][1]))

        insertat = self._data.adddata(hostidx, timestamp, numpy.mean(cpudata)/10.0)
        if insertat is not None:
            self._hostdata[hostidx][insertat] = cpudata / 10.0

        return True

    def results(self):

        values = self._data.get()

        if len(values[0, :, 0]) < 3:
            return {"error": ProcessingError.JOB_TOO_SHORT}

        rates = numpy.diff(values[:, :, 1]) / numpy.diff(values[:, :, 0])

        if len(self._hostdata) > 64:

            # Compute min, max & median data and only save the host data
            # for these hosts

            sortarr = numpy.argsort(rates.T, axis=1)

            retdata = {
                "min": self.collatedata(sortarr[:, 0], rates),
                "max": self.collatedata(sortarr[:, -1], rates),
                "med": self.collatedata(sortarr[:, sortarr.shape[1] // 2], rates),
                "times": values[0, 1:, 0].tolist(),
                "hosts": {}
            }

            uniqhosts = Counter(sortarr[:, 0])
            uniqhosts.update(sortarr[:, -1])
            uniqhosts.update(sortarr[:, sortarr.shape[1] // 2])
            includelist = list(uniqhosts.keys())
        else:
            # Save data for all hosts
            retdata = {
                "times": values[0, 1:, 0].tolist(),
                "hosts": {}
            }
            includelist = list(self._hostdata.keys())


        for hostidx in includelist:
            retdata['hosts'][str(hostidx)] = {}
            retdata['hosts'][str(hostidx)]['all'] = rates[hostidx, :].tolist()
            retdata['hosts'][str(hostidx)]['dev'] = {}

            for devid in self._hostdevnames[hostidx].keys():
                dpnts = len(values[hostidx, :, 0])
                retdata['hosts'][str(hostidx)]['dev'][devid] = (numpy.diff(self._hostdata[hostidx][:dpnts, numpy.int(devid)]) / numpy.diff(values[hostidx, :, 0])).tolist()

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
