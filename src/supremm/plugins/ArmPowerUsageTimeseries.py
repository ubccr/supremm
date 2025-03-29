#!/usr/bin/env python3
""" Timeseries generator module """

from collections import Counter
import numpy

from supremm.plugin import Plugin
from supremm.subsample import TimeseriesAccumulator
from supremm.errors import ProcessingError

class ArmPowerUsageTimeseries(Plugin):
    """ Generate the Power usage as a timeseries data """

    name = property(lambda x: "corepower")
    mode = property(lambda x: "timeseries")
    requiredMetrics = property(lambda x: ["perfevent.hwcounters.arm_a64fx__EA_CORE.value", "perfevent.hwcounters.arm_a64fx__EA_L2.value",
"perfevent.hwcounters.arm_a64fx__EA_MEMORY.value"])
    optionalMetrics = property(lambda x: [])
    derivedMetrics = property(lambda x: [])

    def __init__(self, job):
        super(ArmPowerUsageTimeseries, self).__init__(job)
        self._data = TimeseriesAccumulator(job.nodecount, self._job.walltime)
        self._error = None
        self._hostdata = {}

    @staticmethod
    def computetimepoint(data):
        """ Get the power usage from the data """
        if data[0][0] < numpy.finfo(numpy.float64).eps:
            return None

        return data[0][0]

    def process(self, nodemeta, timestamp, data, description):

        if not self._job.getdata('perf')['active']:
            self._error = ProcessingError.RAW_COUNTER_UNAVAILABLE
            return False

        if len(data[0]) == 0:
            # Skip data point with no data
            return True

        if nodemeta.nodeindex not in self._hostdata:
            self._hostdata[nodemeta.nodeindex] = numpy.empty((TimeseriesAccumulator.MAX_DATAPOINTS, 3))

        cpucount = numpy.sum(data[0])
        l2count = data[1][0] + data[1][12] + data[1][24] + data[1][36]
        memcount = data[2][0] + data[2][12] + data[2][24] + data[2][36]

        energy = (8.04 * cpucount) + (32.8 * l2count) + (271.0 * memcount)

        insertat = self._data.adddata(nodemeta.nodeindex, timestamp, energy)

        if insertat is not None:
            self._hostdata[nodemeta.nodeindex][insertat] = numpy.array([cpucount, l2count, memcount])

        return True

    def results(self):

        if self._error:
            return {"error": self._error}

        if len(self._hostdata) != self._job.nodecount:
            return {"error": ProcessingError.INSUFFICIENT_HOSTDATA}

        values = self._data.get()

        if len(values[0, :, 0]) < 2:
            return {"error": ProcessingError.JOB_TOO_SHORT}

        rates = numpy.diff(values[:, :, 1]) / numpy.diff(values[:, :, 0]) / 1.0e9

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
            includelist = uniqhosts.keys()
        else:
            # Save data for all hosts
            retdata = {
                "times": values[0, 1:, 0].tolist(),
                "hosts": {}
            }
            includelist = self._hostdata.keys()

        scaling = {
            '0': 8.04e-9,
            '1': 32.8e-9,
            '2': 271.e-9
        }

        for hostidx in includelist:
            retdata['hosts'][str(hostidx)] = {}
            retdata['hosts'][str(hostidx)]['all'] = rates[hostidx, :].tolist()
            retdata['hosts'][str(hostidx)]['dev'] = {}
            for devid in ['0', '1', '2']:
                dpnts = len(values[hostidx, :, 0])
                retdata['hosts'][str(hostidx)]['dev'][devid] = (scaling[devid] * numpy.diff(self._hostdata[hostidx][:dpnts, numpy.int(devid)]) / numpy.diff(values[hostidx, :, 0])).tolist()

            retdata['hosts'][str(hostidx)]['names'] = {'0': 'cpu', '1': 'l2', '2': 'mem'}

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
