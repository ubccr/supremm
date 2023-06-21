#!/usr/bin/env python3
""" Timeseries generator module """

from collections import Counter
import numpy

from supremm.plugin import Plugin
from supremm.subsample import TimeseriesAccumulator
from supremm.errors import ProcessingError

class PowerUsageTimeseries(Plugin):
    """ Generate the Power usage as a timeseries data """

    name = property(lambda x: "power")
    mode = property(lambda x: "timeseries")
    requiredMetrics = property(lambda x: ["ipmi.dcmi.power"])
    optionalMetrics = property(lambda x: [])
    derivedMetrics = property(lambda x: [])

    def __init__(self, job):
        super(PowerUsageTimeseries, self).__init__(job)
        self._data = TimeseriesAccumulator(job.nodecount, self._job.walltime)
        self._hostdata = {}

    @staticmethod
    def computetimepoint(data):
        """ Get the power usage from the data """
        if data[0][0] < numpy.finfo(numpy.float64).eps:
            return None

        return data[0][0]

    def process(self, nodemeta, timestamp, data, description):

        if not data[0]:
            # Skip data point with no data
            return True

        if nodemeta.nodeindex not in self._hostdata:
            self._hostdata[nodemeta.nodeindex] = 1

        datum = self.computetimepoint(data)
        if datum != None:
            self._data.adddata(nodemeta.nodeindex, timestamp, datum)

        return True

    def results(self):

        if len(self._hostdata) != self._job.nodecount:
            return {"error": ProcessingError.INSUFFICIENT_HOSTDATA}

        values = self._data.get()

        if len(values[0, :, 0]) < 3:
            return {"error": ProcessingError.JOB_TOO_SHORT}

        power = values[:, :, 1]

        if len(self._hostdata) > 64:

            # Compute min, max & median data and only save the host data
            # for these hosts

            sortarr = numpy.argsort(power.T, axis=1)

            retdata = {
                "min": self.collatedata(sortarr[:, 0], power),
                "max": self.collatedata(sortarr[:, -1], power),
                "med": self.collatedata(sortarr[:, sortarr.shape[1] // 2], power),
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
            retdata['hosts'][str(hostidx)]['all'] = power[hostidx, :].tolist()

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
