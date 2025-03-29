#!/usr/bin/env python3
""" Timeseries generator module """

from supremm.plugin import Plugin
from supremm.subsample import TimeseriesAccumulator
from supremm.errors import ProcessingError
import numpy
from collections import Counter

SVE_METRICS = ["perfevent.hwcounters.arm_a64fx__SVE_INST_RETIRED.value"]

class SveTimeseries(Plugin):
    """ Generate the CPU usage as a timeseries data """

    name = property(lambda x: "sveins")
    mode = property(lambda x: "timeseries")
    requiredMetrics = property(lambda x: [SVE_METRICS])
    optionalMetrics = property(lambda x: [])
    derivedMetrics = property(lambda x: [])

    def __init__(self, job):
        super(SveTimeseries, self).__init__(job)
        self._data = TimeseriesAccumulator(job.nodecount, self._job.walltime)
        self._hostdata = {}
        self._hostdevnames = {}
        self._error = None

    def process(self, nodemeta, timestamp, data, description):

        if not self._job.getdata('perf')['active']:
            self._error = ProcessingError.RAW_COUNTER_UNAVAILABLE
            return False

        if len(data[0]) == 0:
            # Ignore timesteps where data was not available
            return True

        hostidx = nodemeta.nodeindex

        if nodemeta.nodeindex not in self._hostdata:
            self._hostdata[hostidx] = numpy.empty((TimeseriesAccumulator.MAX_DATAPOINTS, len(data[0])))
            self._hostdevnames[hostidx] = dict((str(k), v) for k, v in zip(description[0][0], description[0][1]))

        if len(data) == len(SVE_METRICS): # Note that INTERLAGOS is covered here too
            flops = numpy.array(data[0])
        else:
            flops = 4.0 * data[0] + 2.0 * data[1] + data[2] + data[3]

        insertat = self._data.adddata(hostidx, timestamp, numpy.sum(flops))
        if insertat is not None:
            self._hostdata[hostidx][insertat] = flops

            if insertat > 1:
                if numpy.any(flops - self._hostdata[hostidx][insertat-1] < 0.0):
                    self._error = ProcessingError.PMDA_RESTARTED_DURING_JOB
                    return False

        return True

    def results(self):

        if self._error is not None:
            return {"error": self._error}

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
            includelist = uniqhosts.keys()
        else:
            # Save data for all hosts
            retdata = {
                "times": values[0, 1:, 0].tolist(),
                "hosts": {}
            }
            includelist = self._hostdata.keys()


        for hostidx in includelist:
            retdata['hosts'][str(hostidx)] = {}
            retdata['hosts'][str(hostidx)]['all'] = rates[hostidx, :].tolist()
            retdata['hosts'][str(hostidx)]['dev'] = {}

            for devid in self._hostdevnames[hostidx].iterkeys():
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
