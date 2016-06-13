#!/usr/bin/env python
""" Timeseries generator module """

from supremm.plugin import Plugin
from supremm.subsample import TimeseriesAccumulator
from supremm.errors import ProcessingError
import numpy

from sys import version as python_version
if python_version.startswith("2.6"):
    from backport_collections import Counter
else:
    from collections import Counter

SNB_METRICS = ["perfevent.active",
               "perfevent.hwcounters.snbep_unc_imc0__UNC_M_CAS_COUNT_RD.value",
               "perfevent.hwcounters.snbep_unc_imc0__UNC_M_CAS_COUNT_WR.value",
               "perfevent.hwcounters.snbep_unc_imc1__UNC_M_CAS_COUNT_RD.value",
               "perfevent.hwcounters.snbep_unc_imc1__UNC_M_CAS_COUNT_WR.value",
               "perfevent.hwcounters.snbep_unc_imc2__UNC_M_CAS_COUNT_RD.value",
               "perfevent.hwcounters.snbep_unc_imc2__UNC_M_CAS_COUNT_WR.value",
               "perfevent.hwcounters.snbep_unc_imc3__UNC_M_CAS_COUNT_RD.value",
               "perfevent.hwcounters.snbep_unc_imc3__UNC_M_CAS_COUNT_WR.value"]

NHM_METRICS = ["perfevent.active",
               "perfevent.hwcounters.UNC_LLC_MISS_READ.value",
               "perfevent.hwcounters.UNC_LLC_MISS_WRITE.value"]

class MemBwTimeseries(Plugin):
    """ Generate the memory bandwidth as timeseries data """

    name = property(lambda x: "membw")
    mode = property(lambda x: "timeseries")
    requiredMetrics = property(lambda x: [SNB_METRICS, NHM_METRICS])
    optionalMetrics = property(lambda x: [])
    derivedMetrics = property(lambda x: [])

    def __init__(self, job):
        super(MemBwTimeseries, self).__init__(job)
        self._data = TimeseriesAccumulator(job.nodecount, self._job.walltime)
        self._hostdata = {}
        self._hostdevnames = {}
        self._error = None

    def process(self, nodemeta, timestamp, data, description):

        if len(data[0]) > 0 and data[0][0] == 0:
            # If active == 0 then the PMDA was switched off due to user request
            self._error = ProcessingError.RAW_COUNTER_UNAVAILABLE
            return False

        if len(data[1]) == 0:
            # Ignore timesteps where data was not available
            return True

        hostidx = nodemeta.nodeindex

        if nodemeta.nodeindex not in self._hostdata:
            self._hostdata[hostidx] = numpy.empty((TimeseriesAccumulator.MAX_DATAPOINTS, len(data[1])))
            self._hostdevnames[hostidx] = dict((str(k), v) for k, v in zip(description[0][0], description[0][1]))

        membw = 64.0 * numpy.sum(data[1:]) / 1024.0 / 1024.0 / 1024.0

        insertat = self._data.adddata(hostidx, timestamp, membw)
        if insertat != None:
            self._hostdata[hostidx][insertat] = membw

            if insertat > 1:
                if numpy.any(membw - self._hostdata[hostidx][insertat-1] < 0.0):
                    self._error = ProcessingError.PMDA_RESTARTED_DURING_JOB
                    return False

        return True

    def results(self):

        if self._error != None:
            return {"error": self._error}

        values = self._data.get()
        rates = numpy.diff(values[:, :, 1]) / numpy.diff(values[:, :, 0])

        if len(self._hostdata) > 64:

            # Compute min, max & median data and only save the host data
            # for these hosts

            sortarr = numpy.argsort(rates.T, axis=1)

            retdata = {
                "min": self.collatedata(sortarr[:, 0], rates),
                "max": self.collatedata(sortarr[:, -1], rates),
                "med": self.collatedata(sortarr[:, sortarr.shape[1] / 2], rates),
                "times": values[0, 1:, 0].tolist(),
                "hosts": {}
            }

            uniqhosts = Counter(sortarr[:, 0])
            uniqhosts.update(sortarr[:, -1])
            uniqhosts.update(sortarr[:, sortarr.shape[1] / 2])
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
                retdata['hosts'][str(hostidx)]['dev'][devid] = (numpy.diff(self._hostdata[hostidx][:dpnts, devid]) / numpy.diff(values[hostidx, :, 0])).tolist()

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
