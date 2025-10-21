#!/usr/bin/env python3
""" Socket level performance counter plugin """

from supremm.plugin import Plugin
from supremm.statistics import calculate_stats
from supremm.errors import ProcessingError
import numpy

SNB_METRICS = ["perfevent.hwcounters.snbep_unc_imc0__UNC_M_CAS_COUNT_RD.value",
               "perfevent.hwcounters.snbep_unc_imc0__UNC_M_CAS_COUNT_WR.value",
               "perfevent.hwcounters.snbep_unc_imc1__UNC_M_CAS_COUNT_RD.value",
               "perfevent.hwcounters.snbep_unc_imc1__UNC_M_CAS_COUNT_WR.value",
               "perfevent.hwcounters.snbep_unc_imc2__UNC_M_CAS_COUNT_RD.value",
               "perfevent.hwcounters.snbep_unc_imc2__UNC_M_CAS_COUNT_WR.value",
               "perfevent.hwcounters.snbep_unc_imc3__UNC_M_CAS_COUNT_RD.value",
               "perfevent.hwcounters.snbep_unc_imc3__UNC_M_CAS_COUNT_WR.value"]

IVB_METRICS = ["perfevent.hwcounters.ivbep_unc_imc0__UNC_M_CAS_COUNT_RD.value",
               "perfevent.hwcounters.ivbep_unc_imc0__UNC_M_CAS_COUNT_WR.value",
               "perfevent.hwcounters.ivbep_unc_imc1__UNC_M_CAS_COUNT_RD.value",
               "perfevent.hwcounters.ivbep_unc_imc1__UNC_M_CAS_COUNT_WR.value",
               "perfevent.hwcounters.ivbep_unc_imc2__UNC_M_CAS_COUNT_RD.value",
               "perfevent.hwcounters.ivbep_unc_imc2__UNC_M_CAS_COUNT_WR.value",
               "perfevent.hwcounters.ivbep_unc_imc3__UNC_M_CAS_COUNT_RD.value",
               "perfevent.hwcounters.ivbep_unc_imc3__UNC_M_CAS_COUNT_WR.value"]

NHM_METRICS = ["perfevent.hwcounters.UNC_LLC_MISS_READ.value",
               "perfevent.hwcounters.UNC_LLC_MISS_WRITE.value"]

INTERLAGOS_METRICS = ["perfevent.hwcounters.L3_CACHE_MISSES_ALL.value"]

class UncoreCounters(Plugin):
    """ Compute various uncore performance counter derived metrics """

    name = property(lambda x: "uncperf")
    mode = property(lambda x: "firstlast")
    requiredMetrics = property(lambda x: [SNB_METRICS, IVB_METRICS, NHM_METRICS, INTERLAGOS_METRICS])
    optionalMetrics = property(lambda x: [])
    derivedMetrics = property(lambda x: [])

    def __init__(self, job):
        super(UncoreCounters, self).__init__(job)
        self._first = {}
        self._data = {}
        self._error = None

    def process(self, nodemeta, timestamp, data, description):

        if not self._job.getdata('perf')['active']:
            self._error = ProcessingError.RAW_COUNTER_UNAVAILABLE
            return False

        ndata = numpy.array(data)

        if nodemeta.nodename not in self._first:
            self._first[nodemeta.nodename] = ndata
            return True

        if ndata.shape == self._first[nodemeta.nodename].shape:
            self._data[nodemeta.nodename] = numpy.sum(ndata - self._first[nodemeta.nodename])
            if numpy.any(numpy.fabs(self._data[nodemeta.nodename]) != self._data[nodemeta.nodename]):
                self._error = ProcessingError.PMDA_RESTARTED_DURING_JOB
                return False
        else:
            # Perf counters changed during the job
            self._error = ProcessingError.RAW_COUNTER_UNAVAILABLE
            return False

        return True

    def results(self):

        if self._error is not None:
            return {"error": self._error}

        nhosts = len(self._data)

        if nhosts < 1:
            return {"error": ProcessingError.INSUFFICIENT_DATA}

        membw = numpy.zeros(nhosts)
        for hostindex, data in enumerate(self._data.values()):
            membw[hostindex] = data * 64.0

        results = {"membw": calculate_stats(membw)}
        return results
