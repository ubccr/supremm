#!/usr/bin/env python3
""" CPU performance counter plugin """

from supremm.plugin import Plugin
from supremm.statistics import calculate_stats
from supremm.errors import ProcessingError
import numpy

SNB_METRICS = ["taccstats_perfevent.hwcounters.UNHALTED_REFERENCE_CYCLES.value",
               "taccstats_perfevent.hwcounters.INSTRUCTION_RETIRED.value",
               "taccstats_perfevent.hwcounters.L1D_REPLACEMENT.value",
               "taccstats_perfevent.hwcounters.SIMD_FP_256_PACKED_DOUBLE.value",
               "taccstats_perfevent.hwcounters.FP_COMP_OPS_EXE_SSE_FP_PACKED_DOUBLE.value",
               "taccstats_perfevent.hwcounters.FP_COMP_OPS_EXE_SSE_SCALAR_DOUBLE.value"]

NHM_METRICS = ["taccstats_perfevent.hwcounters.UNHALTED_REFERENCE_CYCLES.value",
               "taccstats_perfevent.hwcounters.INSTRUCTIONS_RETIRED.value",
               "taccstats_perfevent.hwcounters.MEM_LOAD_RETIRED_L1D_HIT.value",
               "taccstats_perfevent.hwcounters.FP_COMP_OPS_EXE_SSE_FP.value"]

class TaccPerfCounters(Plugin):
    """ Compute various performance counter derived metrics """
    name = property(lambda x: "cpuperf")
    mode = property(lambda x: "all")
    requiredMetrics = property(lambda x: [SNB_METRICS, NHM_METRICS])
    optionalMetrics = property(lambda x: [])
    derivedMetrics = property(lambda x: [])

    def __init__(self, job):
        super(TaccPerfCounters, self).__init__(job)
        self._last = {}
        self._data = {}
        self._totalcores = 0
        self._error = None

    def process(self, nodemeta, timestamp, data, description):

        ndata = numpy.array(data)

        if nodemeta.nodename not in self._last:
            self._last[nodemeta.nodename] = ndata
            return True

        if ndata.shape == self._last[nodemeta.nodename].shape:
            if nodemeta.nodename not in self._data:
                # Only populate data for a host when we have at least 2 datapoints
                self._data[nodemeta.nodename] = numpy.zeros(ndata.shape)
                self._totalcores += data[0].size

            self._data[nodemeta.nodename] += (ndata - self._last[nodemeta.nodename]) % (2**48)
            self._last[nodemeta.nodename] = ndata
        else:
            # Perf counters changed during the job
            self._error = ProcessingError.RAW_COUNTER_UNAVAILABLE
            return False

        return True

    def results(self):

        if self._error != None:
            return {"error": self._error}

        nhosts = len(self._data)

        if nhosts < 1:
            return {"error": ProcessingError.INSUFFICIENT_HOSTDATA}

        flops = numpy.zeros(self._totalcores)
        cpiref = numpy.zeros(self._totalcores)
        cpldref = numpy.zeros(self._totalcores)

        coreindex = 0
        for data in self._data.values():
            if len(data) == len(NHM_METRICS):
                flops[coreindex:coreindex+len(data[0])] = 1.0 * data[3]
                cpiref[coreindex:coreindex+len(data[0])] = 1.0 * data[0] / data[1]
                cpldref[coreindex:coreindex+len(data[0])] = 1.0 * data[0] / data[2]
                coreindex += len(data[0])
            elif len(data) == len(SNB_METRICS):
                flops[coreindex:coreindex+len(data[0])] = 4.0 * data[3] + 2.0 * data[4] + 1.0 * data[5]
                cpiref[coreindex:coreindex+len(data[0])] = 1.0 * data[0] / data[1]
                cpldref[coreindex:coreindex+len(data[0])] = 1.0 * data[0] / data[2]
                coreindex += len(data[0])
            else:
                return {"error": ProcessingError.INSUFFICIENT_DATA}

        results = {"flops": calculate_stats(flops), "cpiref": calculate_stats(cpiref), "cpldref": calculate_stats(cpldref)}
        return results
