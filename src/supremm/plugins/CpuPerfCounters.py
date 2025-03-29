#!/usr/bin/env python3
""" CPU performance counter plugin """

from supremm.plugin import Plugin
from supremm.statistics import calculate_stats
from supremm.errors import ProcessingError
import numpy


SNB_METRICS = [
    "perfevent.hwcounters.UNHALTED_REFERENCE_CYCLES.value",
    "perfevent.hwcounters.INSTRUCTIONS_RETIRED.value",
    "perfevent.hwcounters.MEM_LOAD_UOPS_RETIRED_L1_HIT.value",
    "perfevent.hwcounters.SIMD_FP_256_PACKED_DOUBLE.value",
    "perfevent.hwcounters.FP_COMP_OPS_EXE_SSE_FP_PACKED_DOUBLE.value",
    "perfevent.hwcounters.FP_COMP_OPS_EXE_SSE_SCALAR_DOUBLE.value",
    "perfevent.hwcounters.FP_COMP_OPS_EXE_X87.value"
]

NHM_METRICS = [
    "perfevent.hwcounters.UNHALTED_REFERENCE_CYCLES.value",
    "perfevent.hwcounters.INSTRUCTIONS_RETIRED.value",
    "perfevent.hwcounters.MEM_LOAD_RETIRED_L1D_HIT.value",
    "perfevent.hwcounters.FP_COMP_OPS_EXE_SSE_FP.value"
]

NHM_ALT_METRICS = [
    "perfevent.hwcounters.UNHALTED_REFERENCE_CYCLES.value",
    "perfevent.hwcounters.INSTRUCTIONS_RETIRED.value",
    "perfevent.hwcounters.L1D_REPL.value",
    "perfevent.hwcounters.FP_COMP_OPS_EXE_SSE_FP.value"
]

GENERIC_INTEL_METRICS = [
    "perfevent.hwcounters.UNHALTED_REFERENCE_CYCLES.value",
    "perfevent.hwcounters.INSTRUCTIONS_RETIRED.value",
    "perfevent.hwcounters.L1D_REPLACEMENT.value"
]

GENERIC_INTEL_ALT_METRICS = [
    "perfevent.hwcounters.UNHALTED_REFERENCE_CYCLES.value",
    "perfevent.hwcounters.INSTRUCTION_RETIRED.value"
]

GENERIC_INTEL_ALT2_METRICS = [
    "perfevent.hwcounters.UNHALTED_REFERENCE_CYCLES.value",
    "perfevent.hwcounters.INSTRUCTIONS_RETIRED.value"
]

ARM64_METRICS = [
    "perfevent.hwcounters.perf__instructions.value",
    "perfevent.hwcounters.perf__cycles.value"
]

AMD_INTERLAGOS_METRICS = [
    "perfevent.hwcounters.CPU_CLK_UNHALTED.value",
    "perfevent.hwcounters.RETIRED_INSTRUCTIONS.value",
    "perfevent.hwcounters.DATA_CACHE_MISSES_DC_MISS_STREAMING_STORE.value",
    "perfevent.hwcounters.RETIRED_SSE_OPS_ALL.value"
]

class CpuPerfCounters(Plugin):
    """ Compute various performance counter derived metrics """

    name = property(lambda x: "cpuperf")
    mode = property(lambda x: "firstlast")
    requiredMetrics = property(lambda x: [SNB_METRICS, NHM_METRICS, NHM_ALT_METRICS, GENERIC_INTEL_METRICS, ARM64_METRICS, AMD_INTERLAGOS_METRICS, GENERIC_INTEL_ALT_METRICS, GENERIC_INTEL_ALT2_METRICS])
    optionalMetrics = property(lambda x: [])
    derivedMetrics = property(lambda x: [])

    def __init__(self, job):
        super(CpuPerfCounters, self).__init__(job)
        self._first = {}
        self._data = {}
        self._totalcores = 0
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
            self._data[nodemeta.nodename] = ndata - self._first[nodemeta.nodename]
            self._totalcores += data[0].size
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
            return {"error": ProcessingError.INSUFFICIENT_HOSTDATA}

        hasFlops = True
        hasCpld = True
        clks = numpy.zeros(self._totalcores)
        flops = numpy.zeros(self._totalcores)
        cpiref = numpy.zeros(self._totalcores)
        cpldref = numpy.zeros(self._totalcores)

        coreindex = 0
        for _, data in self._data.items():
            if len(data) == len(NHM_METRICS): # also covers the AMD_INTERLAGOS
                flops[coreindex:coreindex + len(data[0])] = 1.0 * data[3]
                cpiref[coreindex:coreindex + len(data[0])] = 1.0 * data[0] / data[1]
                cpldref[coreindex:coreindex + len(data[0])] = 1.0 * data[0] / data[2]
                clks[coreindex:coreindex + len(data[0])] = data[0] / 1232896.0
                coreindex += len(data[0])
            elif len(data) == len(SNB_METRICS):
                flops[coreindex:coreindex + len(data[0])] = 4.0 * data[3] + 2.0 * data[4] + 1.0 * data[5] + 1.0 * data[6]
                cpiref[coreindex:coreindex + len(data[0])] = 1.0 * data[0] / data[1]
                cpldref[coreindex:coreindex + len(data[0])] = 1.0 * data[0] / data[2]
                clks[coreindex:coreindex + len(data[0])] = data[0] / 1232896.0
                coreindex += len(data[0])
            elif len(data) == len(GENERIC_INTEL_METRICS):
                hasFlops = False
                cpiref[coreindex:coreindex + len(data[0])] = 1.0 * data[0] / data[1]
                cpldref[coreindex:coreindex + len(data[0])] = 1.0 * data[0] / data[2]
                clks[coreindex:coreindex + len(data[0])] = data[0] / 1232896.0
                coreindex += len(data[0])
            elif len(data) == len(GENERIC_INTEL_ALT_METRICS): # also covers the ALT2 variant
                hasFlops = False
                hasCpld = False
                cpiref[coreindex:coreindex + len(data[0])] = 1.0 * data[0] / data[1]
                clks[coreindex:coreindex + len(data[0])] = data[0] / 1232896.0
                coreindex += len(data[0])
            else:
                return {"error": ProcessingError.INSUFFICIENT_DATA}

        results = {}

        if hasFlops:
            results['flops'] = calculate_stats(flops)

        if numpy.isfinite(cpiref).all():
            results['cpiref'] = calculate_stats(cpiref)
        else:
            results['cpiref'] = {"error": ProcessingError.RAW_COUNTER_UNAVAILABLE}

        if hasCpld and numpy.isfinite(cpldref).all():
            results['cpldref'] = calculate_stats(cpldref)
        else:
            results['cpldref'] = {"error": ProcessingError.RAW_COUNTER_UNAVAILABLE}

        results['clk_mhz'] = calculate_stats(clks)

        return results
