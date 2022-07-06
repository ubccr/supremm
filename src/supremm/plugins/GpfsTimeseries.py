#!/usr/bin/env python3
""" Timeseries generator module """

from supremm.plugin import RateConvertingTimeseriesPlugin
import numpy

class GpfsTimeseries(RateConvertingTimeseriesPlugin):
    """ Generate the GPFS usage as a timeseries data """

    name = property(lambda x: "lnet")
    requiredMetrics = property(lambda x: ["gpfs.fsios.read_bytes", "gpfs.fsios.write_bytes"])
    optionalMetrics = property(lambda x: [])
    derivedMetrics = property(lambda x: [])

    def __init__(self, job):
        super(GpfsTimeseries, self).__init__(job)

    def computetimepoint(self, data):
        return (numpy.sum(data[0]) + numpy.sum(data[1])) / 1048576.0
