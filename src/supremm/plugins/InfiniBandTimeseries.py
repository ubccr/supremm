#!/usr/bin/env python3
""" Timeseries generator module """

from supremm.plugin import RateConvertingTimeseriesPlugin
import numpy

class InfiniBandTimeseries(RateConvertingTimeseriesPlugin):
    """ Generate the infiniband usage as a timeseries data """

    name = property(lambda x: "ib_lnet")
    mode = property(lambda x: "timeseries")
    requiredMetrics = property(lambda x: ["infiniband.port.switch.in.bytes", "infiniband.port.switch.out.bytes"])
    optionalMetrics = property(lambda x: [])
    derivedMetrics = property(lambda x: [])

    def __init__(self, job):
        super(InfiniBandTimeseries, self).__init__(job)

    def computetimepoint(self, data):
        return (numpy.sum(data[0]) + numpy.sum(data[1])) / 1048576.0
