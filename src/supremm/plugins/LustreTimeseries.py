#!/usr/bin/env python
""" Timeseries generator module """

from supremm.plugin import RateConvertingTimeseriesPlugin
import numpy

class LustreTimeseries(RateConvertingTimeseriesPlugin):
    """ Generate the Lustre usage as a timeseries data """

    name = property(lambda x: "lnet")
    metric_system = property(lambda x: "pcp")
    requiredMetrics = property(lambda x: ["lustre.llite.read_bytes.total", "lustre.llite.write_bytes.total"])
    optionalMetrics = property(lambda x: [])
    derivedMetrics = property(lambda x: [])

    def __init__(self, job, config):
        super(LustreTimeseries, self).__init__(job, config)

    def computetimepoint(self, data):
        return (numpy.sum(data[0]) + numpy.sum(data[1])) / 1048576.0
