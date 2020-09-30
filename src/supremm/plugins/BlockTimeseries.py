#!/usr/bin/env python
""" Timeseries generator module """

from supremm.plugin import RateConvertingTimeseriesPlugin
import numpy

class BlockTimeseries(RateConvertingTimeseriesPlugin):
    """ Generate timeseries summary for block device usage data """

    name = property(lambda x: "block")
    metric_system = property(lambda x: "pcp")
    requiredMetrics = property(lambda x: ["disk.dev.read_bytes",
                                          "disk.dev.write_bytes"])
    optionalMetrics = property(lambda x: [])
    derivedMetrics = property(lambda x: [])

    def __init__(self, job, config):
        super(BlockTimeseries, self).__init__(job, config)

    def computetimepoint(self, data):
        return numpy.sum(numpy.array(data)) / 1048576.0
