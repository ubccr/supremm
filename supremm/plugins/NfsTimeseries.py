#!/usr/bin/env python
""" Timeseries generator module """

from supremm.plugin import RateConvertingTimeseriesPlugin
import numpy

class NfsTimeseries(RateConvertingTimeseriesPlugin):
    """ Generate timeseries summary for NFS usage data """

    name = property(lambda x: "nfs")
    requiredMetrics = property(lambda x: ["nfsclient.bytes.read.normal",
                                          "nfsclient.bytes.read.direct",
                                          "nfsclient.bytes.read.server",
                                          "nfsclient.bytes.write.normal",
                                          "nfsclient.bytes.write.direct",
                                          "nfsclient.bytes.write.server"])
    optionalMetrics = property(lambda x: [])
    derivedMetrics = property(lambda x: [])

    def __init__(self, job):
        super(NfsTimeseries, self).__init__(job)

    def computetimepoint(self, data):
        try:
            return numpy.sum(numpy.array(data)) / 1048576.0
        except ValueError:
            # NFS mount points can dissapear / appear during the job
            # skip points that are inconsistent with the first point
            return None
