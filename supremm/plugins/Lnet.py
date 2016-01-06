#!/usr/bin/env python
""" Lnet statistics """

from supremm.plugin import Plugin
from supremm.statistics import calculate_stats
import numpy

class Lnet(Plugin):
    """ Compute the overall lnet usage for a job """

    name = property(lambda x: "lnet")
    mode = property(lambda x: "firstlast")
    requiredMetrics = property(lambda x: ["lustre.lnet.drop_length", "lustre.lnet.recv_length", "lustre.lnet.send_length", "lustre.lnet.drop_count", "lustre.lnet.recv_count", "lustre.lnet.send_count"])
    optionalMetrics = property(lambda x: [])
    derivedMetrics = property(lambda x: [])

    def __init__(self, job):
        super(Lnet, self).__init__(job)
        self._first = {}
        self._data = numpy.empty((job.nodecount, len(self.requiredMetrics)))
        self._hostidx = 0

    def process(self, nodemeta, timestamp, data, description):

        vals = numpy.array(data)[:, 0]

        if nodemeta.nodename not in self._first:
            self._first[nodemeta.nodename] = vals
            return True

        self._data[self._hostidx, :] = vals -  self._first[nodemeta.nodename]
        self._hostidx += 1

        return True

    def results(self):

        output = {}

        for i, nicename in enumerate(['drop', 'recv', 'send', 'drop_count', 'recv_count', 'send_count']):
            output[nicename] = calculate_stats(self._data[:self._hostidx, i])

        return output
