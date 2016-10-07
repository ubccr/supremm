#!/usr/bin/env python
import numpy

class RangeChange(object):
    def __init__(self, config):
        self.config = config

        self.passthrough = False
        self.accumulator = []
        self.last = []
        self.needsfixup = []

    def set_fetched_metrics(self, metriclist):

        self.accumulator = [None] * len(metriclist)
        self.last = [None] * len(metriclist)
        self.needsfixup = []
        self.passthrough = True

        for metric in metriclist:
            if metric in self.config:
                self.needsfixup.append(self.config[metric])
                self.passthrough = False
            else:
                self.needsfixup.append(None)
        

    def normalise_data(self, data):
        """ Convert the data if needed """

        if self.passthrough:
            return

        i = 0 
        for datum in data:

            if self.needsfixup[i] == None:
                i += 1
                continue

            if len(datum) == 0:
                # Ignore entries with no data - this typically occurs when the
                # plugin requests multiple metrics and the metrics do not all appear 
                # at every timestep
                i += 1
                continue

            if self.accumulator[i] == None:
                self.accumulator[i] = numpy.array(datum)
                self.last[i] = numpy.array(datum)
            else:
                self.accumulator[i] += ( datum - self.last[i] ) % numpy.uint64(1L << self.needsfixup[i])
                numpy.copyto(self.last[i], datum)
                numpy.copyto(datum, self.accumulator[i])

            i += 1


