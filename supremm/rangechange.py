#!/usr/bin/env python
import numpy

class DataCache(object):
    def __init__(self):
        self.mdata = None
        self.timestamp = None
        self.data = None
        self.description = None

    def name(self):
        return 'datacache'

    def process(self, mdata, timestamp, data, description):
        self.mdata = mdata
        self.timestamp = timestamp
        self.data = data
        self.description = description

    def docallback(self, analytic):
        if self.timestamp != None:
            return analytic.process(self.mdata, self.timestamp, self.data, self.description)
        else:
            return True

class RangeChange(object):
    def __init__(self, configobj):
        try:
            self.config = configobj.getsection('normalization')
        except KeyError as e:
            self.config = []

        self.passthrough = False
        self.accumulator = []
        self.last = []
        self.needsfixup = []

    def set_fetched_metrics(self, metriclist):
        """ sets the list of metrics that will be passed to the normalise_data function
            This resets the internal state of the object """

        self.metriclist = metriclist

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
        
    def passthrough(self):
        """ Returns whether the range changer will not modify data """
        return self.passthrough

    def normalise_data(self, timestamp, data):
        """ Convert the data if needed """

        if self.passthrough:
            return

        i = 0 
        for datum in data:

            if self.needsfixup[i] is None:
                i += 1
                continue

            if len(datum) == 0:
                # Ignore entries with no data - this typically occurs when the
                # plugin requests multiple metrics and the metrics do not all appear 
                # at every timestep
                i += 1
                continue

            if self.accumulator[i] is None:
                self.accumulator[i] = numpy.array(datum)
                self.last[i] = numpy.array(datum)
            else:
                self.accumulator[i] += ( datum - self.last[i] ) % numpy.uint64(1L << self.needsfixup[i]['range'])
                numpy.copyto(self.last[i], datum)
                numpy.copyto(datum, self.accumulator[i])

            i += 1


