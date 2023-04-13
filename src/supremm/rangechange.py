#!/usr/bin/env python3
import numpy

class DataCache(object):
    """ Helper class that remembers the last value that it was passed """
    def __init__(self):
        self.mdata = None
        self.timestamp = None
        self.data = None
        self.description = None

    def name(self):
        """ returns the name """
        return 'datacache'

    def process(self, mdata, timestamp, data, description):
        """ process call """
        self.mdata = mdata
        self.timestamp = timestamp
        self.data = data
        self.description = description

    def docallback(self, analytic):
        """ call the analytic with the paramerters from the most recent call to
            process (if any) """
        if self.timestamp != None:
            return analytic.process(self.mdata, self.timestamp, self.data, self.description)
        else:
            return True

class RangeChange(object):
    """ Convert counters that have < 64 bits to 64 bits """
    def __init__(self, configobj):
        try:
            self.config = configobj.getsection('normalization')
        except KeyError:
            self.config = []

        self._passthrough = False
        self.accumulator = []
        self.last = []
        self.needsfixup = []

    def set_fetched_metrics(self, metriclist):
        """ sets the list of metrics that will be passed to the normalise_data function
            This resets the internal state of the object """

        self.accumulator = [None] * len(metriclist)
        self.last = [None] * len(metriclist)
        self.needsfixup = []
        self._passthrough = True

        for metric in metriclist:
            if metric in self.config:
                self.needsfixup.append(self.config[metric])
                self._passthrough = False
            else:
                self.needsfixup.append(None)

    @property
    def passthrough(self):
        """ Returns whether the range changer will not modify data """
        return self._passthrough

    def normalise_data(self, timestamp, data):
        """ Convert the data if needed """

        if self._passthrough:
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
                self.accumulator[i] += (datum - self.last[i]) % numpy.uint64(1 << self.needsfixup[i]['range'])
                numpy.copyto(self.last[i], datum)
                numpy.copyto(datum, self.accumulator[i])

            i += 1


