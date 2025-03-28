#!/usr/bin/env python3
""" Timeseries subsampling module """
import numpy


class TimeseriesAccumulator():
    """ Stores a subset of time-value pairs for a dataseries """
    MAX_DATAPOINTS = 100
    LEAD_IN_DATAPOINTS = 10

    def __init__(self, nhosts, totaltime):
        self._totaltime = totaltime
        self._samplewindow = None
        self._leadout = None
        self._data = numpy.empty((nhosts, TimeseriesAccumulator.MAX_DATAPOINTS, 2))
        self._count = numpy.zeros(nhosts, dtype=int)

    def adddata(self, hostidx, timestamp, value):
        """ Add a datapoint to the collection.
        The sampling algorithm is as follows: The first LEAD_IN data points are
        always added Then the sample interval is computed, and one datapoint
        per interval is collected Near the end of the job, all points are
        collected again (based on the amount of time to get the first LEAD_IN.

        The sampling algorithm could be changed to try to capture more fine
        detail by changing the sample interval in response to the rate of
        change of the value (longer sample interval when there is little
        change, shorter when change is occuring). But this is left as an
        exercise for the reader
        """
        if self._count[hostidx] <= TimeseriesAccumulator.LEAD_IN_DATAPOINTS:
            idx = self._append(hostidx, timestamp, value)
            return idx

        if self._samplewindow == None:
            # compute sample window based on the first host to pass the post
            leadin = self._data[hostidx, TimeseriesAccumulator.LEAD_IN_DATAPOINTS, 0] - self._data[hostidx, 0, 0]
            self._samplewindow = (self._totaltime - (2.0 * leadin)) / (TimeseriesAccumulator.MAX_DATAPOINTS - 2 * TimeseriesAccumulator.LEAD_IN_DATAPOINTS)
            self._leadout = self._data[hostidx, 0, 0] + self._totaltime - leadin

        if ((timestamp > self._leadout) or (timestamp > self._data[hostidx, self._count[hostidx] - 1, 0] + self._samplewindow)) and self._count[hostidx] < TimeseriesAccumulator.MAX_DATAPOINTS:
            idx = self._append(hostidx, timestamp, value)
            return idx

        return None

    def _append(self, hostidx, timestamp, value):
        """ Add this data to the store """
        insertidx = self._count[hostidx]
        self._data[hostidx, insertidx, 0] = timestamp
        self._data[hostidx, insertidx, 1] = value
        self._count[hostidx] += 1
        return insertidx

    def gethost(self, hostidx):
        """ return the data series """
        return self._data[hostidx, :self._count[hostidx], :]

    def get(self):
        """ TODO numpy interp """
        return self._data[:, :numpy.min(self._count), :]

    def __str__(self):
        return str(self._data[:, :self._count, :])


class RangeConverter():
    """
    Convert data from limited width to 64bit width. Optionally raise an exception if
    the counters spin too fast.
    """

    def __init__(self, precision, checkoverflow=False):
        self._range = pow(2.0, precision)
        self._last = None
        self._accumulator = None
        self._checkoverflow = checkoverflow

    def append(self, indata):
        """ add updated data and return stored value """
        value = numpy.array(indata)

        if self._last != None:
            delta = (value - self._last) % self._range

            if self._checkoverflow:
                if delta > (self._range / 2.0):
                    raise Exception("Counter overflow")
            self._accumulator += delta
        else:
            self._accumulator = numpy.zeros(value.shape)

        self._last = value

        return self._accumulator

    def get(self):
        """ get current stored value """
        return self._accumulator
