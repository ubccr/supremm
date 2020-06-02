""" Various utilities for calculating statistics """
import math
import numpy
import scipy.stats


class Integrator(object):
    """ Helper class to itegrate data """
    def __init__(self, x):
        self._x0 = x
        self._total = numpy.zeros_like(x)
        self._elapsed = 0.0

    def add(self, x, y):
        """ Add data to the accumulator """
        delta_x = x - self._x0
        self._x0 = x

        self._total = y * delta_x + self._total
        self._elapsed += delta_x

    @property
    def total(self):
        """ get the total value """
        return self._total

    @property
    def elapsed(self):
        """ Sum of segments """
        return self._elapsed

def calculate_stats(v):
    res = {}

    if len(v) == 1:
        return {'avg': float(v[0]), 'cnt': 1}

    if len(v) > 0:
        (v_n, (v_min, v_max), v_avg, v_var, v_skew, v_kurt) = scipy.stats.describe(v)

        if v_min == v_max:
            return {'avg': float(v[0]), 'cnt': len(v)}

        res['max'] = float(v_max)
        res['avg'] = v_avg
        res['krt'] = v_kurt
        res['min'] = float(v_min)
        res['skw'] = v_skew
        res['cnt'] = len(v)
        if res['min'] == res['max']:
            res['med'] = res['min']
            res['std'] = 0.0
        else:
            res['med'] = float(numpy.median(v, axis=0))
            if len(v) > 2:
                res['std'] = scipy.stats.tstd(v)

        if v_avg > 0:
            res['cov'] = math.sqrt(v_var) / v_avg

    return res


class RollingStats(object):
    """ Uses Welford's method [1] to compute the mean and stddev of
        a series for data without storing all datapoints.
    
        Data should be added to the class instance using the append()
        function and the summary statistics can be accessed using get()

        [1] B. P. Welford (1962) Note on a Method for Calculating
        Corrected Sums of Squares and Products, Technometrics,
        4:3, 419-420, DOI: 10.1080/00401706.1962.10490022
    """
    def __init__(self):
        self._count = 0

    def append(self, x):
        """ append a datum. """ 
        self._count += 1

        if self._count == 1:
            self.m = x
            self.last_m = x
            self.last_s = 0.0
            self.min = x
            self.max = x
        else:
            self.m = self.last_m + (x - self.last_m) / self._count
            self.s = self.last_s + (x - self.last_m) * (x - self.m)

            self.last_m = self.m
            self.last_s = self.s

            self.min = numpy.minimum(self.min, x)
            self.max = numpy.maximum(self.max, x)

    def get(self):
        """ return a dict with the various statistics """
        return {'avg': self.mean(), 'min': self.min, 'max': self.max, 'cnt': self._count, 'std': math.sqrt(self.variance())}

    def mean(self):
        """ return the mean """
        if self._count > 0:
            return self.m
        return 0.0

    def count(self):
        """ returns the number of data points that have been processed """
        return self._count

    def variance(self):
        """ Return the variance of the data """
        if self._count > 1:
            return self.s / (self._count - 1)
        return 0.0

    def __str__(self):
        return str(self.get())

def test():
    """ test """
    indata = [0.1, 0.2, 0.3, 0.4, 0.4, 0.5, 0.1, 0.4]

    stats = RollingStats()
    for i in indata:
        stats.append(i)

    print stats.get()
    print calculate_stats(indata)

if __name__ == "__main__":
    test()

