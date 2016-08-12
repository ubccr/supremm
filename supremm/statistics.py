import scipy.stats
import math
import numpy


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

    def __init__(self):
        self._count = 0
        self.m = 0
        self.last_m = 0
        self.min = 0
        self.max = 0
        self.s = 0
        self.last_s = 0

    def append(self, x):
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

    def __add__(self, other):
        """
        RollingStats of the union of the data involved with self and other
        self and other do not overlap
        """
        ret = RollingStats()
        ret._count = self._count + other._count
        ret.m = ((self.m*self._count) + (other.m*other._count)) / ret._count
        ret.last_m = ret.m
        n1 = self._count
        n2 = other._count
        m1 = self.m
        m2 = other.m
        # Based on http://cas.ee.ic.ac.uk/people/dt10/research/thomas-08-sample-mean-and-variance.pdf (page 4)
        # Note ret.s is going to be an estimate
        ret.s = self.s + other.s + ( ( n2 / (n1*(n1+n2)) ) * ((n1*m2-n2*m1)**2))
        ret.last_s = ret.s
        ret.min = numpy.minimum(self.min, other.min)
        ret.max = numpy.maximum(self.max, other.max)
        return ret

    def __iadd__(self, other):
        ret = self + other
        return ret

    def __sub__(self, other):
        """
        RollingStats of the data in self and not in other
        assumes self is a superset of other
        """
        ret = RollingStats()
        ret._count = self._count # Not sure if that's right - assumes self is superset of other
        ret.m = ((self.m*self._count) - (other.m*other._count)) / ret._count
        ret.last_m = ret.m
        n1 = self._count
        n2 = other._count
        m1 = self.m
        m2 = other.m
        # Based formula mentioned in http://cas.ee.ic.ac.uk/people/dt10/research/thomas-08-sample-mean-and-variance.pdf (page 4)
        # Note ret.s is going to be an estimate
        ret.s = self.s + other.s + ( ( n2 / (n1*(n1+n2)) ) * ((n1*m2-n2*m1)**2))
        ret.last_s = ret.s
        # Probably a better way to do this
        minim = numpy.minimum(self.min, other.min)
        if minim == other.min: # Min might be in set of data we're removing, so then don't know what should be min
            ret.min = None
        else:
            ret.min = numpy.minimum(self.min, other.min)
        maxim = numpy.maximum(self.max, other.max)
        if maxim == other.max: # Same with max
            ret.max = None
        else:
            ret.max = numpy.maximum(self.max, other.max)
        return ret

    def sum(self):
        return self._count * self.mean()

    def get(self):
        """ return a dict with the various statistics """
        return {'avg': self.mean(), 'min': self.min, 'max': self.max, 'cnt': self._count, 'std': math.sqrt(self.variance())}

    def mean(self):
        """ return the mean """
        if self._count > 0:
            return self.m
        return 0.0

    def count(self):
        """ Take a wild guess at what this function does """
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

