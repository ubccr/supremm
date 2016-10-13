import unittest
import numpy
from supremm.rangechange import RangeChange

class MockConfig(object):
    def __init__(self, settings):
        self.settings = settings

    def getsection(self, sectionname):
        return dict(self.settings[sectionname])


class TestRangeChange(unittest.TestCase):

    def test_normalization(self):

        config = MockConfig({"normalization": {"perfevent.hwcounters.CPU_CLK_UNHALTED.value": {"range": 48}}})

        r = RangeChange(config)

        r.set_fetched_metrics(["perfevent.hwcounters.CPU_CLK_UNHALTED.value", "something.else", "perfevent.hwcounters.RETIRED_INSTRUCTIONS.value"])

        self.assertFalse(r.passthrough)

        data = []
        val = numpy.power([2,2,2], 48) - numpy.array([1,2,3])
        data.append(val)
        val = val - numpy.array([3,3,3])
        data.append(val)
        val = val - numpy.array([3,3,3])
        data.append(val)

        r.normalise_data(1.000, data)

        self.assertTrue( numpy.all(data[0] ==  numpy.power([2,2,2], 48) - numpy.array([1,2,3]) ))
        self.assertTrue( numpy.all(data[1] ==  numpy.power([2,2,2], 48) - numpy.array([4,5,6]) ))
        self.assertTrue( numpy.all(data[2] ==  numpy.power([2,2,2], 48) - numpy.array([7,8,9]) ))

        d2 = []
        d2.append( (data[0] + numpy.array([10,10,10])) % numpy.power(2,48))
        d2.append(numpy.array([40,50,60]))
        d2.append(numpy.array([70,80,90]))

        r.normalise_data(2.000, d2)

        delta = d2[0] - data[0]

        self.assertTrue( numpy.all(delta == numpy.array([10,10,10])))
        self.assertTrue( numpy.all(d2[1] == numpy.array([40,50,60])))
        self.assertTrue( numpy.all(d2[2] == numpy.array([70,80,90])))


    def test_passthrough(self):

        config = MockConfig({"normalization": {"perfevent.hwcounters.CPU_CLK_UNHALTED.value": {"range": 48}}})

        r = RangeChange(config)

        r.set_fetched_metrics(["kernel.percpu.cpu.user", "kernel.percpu.cpu.system"])
        self.assertTrue(r.passthrough)

        data = [numpy.array([234,23423,234,23423,23423]), numpy.array([856,5698,789,127,90780])]

        r.normalise_data(1.000, data)

        self.assertTrue(numpy.all(data[0] == numpy.array([234,23423,234,23423,23423])))
        self.assertTrue(numpy.all(data[1] == numpy.array([856,5698,789,127,90780])))

    def test_missingconfig(self):

        config = MockConfig({})
        r = RangeChange(config)

        r.set_fetched_metrics(["kernel.percpu.cpu.user", "kernel.percpu.cpu.system"])

        data = [numpy.array([234,23423,234,23423,23423]), numpy.array([856,5698,789,127,90780])]

        r.normalise_data(1.000, data)

        self.assertTrue(numpy.all(data[0] == numpy.array([234,23423,234,23423,23423])))
        self.assertTrue(numpy.all(data[1] == numpy.array([856,5698,789,127,90780])))

if __name__ == '__main__':
    unittest.main()
