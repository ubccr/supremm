from supremm.plugin import PreProcessor


class MockPreprocessor(PreProcessor):
    name = property(lambda self: "test_preproc")
    mode = property(lambda self: "timeseries")
    requiredMetrics = property(lambda self: ["kernel.percpu.cpu.user"])
    optionalMetrics = property(lambda self: [])
    derivedMetrics = property(lambda self: [])

    def __init__(self, job):
        super(MockPreprocessor, self).__init__(job)
        print job

    def hoststart(self, hostname):
        print hostname

    def process(self, timestamp, data, description):
        print timestamp
        print data
        print description
        return False

    def hostend(self):
        pass

    def results(self):
        return {}
