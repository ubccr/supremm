from supremm.plugin import PreProcessor


class MockPreprocessor(PreProcessor):
    name = property(lambda self: "test_preproc")
    mode = property(lambda self: "timeseries")
    requiredMetrics = property(lambda self: ["hinv.ncpu", "gpfs.fsios.read_bytes"])
    optionalMetrics = property(lambda self: [])
    derivedMetrics = property(lambda self: [])

    def __init__(self, job):
        super(MockPreprocessor, self).__init__(job)
        self.process_called = False  # make sure our test actually runs (can get skipped if things arent set up correctly)

    def hoststart(self, hostname):
        pass

    def process(self, timestamp, data, description):
        self.process_called = True
        print(timestamp)
        print(data)
        print(description)
        assert len(data) == 2
        assert len(description) == 2
        assert description[0] == {}  # hinv.ncpu has no instances but we should get an empty dict

    def hostend(self):
        pass

    def results(self):
        assert self.process_called
        return {}
