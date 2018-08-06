from supremm.plugin import Plugin


class InitThrowingPlugin(Plugin):
    name = property(lambda self: "init_throwing_plugin")
    mode = property(lambda self: "timeseries")
    requiredMetrics = property(lambda self: ["hinv.ncpu", "gpfs.fsios.read_bytes"])
    optionalMetrics = property(lambda self: [])
    derivedMetrics = property(lambda self: [])

    def __init__(self, job):
        super(InitThrowingPlugin, self).__init__(job)
        raise Exception("Exception in __init__")

    def process(self, nodemeta, timestamp, data, description):
        pass

    def results(self):
        pass


class ProcessThrowingPlugin(Plugin):
    name = property(lambda self: "process_throwing_plugin")
    mode = property(lambda self: "timeseries")
    requiredMetrics = property(lambda self: ["hinv.ncpu", "gpfs.fsios.read_bytes"])
    optionalMetrics = property(lambda self: [])
    derivedMetrics = property(lambda self: [])

    def __init__(self, job):
        super(ProcessThrowingPlugin, self).__init__(job)

    def process(self, nodemeta, timestamp, data, description):
        raise Exception("Exception in process")

    def results(self):
        pass


class ResultsThrowingPlugin(Plugin):
    name = property(lambda self: "results_throwing_plugin")
    mode = property(lambda self: "timeseries")
    requiredMetrics = property(lambda self: ["hinv.ncpu", "gpfs.fsios.read_bytes"])
    optionalMetrics = property(lambda self: [])
    derivedMetrics = property(lambda self: [])

    def __init__(self, job):
        super(ResultsThrowingPlugin, self).__init__(job)

    def process(self, nodemeta, timestamp, data, description):
        return False

    def results(self):
        raise Exception("Exception in results")
