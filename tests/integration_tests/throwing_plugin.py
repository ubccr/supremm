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


class ThrowingPlugin(Plugin):
    name = property(lambda self: "throwing_plugin")
    mode = property(lambda self: "timeseries")
    requiredMetrics = property(lambda self: ["hinv.ncpu", "gpfs.fsios.read_bytes"])
    optionalMetrics = property(lambda self: [])
    derivedMetrics = property(lambda self: [])

    def __init__(self, job):
        super(ThrowingPlugin, self).__init__(job)

    def process(self, nodemeta, timestamp, data, description):
        raise Exception("Exception in process")

    def results(self):
        raise Exception("Exception in results")
