#!/usr/bin/env python
""" processing error class is defined so that common errors can be assigned short codes """

class ProcessingError(object):
    """ Container class for processing errors """
    RAW_COUNTER_UNAVAILABLE = 1
    JOB_TOO_SHORT = 2
    INSUFFICIENT_DATA = 3
    INSUFFICIENT_HOSTDATA = 4
    CPUSET_UNKNOWN = 5
    PMDA_RESTARTED_DURING_JOB = 6
    MAX_ERROR = 7

    def __init__(self, err_id):
        self._id = err_id

    def __str__(self):
        names = {
            ProcessingError.RAW_COUNTER_UNAVAILABLE: "Required raw metrics not available.",
            ProcessingError.JOB_TOO_SHORT: "The job was too short.",
            ProcessingError.INSUFFICIENT_DATA: "There were too few datapoints.",
            ProcessingError.INSUFFICIENT_HOSTDATA: "Not all of the hosts had raw metrics available",
            ProcessingError.CPUSET_UNKNOWN: "The cpuset that was assigned to the job is unavailable",
            ProcessingError.PMDA_RESTARTED_DURING_JOB: "The PMDA restarted during the job"
        }
        return names[self._id]

    @staticmethod
    def doc():
        """ Returns a dict containing the documentation for all supported errors """
        docs = {}
        for i in xrange(1, ProcessingError.MAX_ERROR):
            docs[i] = str(ProcessingError(i))

        return docs

    def get(self):
        """ get """
        return self._id

if __name__ == "__main__":
    print ProcessingError.doc()
