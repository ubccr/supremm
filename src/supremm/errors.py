#!/usr/bin/env python3
""" processing error class is defined so that common errors can be assigned short codes """

class ProcessingError(object):
    """ Container class for processing errors """
    RAW_COUNTER_UNAVAILABLE = 1
    JOB_TOO_SHORT = 2
    INSUFFICIENT_DATA = 3
    INSUFFICIENT_HOSTDATA = 4
    CPUSET_UNKNOWN = 5
    PMDA_RESTARTED_DURING_JOB = 6
    INDOMS_CHANGED_DURING_JOB = 7
    PMLOGEXTRACT_ERROR = 8
    PARALLEL_TOO_SHORT = 9
    INVALID_NODECOUNT = 10
    JOB_TOO_BIG = 11
    TIME_TOO_SHORT = 12
    TIME_TOO_LONG = 13
    UNKNOWN_CANNOT_PROCESS = 14
    NO_ARCHIVES = 15
    SUMMARIZATION_ERROR = 16
    RAW_ARCHIVES = 17
    JOB_TOO_MANY_NODEHOURS = 18
    MAX_ERROR = 19
    PROMETHEUS_ERROR = 20

    def __init__(self, err_id):
        self._id = err_id

    def __str__(self):
        names = {
            ProcessingError.RAW_COUNTER_UNAVAILABLE: "Required raw metrics not available.",
            ProcessingError.JOB_TOO_SHORT: "The job was too short.",
            ProcessingError.INSUFFICIENT_DATA: "There were too few datapoints.",
            ProcessingError.INSUFFICIENT_HOSTDATA: "Not all of the hosts had raw metrics available",
            ProcessingError.CPUSET_UNKNOWN: "The cpuset that was assigned to the job is unavailable",
            ProcessingError.PMDA_RESTARTED_DURING_JOB: "The PMDA restarted during the job",
            ProcessingError.INDOMS_CHANGED_DURING_JOB: "The instance domains for required metrics changed during the job",
            ProcessingError.PMLOGEXTRACT_ERROR: "Generic failure in the pmlogextract step",
            ProcessingError.PARALLEL_TOO_SHORT: "Parallel job ran for too short of a time",
            ProcessingError.INVALID_NODECOUNT: "Fewer than 1 node reported for this job",
            ProcessingError.JOB_TOO_BIG: "Processing skipped due to large node count in job",
            ProcessingError.TIME_TOO_SHORT: "Job ran for too short of a time to provide enough performance data",
            ProcessingError.TIME_TOO_LONG: "Job consumed an impossible amount of walltime",
            ProcessingError.UNKNOWN_CANNOT_PROCESS: "Job cannot be summarized for unknown reason",
            ProcessingError.NO_ARCHIVES: "None of the nodes in the job have pcp archives",
            ProcessingError.SUMMARIZATION_ERROR: "There were enough archives to try summarization, but too few archives were successfully processed",
            ProcessingError.RAW_ARCHIVES: "Not enough raw archives to try pmlogextract",
            ProcessingError.JOB_TOO_MANY_NODEHOURS: "Total job node hours exceeded threshold",
            ProcessingError.PROMETHEUS_ERROR: "An error occurred with the Prometheus server during summarization"
        }
        return names[self._id]

    @staticmethod
    def doc():
        """ Returns a dict containing the documentation for all supported errors """
        docs = {}
        for i in range(1, ProcessingError.MAX_ERROR):
            docs[i] = str(ProcessingError(i))

        return docs

    def get(self):
        """ get """
        return self._id

class NotApplicableError(Exception):
    """ Used by plugins to indicate that their analysis is not avaiable for
        the HPC job. For example, if a plugin implements a resource-manager-specific
        analysis and the job was not run on the supported resource manager. """
    pass

if __name__ == "__main__":
    print(ProcessingError.doc())
