""" Definition of the summarize API """

from abc import ABCMeta, abstractmethod


class Summarize(object):
    """ Abstract base class describing the job summarization interface.
        Currently only interfaces with PCP archives and is subject to change.
    """
    __metaclass__ = ABCMeta

    def __init__(self):
        pass
        #self._config

    @abstractmethod
    def adderror(self, category, errormsg):
        """ All errors reported with this function show up in the job summary """
        pass

    @abstractmethod
    def process(self):
        """ Main entry point. All archives are processed """
        pass

    @abstractmethod
    def complete(self):
        """ A job is complete if archives exist for all assigned nodes and they have
            been processed sucessfullly
        """
        pass

    @abstractmethod
    def good_enough(self):
        """ A job is good_enough if archives for 95% of nodes have
            been processed sucessfullly
        """
        pass

    @abstractmethod
    def get(self):
        """ Return a dict with the summary information """
        pass

    @abstractmethod
    def runcallback(self, analytic, result, mtypes, ctx, mdata, metric_id_array):
        """ get the data and call the analytic """
        pass

    @abstractmethod
    def runpreproccall(self, preproc, result, mtypes, ctx, mdata, metric_id_array):
        """ Call the pre-processor data processing function """
        pass

    @abstractmethod
    def processforpreproc(self, ctx, mdata, preproc):
        """ fetch the data from the archive, reformat as a python data structure
        and call the analytic process function """

    @abstractmethod
    def processforanalytic(self, ctx, mdata, analytic):
        """ fetch the data from the archive, reformat as a python data structure
        and call the analytic process function """
        pass

    @abstractmethod
    def logerror(self, archive, analyticname, pmerrorcode):
        """
        Store the detail of archive processing errors
        """
        pass

    @abstractmethod
    def processfirstlast(self, ctx, mdata, analytic):
        """ fetch the data from the archive, reformat as a python data structure
        and call the analytic process function """
        pass

    @abstractmethod
    def processarchive(self, nodename, nodeidx, archive):
        """ process the archive """
        pass
