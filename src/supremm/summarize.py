""" Definition of the summarize API """

from abc import ABCMeta, abstractmethod


class Summarize(object):
    """ Abstract base class describing the job summarization interface.
        Currently only interfaces with PCP archives and is subject to change.
    """
    __metaclass__ = ABCMeta

    def __init__(self, preprocessors, analytics, job, config, fail_fast=False):
        self.preprocs = preprocessors
        self.alltimestamps = [x for x in analytics if x.mode in ("all", "timeseries")]
        self.firstlast = [x for x in analytics if x.mode == "firstlast"]
        self.errors = {}
        self.job = job
        self.start = time.time()
        self.fail_fast = fail_fast

    def get(self):
        """ Return a dict with the summary information """
        output = {}
        timeseries = {}

        je = self.job.get_errors()
        if len(je) > 0:
            self.adderror("job", je)

        if self.job.nodecount > 0:
            for analytic in self.alltimestamps:
                if analytic.status != "uninitialized":
                    if analytic.mode == "all":
                        output[analytic.name] = analytic.results()
                    if analytic.mode == "timeseries":
                        timeseries[analytic.name] = analytic.results()
            for analytic in self.firstlast:
                if analytic.status != "uninitialized":
                    output[analytic.name] = analytic.results()
                    
        output['summarization'] = {
            "version": VERSION,
            "elapsed": time.time() - self.start,
            "created": time.time(),
            "srcdir": self.job.jobdir,
            "complete": self.complete()}

        output['created'] = datetime.datetime.utcnow()

        output['acct'] = self.job.acct
        output['acct']['id'] = self.job.job_id

        if len(timeseries) > 0:
            timeseries['hosts'] = dict((str(idx), name) for name, idx, _ in self.job.nodearchives())
            timeseries['version'] = TIMESERIES_VERSION
            output['timeseries'] = timeseries

        for preproc in self.preprocs:
            result = preproc.results()
            if result != None:
                output.update(result)

        for source, data in self.job.data().items():
            if 'errors' in data:
                self.adderror(source, str(data['errors']))

        if len(self.errors) > 0:
            output['errors'] = {}
            for k, v in self.errors.items():
                output['errors'][k] = list(v)

        return output

    def adderror(self, category, errormsg):
        """ All errors reported with this function show up in the job summary """
        if category not in self.errors:
            self.errors[category] = set()
        if isinstance(errormsg, list):
            self.errors[category].update(set(errormsg))
        else:
            self.errors[category].add(errormsg)

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
