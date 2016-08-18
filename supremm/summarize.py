#!/usr/bin/env python
""" Summarize module """

from ctypes import c_uint
from pcp import pmapi
import cpmapi as c_pmapi
from supremm.pcpfast import pcpfast
import time
import logging
import traceback
from supremm.plugin import NodeMetadata
from profile import Profile

import numpy
import copy

VERSION = "1.0.6"
TIMESERIES_VERSION = 4


class ArchiveMeta(NodeMetadata):
    """ container for achive metadata """
    def __init__(self, nodename, nodeidx, archivedata):
        self._nodename = nodename
        self._nodeidx = nodeidx
        self._archivedata = archivedata

    nodename = property(lambda self: self._nodename)
    nodeindex = property(lambda self: self._nodeidx)
    archive = property(lambda self: self._archivedata)

class Summarize(object):
    """
    Summarize class is responsible for iteracting with the pmapi python code
    and managing the calls to the various analytics to process the data
    """

    def __init__(self, preprocessors, analytics, job):

        self.preprocs = preprocessors
        self.alltimestamps = [x for x in analytics if x.mode in ("all", "timeseries")]
        self.firstlast = [x for x in analytics if x.mode == "firstlast"]
        self.errors = {}
        self.job = job
        self.start = time.time()
        self.archives_processed = 0
        self._profile = False

        self.indomcache = None

    def activate_profile(self):
        self._profile = True
        self.profile_dict = Profile()

    def adderror(self, category, errormsg):
        """ All errors reported with this function show up in the job summary """
        if category not in self.errors:
            self.errors[category] = set()
        if isinstance(errormsg, list):
            self.errors[category].update(set(errormsg))
        else:
            self.errors[category].add(errormsg)

    def process(self):
        """ Main entry point. All archives are processed """
        success = 0
        self.archives_processed = 0

        for nodename, nodeidx, archive in self.job.nodearchives():
            try:
                self.processarchive(nodename, nodeidx, archive)
                self.archives_processed += 1
            except pmapi.pmErr as exc:
                success -= 1
                self.adderror("archive", "{0}: pmapi.pmErr: {1}".format(archive, exc.message()))
            except Exception as exc:
                success -= 1
                self.adderror("archive", "{0}: Exception: {1}. {2}".format(archive, str(exc), traceback.format_exc()))

        return success == 0

    def complete(self):
        """ A job is complete if archives exist for all assigned nodes and they have
            been processed sucessfullly
        """
        return self.job.nodecount == self.archives_processed

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
                        if self._profile:
                            starttime = time.time()
                            output[analytic.name] = analytic.results()
                            delta_time = time.time() - starttime
                            self.profile_dict.add(analytic.name, 'results', delta_time)
                        else:
                            output[analytic.name] = analytic.results()
                    if analytic.mode == "timeseries":
                        if self._profile:
                            starttime = time.time()
                            timeseries[analytic.name] = analytic.results()
                            delta_time = time.time() - starttime
                            self.profile_dict.add(analytic.name, 'results', delta_time)
                        else:
                            timeseries[analytic.name] = analytic.results()
            for analytic in self.firstlast:
                if analytic.status != "uninitialized":
                    if self._profile:
                        starttime = time.time()
                        output[analytic.name] = analytic.results()
                        delta_time = time.time() - starttime
                        self.profile_dict.add(analytic.name, 'results', delta_time)
                    else:
                        output[analytic.name] = analytic.results()


        output['summarization'] = {
            "version": VERSION,
            "elapsed": time.time() - self.start,
            "srcdir": self.job.jobdir,
            "complete": self.complete()}

        output['acct'] = self.job.acct
        output['acct']['id'] = self.job.job_id

        if len(timeseries) > 0:
            timeseries['hosts'] = dict((str(idx), name) for name, idx, _ in self.job.nodearchives())
            timeseries['version'] = TIMESERIES_VERSION
            output['timeseries'] = timeseries

        for preproc in self.preprocs:

            if self._profile:
                starttime = time.time()
                result = preproc.results()
                delta_t = time.time() - starttime
                self.profile_dict.add(preproc.name, 'results', delta_t)
            else:
                result = preproc.results()

            if result != None:
                output.update(result)

        for source, data in self.job.data().iteritems():
            if 'errors' in data:
                self.adderror(source, str(data['errors']))

        if len(self.errors) > 0:
            output['errors'] = {}
            for k, v in self.errors.iteritems():
                output['errors'][k] = list(v)

        return output

    @staticmethod
    def loadrequiredmetrics(context, requiredMetrics):
        """ required metrics are those that must be present for the analytic to be run """
        try:
            required = context.pmLookupName(requiredMetrics)
            return [required[i] for i in xrange(0, len(required))]

        except pmapi.pmErr as e:
            if e.args[0] == c_pmapi.PM_ERR_NAME:
                # Required metric missing - this analytic cannot run on this archive
                return []
            else:
                raise e

    @staticmethod
    def getmetricstofetch(context, analytic):
        """ returns the c_type data structure with the list of metrics requested
            for the analytic """

        metriclist = []

        for derived in analytic.derivedMetrics:
            context.pmRegisterDerived(derived['name'], derived['formula'])
            required = context.pmLookupName(derived['name'])
            metriclist.append(required[0])

        if len(analytic.requiredMetrics) > 0:
            metricOk = False
            if isinstance(analytic.requiredMetrics[0], basestring):
                r = Summarize.loadrequiredmetrics(context, analytic.requiredMetrics)
                if len(r) > 0:
                    metriclist += r
                    metricOk = True
            else:
                for reqarray in analytic.requiredMetrics:
                    r = Summarize.loadrequiredmetrics(context, reqarray)
                    if len(r) > 0:
                        metriclist += r
                        metricOk = True
                        break

            if not metricOk:
                return []

        for optional in analytic.optionalMetrics:
            try:
                opt = context.pmLookupName(optional)
                metriclist.append(opt[0])
            except pmapi.pmErr as e:
                if e.args[0] == c_pmapi.PM_ERR_NAME or e.args[0] == c_pmapi.PM_ERR_NONLEAF:
                    # Optional metrics are allowed to not exist
                    pass
                else:
                    raise e

        metricarray = (c_uint * len(metriclist))()
        for i in xrange(0, len(metriclist)):
            metricarray[i] = metriclist[i]

        return metricarray

    @staticmethod
    def getmetrictypes(context, metric_ids):
        """ returns a list with the datatype of the provided array of metric ids """
        return [context.pmLookupDesc(metric_ids[i]).type for i in xrange(len(metric_ids))]

    @staticmethod
    def pcptypetonumpy(pcptype):
        """ Convert pcp data types to numpy equivalents """
        if pcptype == c_pmapi.PM_TYPE_STRING:
            return object
        return numpy.float

    def runcallback(self, analytic, result, mtypes, ctx, mdata, metric_id_array):
        """ get the data and call the analytic """

        if self.indomcache == None:
            # First time through populate the indom cache
            self.indomcache = self.getindomdict(ctx, metric_id_array)
            if self.indomcache == None:
                # Unable to get indom information
                return False

        data = []
        description = []

        for i in xrange(result.contents.numpmid):
            ninstances = result.contents.get_numval(i)
            if ninstances < 0:
                logging.warning("%s %s ninstances = %s @ %s", mdata.nodename, analytic.name, ninstances, float(result.contents.timestamp))
                self.logerror(mdata.nodename, analytic.name, "get_numval() error")
                return False

            tmp = numpy.empty(ninstances, dtype=self.pcptypetonumpy(mtypes[i]))
            tmpnames = []
            tmpidx = numpy.empty(ninstances, dtype=long)

            for j in xrange(ninstances):
                pcpdata = pcpfast.pcpfastExtractValues(result, i, j, mtypes[i])
                tmp[j] = pcpdata[0]
                if pcpdata[1] > -1:
                    tmpidx[j] = pcpdata[1]
                    if pcpdata[1] not in self.indomcache[i]:
                        # indoms must have changed; rebuild the cache
                        self.indomcache = self.getindomdict(ctx, metric_id_array)
                        if self.indomcache == None:
                            return False
                    if pcpdata[1] not in self.indomcache[i]:
                        # Unable to get indom information for one of the instance domains
                        # Ignore this timestep, but carry on
                        logging.warning("%s %s missing indom @ %s", mdata.nodename, analytic.name, float(result.contents.timestamp))
                        self.logerror(mdata.nodename, analytic.name, "missing indom")
                        return True
                    tmpnames.append(self.indomcache[i][pcpdata[1]])

            data.append(tmp)
            description.append([tmpidx, tmpnames])

        try:
            if self._profile:
                starttime = time.time()
                retval = analytic.process(mdata, float(result.contents.timestamp), data, description)
                delta_t = time.time() - starttime
                self.profile_dict.add(analytic.name, 'process', delta_t)
            else:
                retval = analytic.process(mdata, float(result.contents.timestamp), data, description)
            return retval
        except Exception as e:
            logging.exception("%s %s @ %s", self.job.job_id, analytic.name, float(result.contents.timestamp))
            self.logerror(mdata.nodename, analytic.name, str(e))
            return False

    def runpreproccall(self, preproc, result, mtypes, ctx, mdata, metric_id_array):
        """ Call the pre-processor data processing function """

        description = self.getindomdict(ctx, metric_id_array)
        if description == None:
            return True

        data = []
        for i in xrange(result.contents.numpmid):
            data.append(numpy.array([pcpfast.pcpfastExtractValues(result, i, j, mtypes[i])
                                     for j in xrange(result.contents.get_numval(i))]))

        if self._profile:
            starttime = time.time()
            ret = preproc.process(float(result.contents.timestamp), data, description)
            delta_t = time.time() - starttime
            self.profile_dict.add(preproc.name, 'process', delta_t)
        else:
            ret = preproc.process(float(result.contents.timestamp), data, description)
        return ret

    @staticmethod
    def getindomdict(ctx, metric_id_array):
        """ build a list of dicts that contain the instance domain id to text mappings
            The nth list entry is the nth metric in the metric_id_array
            @throw MissingIndomException if the instance information is not available
        """
        indomdict = []
        for i in xrange(len(metric_id_array)):
            metric_desc = ctx.pmLookupDesc(metric_id_array[i])
            if 4294967295 != pmapi.get_indom(metric_desc):
                try:
                    ivals, inames = ctx.pmGetInDom(metric_desc)
                    if ivals == None:
                        indomdict.append({})
                    else:
                        indomdict.append(dict(zip(ivals, inames)))

                except pmapi.pmErr as exp:
                    if exp.args[0] == c_pmapi.PM_ERR_INDOM:
                        indomdict.append({})
                    elif exp.args[0] == c_pmapi.PM_ERR_INDOM_LOG:
                        return None
                    else:
                        raise exp

            else:
                indomdict.append({})

        return indomdict

    def processforpreproc(self, ctx, mdata, preproc):
        """ fetch the data from the archive, reformat as a python data structure
        and call the analytic process function """

        preproc.hoststart(mdata.nodename)

        metric_id_array = self.getmetricstofetch(ctx, preproc)

        if len(metric_id_array) == 0:
            logging.debug("Skipping %s (%s)" % (type(preproc).__name__, preproc.name))
            preproc.hostend()
            return

        mtypes = self.getmetrictypes(ctx, metric_id_array)

        done = False

        while not done:
            try:
                result = ctx.pmFetch(metric_id_array)

                if False == self.runpreproccall(preproc, result, mtypes, ctx, mdata, metric_id_array):
                    # A return value of false from process indicates the computation
                    # failed and no more data should be sent.
                    done = True

                ctx.pmFreeResult(result)

            except pmapi.pmErr as exp:
                if exp.args[0] == c_pmapi.PM_ERR_EOL:
                    done = True
                else:
                    raise exp

        preproc.status = "complete"
        preproc.hostend()

    def processforanalytic(self, ctx, mdata, analytic):
        """ fetch the data from the archive, reformat as a python data structure
        and call the analytic process function """

        metric_id_array = self.getmetricstofetch(ctx, analytic)

        if len(metric_id_array) == 0:
            logging.debug("Skipping %s (%s)" % (type(analytic).__name__, analytic.name))
            return

        mtypes = self.getmetrictypes(ctx, metric_id_array)
        self.indomcache = None

        done = False

        while not done:
            result = None
            try:
                result = ctx.pmFetch(metric_id_array)

                if False == self.runcallback(analytic, result, mtypes, ctx, mdata, metric_id_array):
                    # A return value of false from process indicates the computation
                    # failed and no more data should be sent.
                    done = True

            except pmapi.pmErr as exp:
                if exp.args[0] == c_pmapi.PM_ERR_EOL:
                    done = True
                else:
                    logging.warning("%s (%s) raised exception %s", type(analytic).__name__, analytic.name, str(exp))
                    analytic.status = "failure"
                    raise exp
            finally:
                if result != None:
                    ctx.pmFreeResult(result)

        analytic.status = "complete"

    def logerror(self, archive, analyticname, pmerrorcode):
        """
        Store the detail of archive processing errors
        """
        #if analyticname != "gpu":
        logging.debug("archive processing exception: %s %s %s", archive, analyticname, pmerrorcode)
        self.adderror("archive", "{0} {1} {2}".format(archive, analyticname, pmerrorcode))

    def processfirstlast(self, ctx, mdata, analytic):
        """ fetch the data from the archive, reformat as a python data structure
        and call the analytic process function """

        metric_id_array = self.getmetricstofetch(ctx, analytic)

        if len(metric_id_array) == 0:
            return

        mtypes = self.getmetrictypes(ctx, metric_id_array)
        self.indomcache = None

        try:
            result = ctx.pmFetch(metric_id_array)
            firstimestamp = copy.deepcopy(result.contents.timestamp)

            if False == self.runcallback(analytic, result, mtypes, ctx, mdata, metric_id_array):
                ctx.pmFreeResult(result)
                return

            ctx.pmFreeResult(result)
            ctx.pmSetMode(c_pmapi.PM_MODE_BACK, ctx.pmGetArchiveEnd(), 0)

            result = ctx.pmFetch(metric_id_array)

            if result.contents.timestamp.tv_sec == firstimestamp.tv_sec and result.contents.timestamp.tv_usec == firstimestamp.tv_usec:
                # This achive must only contain one data point for these metrics
                ctx.pmFreeResult(result)
                return

            if False == self.runcallback(analytic, result, mtypes, ctx, mdata, metric_id_array):
                analytic.status = "failure"
                ctx.pmFreeResult(result)
                return

            analytic.status = "complete"

            ctx.pmFreeResult(result)

        except pmapi.pmErr as e:
            if e.args[0] == c_pmapi.PM_ERR_EOL:
                pass
            else:
                logging.exception("%s", analytic.name)
                raise e

    def processarchive(self, nodename, nodeidx, archive):
        """ process the archive """
        context = pmapi.pmContext(c_pmapi.PM_CONTEXT_ARCHIVE, archive)
        mdata = ArchiveMeta(nodename, nodeidx, context.pmGetArchiveLabel())
        context.pmSetMode(c_pmapi.PM_MODE_FORW, mdata.archive.start, 0)

        # TODO need to benchmark code to see if there is a benefit to interleaving the calls to
        # pmFetch for the different contexts. This version runs all the pmFetches for each analytic
        # in turn.

        basecontext = context.ctx

        for preproc in self.preprocs:
            context._ctx = basecontext
            newctx = context.pmDupContext()
            context._ctx = newctx

            # Optionally keeps track of how long analyitc/preproc takes
            if self._profile:
                starttime = time.time()
                self.processforpreproc(context, mdata, preproc)
                delta_time = time.time() - starttime
                self.profile_dict.add(preproc.name, 'process+extract', delta_time)
            else:
                self.processforpreproc(context, mdata, preproc)

            context.__del__()

        for analytic in self.alltimestamps:
            context._ctx = basecontext
            newctx = context.pmDupContext()
            context._ctx = newctx

            if self._profile:
                starttime = time.time()
                self.processforanalytic(context, mdata, analytic)
                delta_time = time.time() - starttime
                self.profile_dict.add(analytic.name, 'process+extract', delta_time)
            else:
                self.processforanalytic(context, mdata, analytic)

            context.__del__()

        for analytic in self.firstlast:
            context._ctx = basecontext
            newctx = context.pmDupContext()
            context._ctx = newctx

            if self._profile:
                starttime = time.time()
                self.processfirstlast(context, mdata, analytic)
                delta_time = time.time() - starttime
                self.profile_dict.add(analytic.name, 'process+extract', delta_time)
            else:
                self.processfirstlast(context, mdata, analytic)


            context.__del__()

        context._ctx = basecontext
        del context
