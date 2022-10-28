import sys
import os
import json
import time
import logging
import requests
import inspect
import datetime
import traceback
import urllib.parse as urlparse
from collections import OrderedDict

import requests
import numpy as np

from prominterface import PromClient, Context, formatforplugin, formatforpreproc # Use local import for debugging, testing
from supremm.plugin import loadpreprocessors, loadplugins, NodeMetadata
from supremm.summarize import Summarize


VERSION = "2.0.0"
TIMESERIES_VERSION = 4

MAX_CHUNK = 6 #hours

def load_translation():
    """
    Update mapping of available Prometheus metrics
    with corresponding PCP metric names.
    """        
    # Load mapping
    prom2pcp = {}
    version = "v5"
    file = "mapping/%s.json" % (version)
    file_path = os.path.abspath(file)
    with open(file_path, "r") as f:
        prom2pcp = json.load(f)

    logging.debug("Available metric mapping(s): \n{}".format(json.dumps(prom2pcp, indent=2)))
    return prom2pcp        

class NodeMeta(NodeMetadata):
    """ container for node metadata """
    def __init__(self, nodename, idx):
        self._nodename = nodename
        self._nodeidx = idx

    nodename = property(lambda self: self._nodename)
    nodeindex = property(lambda self: self._nodeidx)

class PromSummarize(Summarize):
    def __init__(self, preprocessors, analytics, job, config, chunk):
        super(PromSummarize, self).__init__(preprocessors, analytics, job, config)
        self.start = time.time()

        # Establish connection with server:
        self.url = "http://172.22.0.216:9090"
        self.client = PromClient(url=self.url)
        self.chunk_size = chunk

        # Translation Prom -> PCP metric names
        self.valid_metrics = load_translation()
        self.nodes_processed = 0

        # TODO use config query to parse yaml scrape interval config
        self.timestep = "30s"

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

        #TODO replace job.nodearchives
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

    def logerror(self, nodename, analyticname, error):
        """
        Store the detail of processing errors
        """
        #if analyticname != "gpu":
        logging.debug("Processing exception: %s", analyticname)
        self.adderror(" ", "{0} {1} {2}".format(nodename, analyticname, error))

    def complete(self):
        return True
        
    def good_enough(self):
        return True

    def process(self):
        """ Main entry point. All nodes are processed. """
        success = 0

        for nodename in self.job.nodenames():
            idx = self.nodes_processed
            mdata = NodeMeta(nodename, idx)
            try:
                self.processnode(mdata)
                self.nodes_processed += 1

            except Exception as exc:
                success -= 1
                self.adderror("node", "Exception {0} for node: {1}".format(exc, mdata.nodename))
                if self.fail_fast:
                    raise

        return success == 0

    def processnode(self, mdata):
        for preproc in self.preprocs:
            reqMetrics = self.metric_mapping(preproc.requiredMetrics)
            if False == reqMetrics:
                logging.warning("Skipping %s (%s). No metric mapping available." % (type(preproc).__name__, preproc.name))
                continue
            self.processforpreproc(mdata, preproc, reqMetrics)

        for analytic in self.alltimestamps:
            reqMetrics = self.metric_mapping(analytic.requiredMetrics)
            if False == reqMetrics:
                logging.warning("Skipping %s (%s). No metric mapping available." % (type(analytic).__name__, analytic.name))
                continue
            self.processforanalytic(analytic, mdata, reqMetrics)

        for analytic in self.firstlast:
            reqMetrics = self.metric_mapping(analytic.requiredMetrics)
            if False == reqMetrics:
                logging.warning("Skipping %s (%s). No metric mapping available." % (type(analytic).__name__, analytic.name))
                continue
            self.processfirstlast(analytic, mdata, reqMetrics)

    def processforpreproc(self, mdata, preproc, reqMetrics):
        # Serialize as unix timestamp here for prometheus metadata queries.
        # Later on in this function the self.job.(start/end)_datetime python objects
        # are used for chunking. Maybe determine chunks first (if necessary)? Include job's (start, end) then list of chunked (start, end)
        start_ts, end_ts = self.job.start_datetime.timestamp(), self.job.end_datetime.timestamp()
        preproc.hoststart(mdata.nodename)
        logging.debug("Processing %s (%s)" % (type(preproc).__name__, preproc.name))

        ctx = Context()
        descriptions = []
        for m in reqMetrics.values():
            if preproc.name == "procprom":
                cgroup = self.client.cgroup_info(self.job.acct['uid'], self.job.job_id, start_ts, end_ts)
                metric = m['metric'] % (cgroup, mdata.nodename)
            else:
                metric = m['metric'] % mdata.nodename

            base = metric.split()[0]

            # Check if timeseries is available
            available = self.client.timeseries_meta(start_ts, end_ts, base)
            if not available:
                logging.warning("Skipping %s (%s). No data available." % (type(preproc).__name__, preproc.name))
                preproc.hostend()
                return

            # Get metric label -> description from metric and label
            label = m['label']
            description = self.client.label_val_meta(start_ts, end_ts, base, m['label'], 'preprocessor')
            descriptions.append(description)
            ctx.add_metric(base, label)

        for start, end in chunk_timerange(self.job.start_datetime, self.job.end_datetime, self.chunk_size):
            rdata = OrderedDict()

            for m in reqMetrics.values():
                if preproc.name == "procprom":
                    cgroup = self.client.cgroup_info(self.job.acct['uid'], self.job.job_id, start_ts, end_ts)
                    metric = m['metric'] % (cgroup, mdata.nodename)
                else:
                    metric = m['metric'] % mdata.nodename

                base = metric.split()[0]
                query = self.client.query_range(metric, start, end)
                rdata.update({base : query})

            if False == self.runpreproccall(preproc, mdata, rdata, descriptions, ctx):        
                break

            ctx.reset()

        preproc.status = "complete"
        preproc.hostend()

    def processfirstlast(self, analytic, mdata, reqMetrics):
        start, end = self.job.start_datetime.timestamp(), self.job.end_datetime.timestamp()
        logging.debug("Processing %s (%s)" % (type(analytic).__name__, analytic.name))

        metrics = []
        descriptions = []
        for m in reqMetrics.values():
            metric = m['metric'] % mdata.nodename
            base = metric.split()[0]

            # Check if timeseries is available
            available = self.client.timeseries_meta(start, end, base)
            if not available:
                logging.warning("Skipping %s (%s). No data available." % (type(analytic).__name__, analytic.name))
                analytic.status = "failure"
                return

            # Get metric label -> description from metric and label
            description = self.client.label_val_meta(start, end, base, m['label'], 'plugin')
            metrics.append(metric)
            descriptions.append(description)

        for ts in (start, end):
            rdata = [self.client.query(m, ts) for m in metrics]
            self.runcallback(analytic, mdata, rdata, descriptions)
 
        analytic.status = "complete"

    def processforanalytic(self, analytic, mdata, reqMetrics):
        start, end = self.job.start_datetime.timestamp(), self.job.end_datetime.timestamp()
        logging.debug("Processing %s (%s)" % (type(analytic).__name__, analytic.name))

        ctx = Context()
        descriptions = []
        for m in reqMetrics.values():
            metric = m['metric'] % mdata.nodename
            label = m['label']
            base = metric.split()[0]

            # Check if timeseries is available
            available = self.client.timeseries_meta(start, end, base)
            if not available:
                logging.warning("Skipping %s (%s). No data available." % (type(analytic).__name__, analytic.name))
                analytic.status = "failure"
                return

            # Get metric label -> description from metric and label
            label = m['label']
            description = self.client.label_val_meta(start, end, base, label, 'plugin')
            descriptions.append(description)
            ctx.add_metric(base, label)            

        for start, end in chunk_timerange(self.job.start_datetime, self.job.end_datetime, self.chunk_size): 
            rdata = OrderedDict()

            for m in reqMetrics.values():
                metric = m['metric'] % mdata.nodename
                base = metric.split()[0]

                try:
                    query = self.client.query_range(metric, start, end)
                except Exception as e:
                    print("Exception with query: {}" % e)

                rdata.update({base : query})
            
            try:
                if False == self.runcallback(analytic, mdata, rdata, descriptions, ctx):        
                    continue

            except Exception as exp:
                logging.warning("%s (%s) raised exception %s", type(analytic).__name__, analytic.name, str(exp))
                analytic.status = "failure"
                raise exp

            ctx.reset()

        analytic.status = "complete"

    def runpreproccall(self, preproc, mdata, rdata, descriptions, ctx):
        """ Call the pre-processor data processing function """

        for ts, vals in formatforpreproc(rdata, ctx):
            try:
                retval = preproc.process(ts, vals, descriptions)
                if retval:
                    break
            except Exception as e:
                logging.exception("%s %s @ %s", self.job.job_id, preproc.name, ts)
                self.logerror(mdata.nodename, preproc.name, str(e))
                return False

        return False

    def runcallback(self, analytic, mdata, rdata, description, ctx=None):
        """ Call the plugin data processing function """

        for ts, vals in formatforplugin(rdata, ctx):
            try:
                retval = analytic.process(mdata, ts, vals, description)
                if not retval:
                    break
            except Exception as e:
                logging.exception("%s %s @ %s", self.job.job_id, analytic.name, ts)
                self.logerror(mdata.nodename, analytic.name, str(e))
                return False
                
        return False

    def metric_mapping(self, reqMetrics):
        """
        Recursively checks if a mapping is available from a given metrics list or list of lists.

        params: reqMetrics - list of metrics from preproc/plugin 
        return: OrderedDict of the mapping from PCP metric names to Prometheus metric names
                False if a mapping is not present.
        """ 
        if isinstance(reqMetrics[0], list):
            for metriclist in reqMetrics:
                mapping = self.metric_mapping(metriclist)
                if mapping:
                    return mapping
            return False
        else:
            mapping = OrderedDict.fromkeys(reqMetrics, None)
            for k in mapping.keys():
                try:
                    mapping[k] = self.valid_metrics[k]
                    # Prometheus-only plugins/preprocs
                    if k[:5] == "prom:":
                        full_metric = mapping[k]['metric']
                        if k[5:] not in full_metric:
                            mapping[k]['metric'] = k[5:] + full_metric
                        else:
                            mapping[k]['metric'] = full_metric
                except KeyError:
                    logging.debug("Mapping unavailable for metric: %s", k)
                    return False
            return mapping

def chunk_timerange(job_start, job_end, chunk_size):
    """
    Generator function to return chunked time ranges for a job of arbitrary length.
    This is necessary due to Prometheus's hard-coded limit of 11,000 data points:
    https://github.com/prometheus/prometheus/blob/30af47535d4d7c0a7566df78e63e77515ba26863/web/api/v1/api.go#L202

    params: job_start, job_end - Python datetime objects of a job's start and end times
    yield: chunk_start, chunk_end - Python datetime objects of a given chunk's start and end times

    'chunk_end' will be the 'job_end' for the final chunk less than the maximum specified chunk size.
    """

    chunk_start = job_start
    while True:
        chunk_end = chunk_start + datetime.timedelta(hours=chunk_size)
        if chunk_end > job_end:
            yield chunk_start.timestamp(), job_end.timestamp()
            break
        yield chunk_start.timestamp(), chunk_end.timestamp()
        chunk_start = chunk_end
