import os
import logging
import urllib.parse as urlparse
import math
import json

import numpy as np
import requests
from supremm.config import autodetectconfpath

HTTP_TIMEOUT = 5
MAX_DATA_POINTS = 11000 # Prometheus queries return maximum of 11,000 data points


def load_mapping():
    """
    Update mapping of available Prometheus metrics
    with corresponding PCP metric names.
    """
    # Load mapping
    fpath = autodetectconfpath("mapping.json")
    file = os.path.join(fpath, "mapping.json")
    with open(file, "r") as f:
        mapping = json.load(f)

    for pcp, prom in mapping.items():
        query = mapping[pcp]["metric"]
        mapping[pcp].update({"query": query})

    logging.debug("Loaded metric mapping from {}".format(fpath))
    return mapping

class PromClient():
    def __init__(self, resconf):
        self._url = "{}:{}".format(resconf['prometheus_url'], resconf['prometheus_port'])
        self._step = '30s'

        self._client = requests.Session()
        self._client.mount(self._url, self._client.get_adapter(self._url))
        self._client.headers.update({'Content-Type': 'application/x-www-form-urlencoded',
                                     'Accept': 'application/json'})

    def __str__(self):
        return self._url

    def query(self, query, time):

        params = {
            'query': query,
            'time': time,
        }

        endpoint = "/api/v1/query"
        url = urlparse.urljoin(self._url, endpoint)
 
        r = self._client.get(url, params=params)
        if r.status_code != 200:
            print(str(r.content))
            return None

        return r.json()

    def query_range(self, query, start, end):

        params = {
            'query': query,
            'start': start,
            'end': end,
            'step': self._step
        }

        endpoint = "/api/v1/query_range"
        url = urlparse.urljoin(self._url, endpoint)
        
        r = self._client.get(url, params=params)
        if r.status_code != 200:
            print(r.content)
            return None

        return r.json()

    def timeseries_meta(self, start, end, match):
        # This is basis for checking if timeseries is available
        # Checks if a timeseries or list of timeseries ('match[]') are available
        # without returning any actual data

        params = {
            'match[]': match,
            'start': str(start),
            'end': str(end)
        }

        endpoint = "/api/v1/series"
        urlparse.urlencode(params, doseq=True)
        url = urlparse.urljoin(self._url, endpoint)
        logging.debug('Prometheus QUERY SERIES META, start=%s end=%s', start, end)

        r = self._client.get(url, params=params)
        if r.status_code != 200:
            return False

        # "data" is a list of zero or more timeseries present at the specified times
        data = r.json() 
        return bool(data["data"])


    def label_val(self, start, end, matches, label):
        """
        Queries label values for a corresponding metric.
        """

        params = {
            'match[]': matches,
            'start': str(start),
            'end': str(end)
        }

        urlparse.urlencode(params, doseq=True)
        url = urlparse.urljoin(self._url, "/api/v1/label/%s/values" % label)
        logging.debug('Prometheus QUERY LABEL VALUES, start=%s end=%s', start, end)

        # Query data
        r = self._client.get(url, params=params)
        if r.status_code != 200:
            logging.error("Label Name Query Error: %s", r.content)
            return False

        data = r.json()
        names = data["data"]
        return names

    def cgroup_info(self, uid, jobid, start, end):
        """
        Queries a job's cgroup
        """

        match = "cgroup_info{uid='%s',jobid='%s'}" % (uid, jobid)

        params = {
            'match[]': match,
            'start': str(start),
            'end': str(end)
        }

        urlparse.urlencode(params, doseq=True)
        url = urlparse.urljoin(self._url, "/api/v1/label/cgroup/values")
        logging.debug('Prometheus QUERY CGROUP, start=%s end=%s', start, end)

        # Query data
        r = self._client.get(url, params=params)
        if r.status_code != 200:
            logging.error("Cgroup Query Error: %s", r.content)
            return False

        data = r.json()
        cgroup = data["data"][0]
        return cgroup


def formatforpreproc(response, ctx):
    """
    Format Prometheus query response into the expected format for preprocessors.
    
    params: Prometheus json response
    return: formatting generator for the
            appropriate prometheus response type
    """

    if ctx:
        return formatmatrixpreproc(response, ctx)
    else:
        return formatvectorpreproc(response)

def formatforplugin(response, ctx):
    """
    Format Prometheus query response into the expected format for plugin.
    
    params: List of Prometheus json responses
    return: Formatting generator for the
            appropriate prometheus response type
    """

    if ctx:
        return formatmatrix(response, ctx)
    else:
        return formatvector(response)        


def formatvectorpreproc(response):
    """ """
    first = next(iter(response))

    # Timestamp is the same for all instances in a vector
    ts = int(response[first]["data"]["result"][0]["value"][0])
    
    data = []
    for m in response.values():
        size = len(m["data"]["result"])
        idx = np.arange(size)
        vals = np.fromiter(populatematrix(m), np.float64, size)
        data.append(np.column_stack((vals, idx)))

    yield ts, data
    
def formatmatrixpreproc(response, ctx):
 
    for metric, data in response.items():
        label = ctx.get_label(metric)
        for inst in data["data"]["result"]:
            inst_id = inst["metric"][label]
            min_ts = inst["values"][0][0]
            ctx.add_inst(metric, inst_id, min_ts)

    done = False
    while not done:
        data = []
        for m, d in response.items():
            size = len(d["data"]["result"])
            idx = np.arange(size)
            vals = np.fromiter(populatematrix(m, d, ctx), np.float64, size)
            pdata = np.column_stack((vals, idx))
            data.append(pdata)

        min_ts = ctx.min_ts()
        ctx.update_min_ts()
        if np.inf == ctx.min_ts():
            done = True

        yield min_ts, data

def formatvector(response):
    first = next(iter(response))

    # Timestamp is the same for all instances in a vector
    ts = int(response[first]["data"]["result"][0]["value"][0])

    data = []
    for m in response.values():
        size = len(m["data"]["result"])
        data.append(np.fromiter(populatevector(m), np.float64, size))

    yield ts, data

def formatmatrix(response, ctx):

    for metric, data in response.items():
        label = ctx.get_label(metric)
        for inst in data["data"]["result"]:
            inst_id = inst["metric"][label]
            min_ts = inst["values"][0][0]
            ctx.add_inst(metric, inst_id, min_ts)
    
    done = False
    while not done:
        data = []
        for m, d in response.items():
            size = len(d["data"]["result"])
            data.append(np.fromiter(populatematrix(m, d, ctx), np.float64, size))
        
        min_ts = ctx.min_ts()
        ctx.update_min_ts()
        if np.inf == ctx.min_ts():
            done = True

        yield min_ts, data

def populatematrix(metric, data, context):
    min_ts = context.min_ts()
    label = context.get_label(metric)

    for inst in data["data"]["result"]:
        inst_id = inst["metric"][label]
        idx = context.get_idx(metric, inst_id)
        try:
            ts = inst["values"][idx][0]
        except IndexError:
            yield np.NaN

        if ts == min_ts:
            value = inst["values"][idx][1]
            try:
                next_ts = inst["values"][idx+1][0]
            except IndexError:
                next_ts = np.inf
            context.update(metric, inst_id, next_ts)
            yield value
        else:
            yield np.NaN
            
def populatevector(metric):
    """ Generator to populate numpy array 
        from prometheus response data
    """
    for inst in metric["data"]["result"]:
        yield inst["value"][1]

class Context():
    def __init__(self):
        self._min_ts = np.inf
        self._next_min_ts = np.inf
        self._idx_dict = {}

    def __str__(self):
        return str(self._idx_dict)

    def inst_cnt(self, metric):
        return len(self._idx_dict[metric]["inst"].keys())        

    def update(self, metric, inst, ts):
        self._idx_dict[metric]["insts"][inst]["ts"] = ts
        self._idx_dict[metric]["insts"][inst]["idx"] += 1
        self._next_min_ts = min(self._next_min_ts, ts)

    def update_min_ts(self):
        self._min_ts = self._next_min_ts
        self._next_min_ts = np.inf

    def min_ts(self):
        return self._min_ts

    def get_label(self, metric):
        return self._idx_dict[metric]["label"]

    def get_idx(self, metric, inst):
        return self._idx_dict[metric]["insts"][inst]["idx"]
        
    def get_ts(self, metric, inst):
        return self._idx_dict[metric]["insts"][inst]["ts"]

    def add_metric(self, metric, label):
        self._idx_dict.update({metric : {"insts" : {}, "label" : label}})

    def add_inst(self, metric, inst, ts):
        idx_dict = {"idx" : 0, "ts" : np.inf}
        self._idx_dict[metric]["insts"].update({inst : idx_dict})
        self._min_ts = min(self._min_ts, ts)

    def reset(self):
        for m in self._idx_dict.values():
            for inst in m["insts"].values():
                inst = {"idx" : 0, "ts" : np.inf}
