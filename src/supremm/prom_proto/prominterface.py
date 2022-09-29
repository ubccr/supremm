import logging
import sys
import urllib.parse as urlparse
import math

import numpy as np
import requests

class Context():
    def __init__(self):
        self._min_ts = np.inf
        self._idx_dict = {}

    # Build context
    def add_metric(query, label):
        for m in query:
            inst_cnt = 0
            name = m["data"]["result"][0]["metric"]["__name__"]
            metrics = { name : {"label" : label, "inst" : {}} }
            for inst in m["data"]["result"]:
                ### 'Label' is uniqueness
                inst_cnt = inst["metric"][ctx["label"]]
                ts = int(inst["values"][0][0])
                if ts < self._min_ts:
                    self._min_ts = ts
                metrics[name].update({inst_id :
                                        {"idx"    : 0,
                                         "ts"     : ts,
                                         "update" : False,
                                         "id"     : inst_cnt}})
                inst_cnt += 1
            self._idx_dict.update(metrics)

HTTP_TIMEOUT = 5
MAX_DATA_POINTS = 11000 # Prometheus queries return maximum of 11,000 data points

class PromClient():
    def __init__(self, url):
        self._url = url
        self._step = '30s'

    def __str__(self):
        return self._url

    def query(self, query, time):
        headers = {'Content-Type': 'application/x-www-form-urlencoded'}
        params = {
            'query': query,
            'time': time,
        }

        endpoint = "/api/v1/query"
        url = urlparse.urljoin(self._url, endpoint)
 
        r = requests.get(url, params=params, headers=headers)
        if r.status_code != 200:
            print(str(r.content))
            return None

        return r.json()

    def query_range(self, query, start, end):
        headers = {'Content-Type': 'application/x-www-form-urlencoded'}
        params = {
            'query': query,
            'start': start,
            'end': end,
            'step': self._step
        }

        endpoint = "/api/v1/query_range"
        url = urlparse.urljoin(self._url, endpoint)
        
        r = requests.get(url, params=params, headers=headers)
        if r.status_code != 200:
            print(r.content)
            return None

        return r.json()

    def timeseries_meta(self, start, end, match):
        # This is basis for checking if timeseries is available
        # Checks if a timeseries or list of timeseries ('match[]') are available
        # without returning any actual data
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded',
        }
        params = {
            'match[]': match,
            'start': str(start),
            'end': str(end)
        }

        endpoint = "/api/v1/series"
        urlparse.urlencode(params, doseq=True)
        url = urlparse.urljoin(self._url, endpoint)
        logging.debug('Prometheus QUERY SERIES META, start=%s end=%s', start, end)

        r = requests.get(url, params=params, headers=headers)
        if r.status_code != 200:
            return False

        data = r.json() # "data" is a list of zero or more 
                        # timeseries present at the specified times

        return bool(data["data"])


    def label_val_meta(self, start, end, matches, label, type):
        """
        Queries label values for a given metric.
        """
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded',
        }
        params = {
            'match[]': matches,
            'start': str(start),
            'end': str(end)
        }

        urlparse.urlencode(params, doseq=True)
        url = urlparse.urljoin(self._url, "/api/v1/label/%s/values" % label)
        logging.debug('Prometheus QUERY LABEL VALUES, start=%s end=%s', start, end)

        # Query data
        r = requests.get(url, params=params, headers=headers)
        if r.status_code != 200:
            logging.error("Label Name Query Error: %s", r.content)
            return False
        data = r.json()
        names = data["data"]
        label_idx = np.arange(0, len(names))

        # Format for preprocessor
        if type == "preprocessor":
            description = dict(zip(label_idx, names))
            return description #[label_idx, names]

        # Format for plugin
        elif type == "plugin":
            description = (label_idx, names)
            return description #[label_idx, names]

    def cgroup_info(self, uid, jobid, start, end):
        """
        Queries a job's cgroup
        """
        match = "cgroup_info{uid='%s',jobid='%s'}" % (uid, jobid)

        headers = {
            'Content-Type': 'application/x-www-form-urlencoded',
        }
        params = {
            'match[]': match,
            'start': str(start),
            'end': str(end)
        }

        urlparse.urlencode(params, doseq=True)
        url = urlparse.urljoin(self._url, "/api/v1/label/cgroup/values")
        logging.debug('Prometheus QUERY CGROUP, start=%s end=%s', start, end)

        # Query data
        r = requests.get(url, params=params, headers=headers)
        if r.status_code != 200:
            logging.error("Cgroup Query Error: %s", r.content)
            return False

        data = r.json()
        cgroup = data["data"][0]
        return cgroup

def formatforpreproc(response):
    """
    Format Prometheus query response into the expected format for preprocessors.
    
    params: Prometheus json response
    return: formatting generator for the
            appropriate prometheus response type
    """

    rtype = response[0]["data"]["resultType"]

    # Process vector
    if rtype == "vector":
        return formatvectorpreproc(response)

    # Process matrix
    elif rtype == "matrix":
        return formatmatrixpreproc(response)

def formatforplugin(response):
    """
    Format Prometheus query response into the expected format for plugin.
    
    params: Prometheus json response
    return: formatting generator for the
            appropriate prometheus response type
    """

    rtype = response[0]["data"]["resultType"]

    # Process vector
    if rtype == "vector":
        return formatvector(response)

    # Process matrix
    elif rtype == "matrix":
        return formatmatrix(response)

def formatvectorpreproc(response):
    """ """

    def populatevector(metric):
        """ Generator to populate data array """
        for inst in metric["data"]["result"]:
            yield inst["value"][1]

    # Timestamp is the same for all instances in a vector
    ts = int(response[0]["data"]["result"][0]["value"][0])
    
    data = []
    for m in response:
        size = len(m["data"]["result"])
        data.append(np.fromiter(populatevector(m), np.float64, size))
        data.append([i for i in range(0, size)])

    yield ts, data
    
def formatmatrixpreproc(response, label="cpu"):

    def getdata(metric, ctx):
        """ Generator to populate data array """
        label = ctx["label"]
        ts_min = ctx["ts_min"]

        for inst in metric["data"]["result"]:
            name = inst["metric"]["__name__"]
            inst_id = inst["metric"][label]
            idx = ctx["idx_dict"][name][inst_id]["idx"]
            try:
                ts = inst["values"][idx][0]
            except IndexError:
                yield np.NaN

            if ts == ts_min:
                value = inst["values"][idx][1]
                try:
                    idx += 1
                    ctx["idx_dict"][name][inst_id]["ts"] = inst["values"][idx][0]
                    ctx["idx_dict"][name][inst_id]["idx"] = idx
                except IndexError:
                    ctx["idx_dict"][name][inst_id]["ts"] = np.inf
                yield value
            else:
                yield np.NaN

    # Initialize minimum timestamp with first available timestamp
    # Note: 'label' is not universal for required metrics so this should
    # be moved out of context dict.
    ctx = {
        "ts_min" : int(response[0]["data"]["result"][0]["values"][0][0]),
        "label" : label,
        "idx_dict" : dict()
    }
    # Build context
    for m in response:
        print(m["data"]["result"][0]["metric"])
        inst_cnt = 0
        name = m["data"]["result"][0]["metric"]["__name__"]
        metrics = { name : {} }
        for inst in m["data"]["result"]:
            ### 'Label' is uniqueness
            inst_cnt = inst["metric"][ctx["label"]]
            ts = int(inst["values"][0][0])
            if ts < ctx["ts_min"]:
                ctx["ts_min"] = ts
            metrics[name].update({inst_id :
                                    {"idx"    : 0,
                                     "ts"     : ts,
                                     "update" : False,
                                     "id"     : inst_cnt}})
            inst_cnt += 1
        ctx["idx_dict"].update(metrics)

    done = False
    while not done:
        # Build plugin-formatted data from prometheus response
        # for a single timestamp
        data = []
        for m in response:
            size = len(m["data"]["result"])
            data.append([d for d in getdata(m, ctx)])
            data.append(inst["id"] for inst in ctx["idx_dict"].values())

        # Update minimum timestamps in context dict
        ts_min = ctx["ts_min"]

        ctx["ts_min"] = next_ts_min
        if ctx["ts_min"] == np.inf:
            #print("No more timestamps!")
            done = True

        yield ts_min, data

def formatvector(response):
    def populatevector(metric):
        for inst in metric["data"]["result"]:
            yield inst["value"][1]

    # Timestamp is the same for all instances in a vector
    ts = int(response[0]["data"]["result"][0]["value"][0])

    data = []
    for m in response:
        size = len(m["data"]["result"])
        data.append(np.fromiter(populatevector(m), np.float64, size))

    yield ts, data

def formatmatrix(response, label="host"):

    def getdata(metric, ctx):
        label = ctx["label"]
        ts_min = ctx["ts_min"]

        for inst in metric["data"]["result"]:
            name = inst["metric"]["__name__"]
            inst_id = inst["metric"][label]
            idx = ctx["idx_dict"][name][inst_id]["idx"]
            try:
                ts = inst["values"][idx][0]
            except IndexError:
                yield np.NaN

            if ts == ts_min:
                value = inst["values"][idx][1]
                try:
                    idx += 1
                    ctx["idx_dict"][name][inst_id]["ts"] = inst["values"][idx][0]
                    ctx["idx_dict"][name][inst_id]["idx"] = idx
                except IndexError:
                    ctx["idx_dict"][name][inst_id]["ts"] = np.inf
                yield value
            else:
                yield np.NaN

    # Initialize minimum timestamp with first available timestamp
    # Note: 'label' is not universal for required metrics so this should
    # be moved out of context dict.
    ctx = {
        "ts_min" : int(response[0]["data"]["result"][0]["values"][0][0]),
        "label" : label,
        "idx_dict" : dict()
    }
    # Build context
    for m in response:
        inst_cnt = 0
        name = m["data"]["result"][0]["metric"]["__name__"]
        metrics = { name : {} }
        for inst in m["data"]["result"]:
            ### 'Label' is uniqueness
            inst_id = inst["metric"][ctx["label"]]
            ts = int(inst["values"][0][0])
            if ts < ctx["ts_min"]:
                ctx["ts_min"] = ts
            metrics[name].update({inst_id :
                                    {"idx"    : 0,
                                     "ts"     : ts,
                                     "update" : False,
                                     "id"     : inst_cnt}})
            inst_cnt += 1
        ctx["idx_dict"].update(metrics)

    done = False
    while not done:
        # Build plugin-formatted data from prometheus response
        # for a single timestamp
        data = []
        for m in response:
            size = len(m["data"]["result"])
            data.append(np.fromiter(getdata(m, ctx), np.float64, size))

        # Update minimum timestamps in context dict
        ts_min = ctx["ts_min"]
        for m in ctx["idx_dict"].values():
            ctx["ts_min"] = min(inst["ts"] for inst in m.values())

        if ctx["ts_min"] == np.inf:
            done = True

        yield ts_min, data

if __name__=="__main__":
    pass
    #url = "http://172.22.0.216:9090"

    #start = "2022-06-26T00:00:00.000Z"
    #end = "2022-06-29T19:30:00.000Z"

    #client = PromClient(url)
    #data = client.query_range("node_cpu_seconds_total{mode='user', host='prometheus-dev'}", start, end)
    #data = client.query("node_cpu_seconds_total{mode='user', host='prometheus-dev'} * 1000", start)
    #labels = client.label_val_meta(start, end, ["node_cpu_seconds_total{mode='user', host='prometheus-dev'}"], "cpu")

    #print(data.nbytes)
    #print(data)
