import logging
import sys
import urllib.parse as urlparse
import math

import numpy as np
import requests


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

def formatforpreproc(rdata):
    """
    Format Prometheus query response into the expected format for preprocessors.
    Check out https://prometheus.io/docs/prometheus/latest/querying/api/ for the formatting information.
    
    params: Prometheus json response
    return:
    """
    rtype = rdata["data"]["resultType"]
    result = rdata["data"]["result"]

    # Process vector
    if rtype == "vector":
        return formatvector(result)

    # Process matrix
    elif rtype == "matrix":
        return formatmatrix(result)

def formatforplugin(response, ctx=None):
    """
    
    """
    rtype = response[0]["data"]["resultType"]

    # Process vector
    if rtype == "vector":
        return formatvector(response)

    # Process matrix
    elif rtype == "matrix":
        return formatmatrix(response, ctx)

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

    yield ts, data, _

def formatmatrix(response, label="host"):
    def populatematrix(metric, ctx):
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
                yield inst["values"][idx][1]
            else:
                yield np.NaN

    done = False
    while not done:
        ctx = yield
        if ctx["ts_min"] == math.inf:
            #print("No more timestamps!")
            done = True

        # Get data from prometheus response
        data = []
        for m in response:
            size = len(m["data"]["result"])
            data.append(np.fromiter(populatematrix(m, ctx), np.float64, size))

        # Update minimum timestamps
        ts_min = ctx["ts_min"]
        next_ts_min = math.inf
        for m in response:
            for inst in m["data"]["result"]:
                name = inst["metric"]["__name__"]
                inst_id = inst["metric"][ctx["label"]]
                
                idx = ctx["idx_dict"][name][inst_id]["idx"]
                ts = ctx["idx_dict"][name][inst_id]["ts"]

                if ts == ts_min:
                    idx += 1
                    ctx["idx_dict"][name][inst_id]["idx"] = idx
                    try:
                        next_ts_min = inst["values"][idx][0]
                    except IndexError:
                        next_ts_min = math.inf
                else:
                    next_ts_min = min(next_min_ts, ts)

                ctx["idx_dict"][name][inst_id]["ts"] = next_ts_min
        ctx["ts_min"] = next_ts_min
        yield ts_min, data, ctx


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
