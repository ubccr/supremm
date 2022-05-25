import urllib.parse as urlparse

import numpy as np
import requests


HTTP_TIMEOUT = 5

class PromClient():

    def __init__(self, url):
        self._url = url

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
        data = r.json()
        plugin_data = formatforplugin(data)
        return plugin_data

    def query_range(self, query, start, end, step="30s"):
        headers = {'Content-Type': 'application/x-www-form-urlencoded'}
        params = {
            'query': query,
            'start': start,
            'end': end,
            'step': step
        }

        endpoint = "/api/v1/query_range"
        url = urlparse.urljoin(self._url, endpoint)

        r = requests.get(url, params=params, headers=headers)
        if r.status_code != 200:
            print(r.content)
            return None
        data = r.json()
        plugin_data = formatforplugin(data)
        return plugin_data

    def series_meta(self):
        headers = {'Content-Type': 'application/x-www-form-urlencoded'}
        params = {
            'match[]': matches,
            'start': start,
            'end': end
        }

        endpoint = "/api/v1/series"
        url = urlparse.urljoin(self._url, endpoint)
        return

    def label_val_meta(self, start, end, matches, l):
        "    def label_val_meta(self, start, end, matches, l):
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
        # type(l) == set, len(l) == 1

        urlparse.urlencode(params, doseq=True)
        url = urlparse.urljoin(self.url, "/api/v1/label/%s/values" % l)
#        logging.debug('Prometheus QUERY LABEL VALUES, url="(%s).20s" start=%s end=%s', url, start, end
)

        # Get data
        r = requests.get(url, params=params, headers=headers)
        if r.status_code != 200:
            print(r.content)
            return False
        data = r.json()

        # Format for plugin
        label_idx = np.arange(0, len(data["data"]))
        return [label_idx, data["data"]]""
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
        # type(l) == set, len(l) == 1

        urlparse.urlencode(params, doseq=True)
        url = urlparse.urljoin(self.url, "/api/v1/label/%s/values" % l)
#        logging.debug('Prometheus QUERY LABEL VALUES, url="(%s).20s" start=%s end=%s', url, start, end)

        # Get data
        r = requests.get(url, params=params, headers=headers)
        if r.status_code != 200:
            print(r.content)
            return False
        data = r.json()

        # Format for plugin
        label_idx = np.arange(0, len(data["data"]))
        return [label_idx, data["data"]] 

def formatforplugin(rdata):
    rtype = rdata["data"]["resultType"]
    # Process vector
    if rtype == "vector":
        return formatvector(rdata["data"]["result"])

    # Process matrix
    elif rtype == "matrix":
        return formatmatrix(rdata["data"]["result"])

def formatvector(rdata):
    pdata = []
    ts = rdata[0]["value"][0] # (TODO: Ensure timestamps are equivalent)
    for m in rdata:
        pdata.append(m["value"][1])
    return ts, pdata

def formatmatrix(rdata):
    for idx, val in enumerate(rdata[0]["values"]):
        ts = val[0]
        pdata = [m["values"][idx][1] for m in rdata]
        yield ts, pdata

if __name__=="__main__":
    url = "http://172.22.0.216:9090"

    start = "2022-05-02T00:30:00.000Z"
    end = "2022-05-02T09:00:00.000Z"

    client = PromClient(url)
    data = client.query_range("node_cpu_seconds_total{mode='user'}", start, end)
    for item in data:
        print(item)
