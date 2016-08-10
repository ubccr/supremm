from __future__ import print_function
import sys
import os
from pymongo import MongoClient
from pymongo.errors import InvalidDocument
import json

class factory(object):
    """ output class generator helper """
    def __init__(self, config, resconf):
        outconf = config.getsection("outputdatabase")

        if outconf['db_engine'].lower() == "mongodb":
            self._impl = MongoOutput(outconf, resconf)
        elif outconf['db_engine'].lower() == "stdout":
            self._impl = StdoutOutput(outconf, resconf)
        elif outconf['db_engine'] == 'file':
            self._impl = FileOutput(outconf, resconf)
        else:
            raise Exception("Unsupported output mechanism {0}".format(outconf['db_engine']))

    def __enter__(self):
        return self._impl.__enter__()

    def __exit__(self, exception_type, exception_val, trace):
        return self._impl.__exit__(exception_type, exception_val, trace)


class FileOutput(object):
    """
    Dumps output into a file in one of two fashions
    1. Fragment - dumps snippets of json (currently one json object per job) into
                  a file as the process runs. This file will NOT be valid json, however
                  useful for pseudo interactive debugging purposes.
    2. Complete - dumps entire result as one large json array at end of process.
                  File will be empty during runtime, but will be valid json.
    """
    def __init__(self, outconf, resconf):
        self._resid = resconf['resource_id']

        jsonoption = outconf['json_format']
        if jsonoption == 'both':
            self._fragjson = True
            self._completejson = True
        elif jsonoption == 'fragment':
            self._fragjson = True
            self._completejson = False
        elif jsonoption == 'complete':
            self._fragjson = False
            self._completejson = True
        else:
            raise Exception("Not a valid json option {0}".format(jsonoption))

        if self._fragjson:
            self._fragpath = outconf['frag_file']
        if self._completejson:
            self._comppath = outconf['comp_file']

        if self._fragjson and not os.path.exists(self._fragpath):
            raise Exception("Path specified by frag_file does not exist")
        if self._completejson and not os.path.exists(self._comppath):
            raise Exception("Path specified by comp_file does not exist")

        if self._fragjson:
            self._fragfile = open(self._fragpath, 'w')
        if self._completejson:
            self._compfile = open(self._comppath, 'w')
            self._jsonarray = []

    def __enter__(self):
        return self

    def process(self, summary, mdata):
        """
        json print
        """
        if self._fragjson:
            print(self._resid, json.dumps(summary.get(), indent=4), file=self._fragfile)
            print("MDATA: ", json.dumps(mdata, indent=4), file=self._fragfile)
        if self._completejson:
            self._jsonarray.append(summary.get())
            self._jsonarray.append(mdata)

    def __exit__(self, exception_type, exception_val, trace):
        if self._fragjson:
            self._fragfile.close()
        if self._completejson:
            print(json.dumps(self._jsonarray, indent=4), file=self._compfile)
            self._compfile.close()


class MongoOutput(object):
    """ Support for mongodb output """
    def __init__(self, outconf, resconf):
        self._uri = outconf['uri']
        self._dname = outconf.get('dbname', outconf.get('db', 'supremm'))
        self._collection = "resource_" + str(resconf['resource_id'])
        self._timeseries = "timeseries-" + self._collection
        self._client = None
        self._outdb = None

    def __enter__(self):
        self._client = MongoClient(host=self._uri)
        self._outdb = self._client[self._dname]
        return self

    def process(self, summaryobj, mdata):
        """ output the summary record """

        summary = summaryobj.get()
        mongoid = str(summary['acct']['id']) + '-' + str(summary['acct']['end_time'])
        summary['_id'] = mongoid
        summary['summarization'].update(mdata)

        if 'timeseries' in summary:
            self._outdb[self._timeseries].update({"_id": summary["_id"]}, summary['timeseries'], upsert=True)
            del summary['timeseries']

        self._outdb[self._collection].update({"_id": summary["_id"]}, summary, upsert=True)

    def __exit__(self, exception_type, exception_val, trace):
        if self._client != None:
            self._outdb = None
            self._client.close()
            self._client = None


class StdoutOutput(object):
    """
    Simple outputter that dumps the job summary to stdout. Intended for debug purposes.
    """
    def __init__(self, _, resconf):
        self._resid = resconf['resource_id']

    def __enter__(self):
        return self

    def process(self, summary, mdata):
        """
        json print
        """
        print(self._resid, json.dumps(summary.get(), indent=4))
        print("MDATA: ", json.dumps(mdata, indent=4))

    def __exit__(self, exception_type, exception_val, trace):
        pass
