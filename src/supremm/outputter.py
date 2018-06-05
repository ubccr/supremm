from __future__ import print_function
import sys
import os
from pymongo import MongoClient
from pymongo.errors import InvalidDocument
import json

class factory(object):
    """ output class generator helper """
    def __init__(self, config, resconf, dry_run=False):
        outconf = config.getsection("outputdatabase")

        if 'db_engine' not in outconf and 'type' in outconf:
            # Older versions of the documentation recommended 'type' as the
            # configuration parameter use this if 'db_engine' is absent
            outconf['db_engine'] = outconf['type']

        if outconf['db_engine'].lower() == "mongodb":
            self._impl = MongoOutput(outconf, resconf) if not dry_run else NullOutput()
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
            self._comppath = outconf['comp_file'].replace("%r", resconf["name"])

        if self._fragjson:
            self._fragfile = open(self._fragpath, 'w')
        if self._completejson:
            self._jsonarray = []

    def __enter__(self):
        return self

    def process(self, summary, mdata):
        """
        json print
        """
        if self._fragjson:
            print(self._resid, json.dumps(summary, indent=4, default=str), file=self._fragfile)
            print("MDATA: ", json.dumps(mdata, indent=4, default=str), file=self._fragfile)
        if self._completejson:
            self._jsonarray.append(summary)
            self._jsonarray.append(mdata)

    def __exit__(self, exception_type, exception_val, trace):
        if self._fragjson:
            self._fragfile.close()
        if self._completejson:
            with open(self._comppath, 'w') as f:
                json.dump(self._jsonarray, f, indent=4, default=str)


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

    def process(self, summary, mdata):
        """ output the summary record """

        mongoid = str(summary['acct']['id']) + '-' + str(summary['acct']['end_time'])
        summary['_id'] = mongoid
        summary['summarization'].update(mdata)

        if 'timeseries' in summary:
            summary['timeseries']['_id'] = summary["_id"]
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
        print(self._resid, json.dumps(summary, indent=4))
        print("MDATA: ", json.dumps(mdata, indent=4))

    def __exit__(self, exception_type, exception_val, trace):
        pass


class NullOutput(object):
    """
    Outputter used when configured outputter is a database (Mongo) but the dry-run option is set.
    Discards all data.
    """
    def __enter__(self):
        return self

    def process(self, summary, mdata):
        pass

    def __exit__(self, exception_type, exception_val, trace):
        pass
