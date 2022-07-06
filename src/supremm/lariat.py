#!/usr/bin/env python3
""" Utilities for processing Lariat data """
import datetime
import os
import json
import logging

class LariatManager(object):
    """ find and cache the lariat data for a job """
    def __init__(self, lariatpath):
        self.lariatpath = lariatpath
        self.lariatdata = dict()
        self.filesprocessed = []
        self.errors = dict()

    def find(self, jobid, jobstarttime, jobendtime):
        """ returns a dict containing the lariat data for a job """

        if jobid in self.lariatdata:
            print("Lariat cache size is ", len(self.lariatdata))
            return self.lariatdata.pop(jobid)

        for days in (0, -1, 1):
            searchday = datetime.datetime.utcfromtimestamp(jobendtime) + datetime.timedelta(days)
            lfilename = os.path.join(self.lariatpath, searchday.strftime('%Y'), searchday.strftime('%m'), searchday.strftime('lariatData-sgeT-%Y-%m-%d.json'))
            self.loadlariat(lfilename)
            if jobid in self.lariatdata:
                return self.lariatdata[jobid]

        for days in (0, -1, 1):
            searchday = datetime.datetime.utcfromtimestamp(jobstarttime) + datetime.timedelta(days)
            lfilename = os.path.join(self.lariatpath, searchday.strftime('%Y'), searchday.strftime('%m'), searchday.strftime('lariatData-sgeT-%Y-%m-%d.json'))
            self.loadlariat(lfilename)

            if jobid in self.lariatdata:
                return self.lariatdata[jobid]

        return None

    @staticmethod
    def removeDotKey(obj):
        """ replace . with - in the keys for the json object """
        for key in list(obj.keys()):
            new_key = key.replace(".", "-")
            if new_key != key:
                obj[new_key] = obj[key]
                del obj[key]
        return obj

    def loadlariat(self, filename):
        """ load and store the contents of  lariat output file "filename" """

        if filename in self.filesprocessed:
            # No need to reparse file. If the job data was in the file, then this search
            # function would not have been called.
            return

        try:
            with open(filename, "rb") as fp:

                # Unfortunately, the lariat data is not in valid json
                # This workaround converts the illegal \' into valid quotes
                content = fp.read().replace("\\'", "'")
                lariatJson = json.loads(content, object_hook=LariatManager.removeDotKey)

                for k, v in lariatJson.items():
                    if k not in self.lariatdata:
                        self.lariatdata[k] = v[0]
                    else:
                        # Have already got a record for this job. Keep the record
                        # that has longer recorded runtime since this is probably
                        # the endofjob record.
                        if 'runtime' in v[0] and 'runtime' in self.lariatdata[k] and self.lariatdata[k]['runtime'] < v[0]['runtime']:
                            self.lariatdata[k] = v[0]

                self.filesprocessed.append(filename)

        except Exception as e:
            logging.error("Error processing lariat file %s. Error was %s.", filename, str(e))

