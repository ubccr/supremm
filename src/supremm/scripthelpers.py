#!/usr/bin env python
""" common functions used in the command line scripts """

import re
import datetime
import MySQLdb
import MySQLdb.cursors
import sys
import logging
import requests

def parsetime(strtime):
    """ Try to be flexible in the time formats supported:
           1) unixtimestamp prefixed with @
           2) year-month-day zero-padded
           3) year-month-day hour:minute:second zero padded optional T between date and time
           4) locale specific format
    """
    m = re.search(r"^@(\d*)$", strtime)
    if m:
        return datetime.datetime.fromtimestamp(int(m.group(1)))
    if re.search(r"^\d{4}-\d{2}-\d{2}$", strtime):
        return datetime.datetime.strptime(strtime, "%Y-%m-%d")
    m = re.search(r"^(\d{4}-\d{2}-\d{2}).(\d{2}:\d{2}:\d{2})$", strtime)
    if m:
        return datetime.datetime.strptime(m.group(1) + " " + m.group(2), "%Y-%m-%d %H:%M:%S")

    return datetime.datetime.strptime(strtime, "%c")

def getdbconnection(configsection, as_dict=False, defaultargs={}):
    """ Helper function that gets a database connection object from a config dictionary """

    dbengine = configsection['dbengine'] if 'dbengine' in configsection else 'MySQLDB'

    if dbengine == 'MySQLDB':

        dbargs = defaultargs.copy()
        # Convert the external configuration names to python PEP-249 config names
        translate = {"host": "host", 
                     "defaultsfile": "read_default_file",
                     "user": "user",
                     "pass": "passwd",
                     "port": "port"}

        for confval, myval in translate.iteritems():
            if confval in configsection:
                dbargs[myval] = configsection[confval]

        if as_dict:
            dbargs['cursorclass'] = MySQLdb.cursors.DictCursor

        dbargs['local_infile'] = 1

        return MySQLdb.connect(**dbargs)
    else:
        raise Exception("Unsupported database engine %s" % (dbengine))

def setuplogger(consolelevel, filename=None, filelevel=None):
    """ setup the python root logger to log to the console with defined log
        level. Optionally also log to file with the provided level """

    if filelevel == None:
        filelevel = consolelevel

    if sys.version.startswith("2.7"):
        logging.captureWarnings(True)

    rootlogger = logging.getLogger()
    rootlogger.setLevel(min(consolelevel, filelevel))

    formatter = logging.Formatter('%(asctime)s.%(msecs)03d [%(levelname)s] %(message)s', datefmt='%Y-%m-%dT%H:%M:%S')

    if filename != None:
        filehandler = logging.FileHandler(filename)
        filehandler.setLevel(filelevel)
        filehandler.setFormatter(formatter)
        rootlogger.addHandler(filehandler)

    consolehandler = logging.StreamHandler()
    consolehandler.setLevel(consolelevel)
    consolehandler.setFormatter(formatter)
    rootlogger.addHandler(consolehandler)
    logging.getLogger('urllib3.connectionpool').setLevel(logging.ERROR)
    logging.getLogger('requests').setLevel(logging.ERROR)

