#!/usr/bin/env python
""" Script that reads hardware info from the archives 
    and outputs the data into a json file

    @author Max Dudek <maxdudek@gmail.com>
"""

import argparse
from datetime import datetime, timedelta
import logging
import os
import re
from re import sub, search
import time

import json
from collections import defaultdict

from pcp import pmapi
import cpmapi as c_pmapi

from supremm.config import Config
from supremm.scripthelpers import parsetime, setuplogger
from supremm.indexarchives import PcpArchiveFinder
from supremm.account import DbArchiveCache
from supremm.xdmodaccount import XDMoDArchiveCache

# Testing only
import sys
import traceback

DAY_DELTA = 3

STAGING_COLUMNS = [
    'hostname',
    'manufacturer',
    'codename',
    'model_name',
    'clock_speed',
    'core_count',
    'board_manufacturer',
    'board_name',
    'board_version',
    'system_manufacturer',
    'system_name',
    'system_version',
    'physmem',
    'numa_node_count',
    'disk_count',
    'ethernet_count',
    'ib_device_count',
    'ib_device',
    'ib_ca_type',
    'ib_ports',
    'gpu_device_count',
    'gpu_device_manufacturer',
    'gpu_device_name',
    'record_time_ts'
]

class PcpArchiveHardwareProcessor(object):
    """ Parses a pcp archive and adds the archive information to the index """

    def __init__(self, resconf):
        self.hostname_mode = resconf['hostname_mode']
        if self.hostname_mode == "fqdn":
            self.hostnameext = resconf['host_name_ext']
        # self.tz_adjuster = TimezoneAdjuster(resconf.get("timezone"))

    @staticmethod
    def getDataFromArchive(archive, host_from_path=None):
        """ Open the pcp archive and get hardware data
            @return a dictionary containing the data,
            or None if the processor encounters an error
        """
        context = pmapi.pmContext(c_pmapi.PM_CONTEXT_ARCHIVE, archive)
        mdata = context.pmGetArchiveLabel()
        hostname = mdata.hostname.split('.')[0]
        record_time_ts = float(mdata.start)

        pmfg = pmapi.fetchgroup(c_pmapi.PM_CONTEXT_ARCHIVE, archive)

        metrics = {
            "hinv.ncpu": {
                "type": "item",
                "alias": "ncpu",
            },
            "hinv.ndisk": {
                "type": "item",
                "alias": "ndisk",
            },
            "hinv.physmem": {
                "type": "item",
                "alias": "physmem",
            },
            "hinv.cpu.vendor": {
                "type": "indom",
                "alias": "manufacturer",
            },
            "hinv.cpu.model_name": {
                "type": "indom",
                "alias": "model_name",
            },
            "hinv.map.cpu_node": {
                "type": "indom",
                "alias": "numa_mapping",
            },
            "infiniband.hca.type": {
                "type": "indom",
                "alias": "ina",
            },
            "infiniband.hca.ca_type": {
                "type": "indom",
                "alias": "inb",
            },
            "infiniband.hca.numports": {
                "type": "indom",
                "alias": "inc",
            },
            "network.interface.in.bytes": {
                "type": "indom",
                "alias": "ethernet",
            },
            "nvidia.cardname": {
                "type": "indom",
                "alias": "nvidia",
            },
        }

        # Extend objects maps metric aliases to PCP extend objects
        # Metrics map to None if the metric does not appear in the archive
        extObj = {}
        fetchedData = {}
        for metric in metrics:
            try:
                metricType = metrics[metric]["type"]
                alias = metrics[metric]["alias"]
                if metricType == "item":
                    extObj[alias] = pmfg.extend_item(metric)
                elif metricType == "indom":
                    extObj[alias] = pmfg.extend_indom(metric)
            except pmapi.pmErr as exc:
                # If the metric doesn't appear in the archive
                if exc.message().startswith("Unknown metric name"):
                    extObj[alias] = None
                    fetchedData[alias] = None
                    ##########
                    if alias != "nvidia" and alias != "model_name":
                        print("A metric could not be found for alias '%s', returning None..." % (alias))
                    ##########
                else:
                    err = {"archive": archive, "metric": metric, "error": exc.message()}
                    print(json.dumps(err) + ",")
                    print("ERROR: pmfg.extend_item or pmfd.extend_indom threw an unexpected exception")
                    sys.exit(1)

        # fetch data until all metrics have been retrieved, or the end of the archive is reached
        while not (all(metrics[metric]["alias"] in fetchedData for metric in metrics)):
            try:
                pmfg.fetch()
            except pmapi.pmErr as exc:
                if exc.message().startswith("End of PCP archive log"):
                    # End of archive - fill in missing data with 'None'
                    for metric in metrics:
                        if metric not in fetchedData:
                            fetchedData[metric] = None
                    break
                else:
                    err = {"archive": archive, "error": exc.message()}
                    print(json.dumps(err) + ",")
                    print("ERROR: pmfg.fetch() threw an unexpected exception")
                    sys.exit(1)

            for metric in metrics:
                metricType = metrics[metric]["type"]
                alias = metrics[metric]["alias"]
                if (metricType == "item") and (alias not in fetchedData):
                    fetchedData[alias] = extObj[alias]()
                elif (metricType == "indom") and (alias not in fetchedData) and (len(extObj[alias]()) > 0):
                    fetchedData[alias] = extObj[alias]()

        

        # Map the fetched data to the data that needs to be collected
        data = {
            'record_time_ts': record_time_ts,
            'hostname': hostname,
            'core_count': fetchedData["ncpu"],
            'disk_count': fetchedData["ndisk"],
            'physmem': fetchedData["physmem"],
            'manufacturer': fetchedData["manufacturer"][0][2]() if fetchedData["manufacturer"] else None,
            'model_name': fetchedData["model_name"][0][2]() if fetchedData["model_name"] else None,
            'numa_node_count': max([n[2]() for n in fetchedData["numa_mapping"]]) + 1,
            'ethernet_count': len([device[1] for device in fetchedData["ethernet"] if device[1] != "lo"]) if fetchedData["ethernet"] else 0
        }

        # Transform the infiniband data
        infini = defaultdict(list)
        if fetchedData["ina"]:
            for _, iname, value in fetchedData["ina"]:
                infini[iname].append(value())
        if fetchedData["inb"]:
            for _, iname, value in fetchedData["inb"]:
                infini[iname].append(value())
        if fetchedData["inc"]:
            for _, iname, value in fetchedData["inc"]:
                infini[iname].append(value())
        if infini:
            data['infiniband'] = dict(infini)
        
        # Transform gpu data
        try:
            if fetchedData["nvidia"]:
                data['gpu'] = {}
                for _, iname, value in fetchedData["nvidia"]:
                    data['gpu'][iname] = value()
        except pmapi.pmErr as exc:
            print("ERROR: unexpected exception raised while entering gpu data")
            traceback.print_exc()

        
        return data

    @staticmethod
    def isJobArchive(archive):
        fname = os.path.basename(archive)
        return fname.startswith("job-")

class HardwareStagingTransformer(object):
    """ Transforms the raw data from the archive into a list
        representing rows in the hardware staging table
    """

    def __init__(self, archiveData, replacementPath=None, outputFilename='hardware_staging.json'):
        """ Run the transformation

        Parameters:
        list archiveData: the raw data from the archive
        replacement: the path to the replacement dictionary
        outputFilename: the name/path of the json output file
        """
        
        self.result = [ STAGING_COLUMNS ]

        # Generate replacement rules from file
        if replacementPath == None:
            replacementPath = self.autoDetectReplacementPath()

        if replacementPath is None or os.path.isdir(replacementPath) == False:
            print("No replacement_rules.json file found. Replacement will not be performed")
        else:
            replacementFile = os.path.join(replacementPath, "replacement_rules.json")
            try:
                with open(replacementFile, 'r') as inFile:
                    self.replacementRules = json.load(inFile)
            except IOError as e:
                self.replacementRules = None

        # Transform the archive data
        for hw_info in archiveData:

            if 'infiniband' in hw_info:
                for device in hw_info['infiniband']:
                    hw_info['ib_device'] = device
                    hw_info['ib_ca_type'] = hw_info['infiniband'][device][1]
                    hw_info['ib_ports'] = hw_info['infiniband'][device][2]

            if ('gpu' in hw_info) and ('gpu0' in hw_info['gpu']):
                devices = list(hw_info['gpu'])
                hw_info['gpu_device_count'] = len(devices)
                hw_info['gpu_device_manufacturer'] = 'NA'
                hw_info['gpu_device_name'] = hw_info['gpu']['gpu0']
            elif 'gpu_device_count' not in hw_info:
                hw_info['gpu_device_count'] = 0

            self.result.append([
                hw_info['hostname'],
                self.get(hw_info.get('manufacturer')),
                self.get(hw_info.get('codename')),
                self.get(hw_info.get('model_name')), # model_name (node_mapping)
                'NA', # clock_speed
                self.get(hw_info.get('core_count'), 'int'),
                'NA',
                'NA',
                'NA',
                self.get(hw_info.get('system_manufacturer')),
                self.get(hw_info.get('system_name')),
                'NA',
                self.get(hw_info.get('physmem'), 'int'),
                self.get(hw_info.get('numa_node_count'), 'int'),
                self.get(hw_info.get('disk_count'), 'int'),
                self.get(hw_info.get('ethernet_count'), 'int'),
                1 if ('ib_device' in hw_info) else 0,
                self.get(hw_info.get('ib_device')),
                self.get(hw_info.get('ib_ca_type')),
                self.get(hw_info.get('ib_ports'), 'int'),
                hw_info['gpu_device_count'],
                self.get(hw_info.get('gpu_device_manufacturer')),
                self.get(hw_info.get('gpu_device_name')),
                self.get(hw_info.get('record_time_ts'))
            ])

        if self.replacementRules != None:
            self.doReplacement()

        with open(outputFilename, "w") as outFile:
            outFile.write(json.dumps(self.result, indent=4, separators=(',', ': ')))
    
    @staticmethod
    def autoDetectReplacementPath():
        searchpaths = [
            os.path.dirname(os.path.abspath(__file__)) + "/../../../../etc/supremm",
            "/etc/supremm",
            pkg_resources.resource_filename(pkg_resources.Requirement.parse("supremm"), "etc/supremm")
        ]

        for path in searchpaths:
            if os.path.exists(os.path.join(path, "replacement_rules.json")):
                return os.path.abspath(path)

        return None

    @staticmethod
    def get(value, typehint='str'):
        if (value != None) and (value != ""):
            return value
        if typehint == 'str':
            return 'NA'
        else:
            return -1
    
    def doReplacement(self):
        print('doing replacement')

        # Build a dictionary mapping column names to index
        columnToIndex = {}
        for i in range(len(self.result[0])):
            columnToIndex[self.result[0][i]] = i

        for row in self.result[1:]:
            for rule in self.replacementRules:
                # Check if conditions are true
                conditionsMet = True
                if 'conditions' in rule:
                    for condition in rule['conditions']:
                        assert 'column' in condition, 'Conditions must contain a "column" entry'
                        value = row[columnToIndex[condition['column']]]
                        reverse = ('reverse' in condition) and (condition['reverse']) # If 'reverse' is true, then the condition must be FALSE to replace
                        # Case one: equality condition
                        if 'equals' in condition:
                            if (condition['equals'] != value) != reverse:
                                conditionsMet = False
                                break
                        # Case two: contains condition
                        else:
                            assert 'contains' in condition, 'Conditions must contain either an "equals" or a "contains" property'
                            if (search(condition['contains'], value) == None) != reverse:
                                conditionsMet = False
                                break
                # Process replacements
                if conditionsMet:
                    assert 'replacements' in rule, "Rules must contain a 'replacements' entry"
                    for replacement in rule['replacements']:
                        assert 'column' in replacement, "Replacements must contain a 'column' entry"
                        assert 'repl' in replacement, "Replacements must contain a 'repl' entry"
                        index = columnToIndex[replacement['column']]
                        # Case one: regex pattern replacement
                        if 'pattern' in replacement:
                            row[index] = sub(replacement['pattern'], replacement['repl'], row[index])
                        # Case two: replace whole value
                        else:
                            row[index] = replacement['repl']

def getOptions():
    """ process comandline options """
    parser = argparse.ArgumentParser()

    parser.add_argument("-c", "--config", help="Specify the path to the configuration directory")

    parser.add_argument("-r", "--replace", help="Specify the path to the replacement_rules directory (if none, check config dir)")

    parser.add_argument("-o", "--output", default="hardware_staging.json", help="Specify the name and path of the output json file")

    parser.add_argument(
        "-m", "--mindate", metavar="DATE", type=parsetime, default=datetime.now() - timedelta(days=DAY_DELTA),
        help="Specify the minimum datestamp of archives to process (default {} days ago)".format(DAY_DELTA)
    )

    parser.add_argument(
        "-M", "--maxdate", metavar="DATE", type=parsetime, default=datetime.now() - timedelta(minutes=10),
        help="Specify the maximum datestamp of archives to process (default now())"
    )

    parser.add_argument("-a", "--all", action="store_true", help="Process all archives regardless of age")

    grp = parser.add_mutually_exclusive_group()
    grp.add_argument("-d", "--debug", dest="log", action="store_const", const=logging.DEBUG, default=logging.INFO,
                     help="Set log level to debug")
    grp.add_argument("-q", "--quiet", dest="log", action="store_const", const=logging.ERROR,
                     help="Only log errors")

    parser.add_argument(
        "-D", "--debugfile",
        help="""
        Specify the path to a log file. If this option is present the process will log a DEBUG level to this file.
        This logging is independent of the console log.
        """
    )

    args = parser.parse_args()
    return vars(args)

def getHardwareInfo():
    """Main entry point"""
    opts = getOptions()
    config = Config(opts['config'])
    outputFilename = opts['output']

    ##########
    if opts['config'] != None:
        print('CONFIG_DIR = ' + os.path.abspath(opts['config']))
    ##########

    data = []

    # TODO: Multiple resources?
    for resourcename, resource in config.resourceconfigs():

        ##########
        print('LOG_DIR = ' + resource['pcp_log_dir'])
        print('MIN_DATE = ' + str(opts['mindate']))
        ##########
        count = 0

        acache = PcpArchiveHardwareProcessor(resource)
        afind = PcpArchiveFinder(opts['mindate'], opts['maxdate'], opts['all'])
        for archive, fast_index, hostname in afind.find(resource['pcp_log_dir']):
            if not PcpArchiveHardwareProcessor.isJobArchive(archive):
                hw_info = PcpArchiveHardwareProcessor.getDataFromArchive(archive)
                if hw_info != None:
                    data.append(hw_info)
                ##########
                count += 1
                if count % 100 == 0:
                    print('count = ' + str(count))
                ##########

    HardwareStagingTransformer(data, replacementPath=opts['replace'], outputFilename=outputFilename)

    # with open(outputFilename, "w") as outFile:
    #     outFile.write(json.dumps(data, indent=4, separators=(',', ': ')))

if __name__ == "__main__":
    getHardwareInfo()
    