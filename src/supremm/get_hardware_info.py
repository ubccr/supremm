#!/usr/bin/env python
""" Script that reads hardware info from the archives 
    and outputs the data into a json file
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

from supremm.account import DbArchiveCache
from supremm.xdmodaccount import XDMoDArchiveCache

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
        try:
            context = pmapi.pmContext(c_pmapi.PM_CONTEXT_ARCHIVE, archive)
            mdata = context.pmGetArchiveLabel()
            hostname = mdata.hostname.split('.')[0]
            record_time_ts = float(mdata.start)

            pmfg = pmapi.fetchgroup(c_pmapi.PM_CONTEXT_ARCHIVE, archive)

            fetchedData = {}

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
                    extObj[alias] = None
                    fetchedData[alias] = None
                    ##########
                    if alias != "nvidia":
                        print("alias = %s, exc = %s" % (alias, str(exc)))
                    ##########

            # fetch data until all metrics have been retrieved
            while not (all(metrics[metric]["alias"] in fetchedData for metric in metrics)):
                try:
                    pmfg.fetch()
                except pmapi.pmErr as exc:
                    if exc.message() == "End of PCP archive log":
                        # End of archive - fill in missing data with 'None'
                        for metric in metrics:
                            if metric not in fetchedData:
                                fetchedData[metric] = None
                        break

                for metric in metrics:
                    metricType = metrics[metric]["type"]
                    alias = metrics[metric]["alias"]
                    if (metricType == "item") and (alias not in fetchedData):
                        fetchedData[alias] = extObj[alias]()
                    elif (metricType == "indom") and (alias not in fetchedData) and (len(extObj[alias]()) > 0):
                        fetchedData[alias] = extObj[alias]()

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

            # Map the extend objects to the data to be collected
            data = {
                'record_time_ts': record_time_ts,
                'hostname': hostname,
                'core_count': fetchedData["ncpu"],
                'disk_count': fetchedData["ndisk"],
                'physmem': fetchedData["physmem"],
                'manufacturer': fetchedData["manufacturer"][0][2](),
                'numa_node_count': max([n[2]() for n in fetchedData["numa_mapping"]]) + 1,
                'ethernet_count': len([device[1] for device in fetchedData["ethernet"] if device[1] != "lo"]) if fetchedData["ethernet"] else 0
            }
            
            if infini:
                data['infiniband'] = dict(infini)
            if fetchedData["nvidia"]:
                data['gpu'] = {}
                for _, iname, value in fetchedData["nvidia"]:
                    data['gpu'][iname] = value()
            
            return data

        except pmapi.pmErr as exc:
            #pylint: disable=not-callable
            err = {"archive": archive, "error": exc.message()}
            print json.dumps(err) + ","
            return None
        

    @staticmethod
    def isJobArchive(archive):
        fname = os.path.basename(archive)
        return fname.startswith("job-")

# No changes
class PcpArchiveFinder(object):
    """ Helper class that finds all pcp archive files in a directory
        mindate is the minimum datestamp of files that should be processed
    """

    def __init__(self, mindate, maxdate, all=False):
        self.mindate = mindate if not all else None
        self.maxdate = maxdate if not all else None
        if self.mindate != None:
            self.minmonth = datetime(year=mindate.year, month=mindate.month, day=1) - timedelta(days=1)
        else:
            self.minmonth = None
        self.fregex = re.compile(
            r".*(\d{4})(\d{2})(\d{2})(?:\.(\d{2}).(\d{2})(?:[\.-](\d{2}))?)?\.index$")
        self.sregex = re.compile(r"^(\d{4})(\d{2})$")
        self.yearregex = re.compile(r"^\d{4}$")
        self.dateregex = re.compile(r"^(\d{4})-(\d{2})-(\d{2})$")

    def subdirok(self, subdir):
        """ check the name of a subdirectory and return whether to
            descend into it based on the name.
            @returns None if the name is not a datestamp
                     true if the name is a date that is >= the reference date
                     false if the name is a date that is < the reference
        """
        mtch = self.sregex.match(subdir)
        if mtch == None:
            return None

        if self.minmonth == None:
            return True

        subdirdate = datetime(year=int(mtch.group(1)), month=int(mtch.group(2)), day=1)

        return subdirdate > self.minmonth

    def filenameok(self, filename):
        """ parse filename to get the datestamp and compare with the reference datestamp
        """
        if self.mindate == None:
            return True

        mtch = self.fregex.match(filename)
        if mtch == None:
            logging.error(
                "Unparsable filename %s processing anyway.", filename)
            return True

        if mtch.group(4) != None and mtch.group(5) != None:
            filedate = datetime(year=int(mtch.group(1)), month=int(mtch.group(2)), day=int(mtch.group(3)), hour=int(mtch.group(4)), minute=int(mtch.group(5)))
        else:
            filedate = datetime(year=int(mtch.group(1)), month=int(mtch.group(2)), day=int(mtch.group(3)))

        if self.maxdate == None:
            return filedate > self.mindate
        else:
            return filedate > self.mindate and filedate < self.maxdate

    def ymdok(self, year, month=12, day=None):
        """ Check candidate dates for YYYY/MM/DD directory structure """
        if len(year) != 4:
            return None

        try:
            yyyy = int(year)
            mm = int(month)

            if day == None:
                # Some datetime arithmetic to get the last day of the month
                tmpdate = datetime(year=yyyy, month=mm, day=28, hour=23, minute=59, second=59) + timedelta(days=4)
                dirdate = tmpdate - timedelta(days=tmpdate.day)
            else:
                dirdate = datetime(year=yyyy, month=mm, day=int(day), hour=23, minute=59, second=59)

        except ValueError:
            return None

        if self.mindate == None:
            return True

        return dirdate > self.mindate

    @staticmethod
    def listdir(pathname):
        """ Return sorted list of paths under the supplied path. I/O errors
            such as permission denied are logged at error level and an empty
            list is returned """

        dirents = []
        try:
            dirents = os.listdir(pathname)
        except OSError as err:
            logging.error(str(err))

        dirents.sort()

        return dirents

    def find(self, topdir):
        """ main entry for the archive file finder. There are multiple different
            directory structures supported. The particular directory stucture
            is automatically detected based on the directory names. """

        if topdir == "":
            return

        dirs = self.listdir(topdir)

        yeardirs = []
        hostdirs = []
        for dirpath in dirs:
            if dirpath == 'README':
                continue

            if self.yearregex.match(dirpath):
                yeardirs.append(dirpath)
            else:
                hostdirs.append(dirpath)

        for archivefile, fast_index, hostname in self.parse_by_date(topdir, yeardirs):
            yield archivefile, fast_index, hostname

        for archivefile, fast_index, hostname in self.parse_by_host(topdir, hostdirs):
            yield archivefile, fast_index, hostname

    def parse_by_date(self, top_dir, year_dirs):
        """ find all archives that are organised in a directory
            structure like:
                [top_dir]/[YYYY]/[MM]/[HOSTNAME]/[YYYY-MM-DD]
        """

        for year_dir in year_dirs:
            year_dir_ok = self.ymdok(year_dir)
            if year_dir_ok is True:
                for month_dir in self.listdir(os.path.join(top_dir, year_dir)):
                    if self.ymdok(year_dir, month_dir) is True:
                        for host_dir in self.listdir(os.path.join(top_dir, year_dir, month_dir)):
                            for date_dir in self.listdir(os.path.join(top_dir, year_dir, month_dir, host_dir)):
                                date_match = self.dateregex.match(date_dir)
                                if date_match and self.ymdok(date_match.group(1), date_match.group(2), date_match.group(3)):
                                    dirpath = os.path.join(top_dir, year_dir, month_dir, host_dir, date_dir)
                                    filenames = self.listdir(dirpath)
                                    for filename in filenames:
                                        if filename.endswith(".index") and self.filenameok(filename):
                                            yield os.path.join(dirpath, filename), True, host_dir


    def parse_by_host(self, topdir, hosts):
        """ find all archive files that are organised in a directory
            structure like:
               [topdir]/[HOSTNAME]/[YYYY]/[MM]/[DD]/{archive files}

            also support:
               [topdir]/[HOSTNAME]/[YYYYMM]/{archive files}

            and:
               [topdir]/[HOSTNAME]/{archive files}
        """

        starttime = time.time()
        hostcount = 0
        currtime = starttime

        for hostname in hosts:
            hostdir = os.path.join(topdir, hostname)
            listdirtime = 0.0
            yieldtime = 0.0
            t1 = time.time()
            datedirs = self.listdir(hostdir)
            listdirtime += (time.time() - t1)

            for datedir in datedirs:
                t1 = time.time()

                yeardirOk = self.ymdok(datedir)

                if yeardirOk is True:
                    for monthdir in self.listdir(os.path.join(hostdir, datedir)):
                        if self.ymdok(datedir, monthdir) is True:
                            for daydir in self.listdir(os.path.join(hostdir, datedir, monthdir)):
                                if self.ymdok(datedir, monthdir, daydir) is True:
                                    for filename in self.listdir(os.path.join(hostdir, datedir, monthdir, daydir)):
                                        if filename.endswith(".index") and self.filenameok(filename):
                                            beforeyield = time.time()
                                            yield os.path.join(hostdir, datedir, monthdir, daydir, filename), True, hostname
                                            yieldtime += (time.time() - beforeyield)
                    listdirtime += (time.time() - t1 - yieldtime)
                    continue
                elif yeardirOk is False:
                    continue
                # else fall through to check other formats
                elif yeardirOk is None:
                    datedirOk = self.subdirok(datedir)
                    if datedirOk is None:
                        if datedir.endswith(".index") and self.filenameok(datedir):
                            yield os.path.join(hostdir, datedir), False, None
                    elif datedirOk is True:
                        dirpath = os.path.join(hostdir, datedir)
                        filenames = self.listdir(dirpath)
                        for filename in filenames:
                            if filename.endswith(".index") and self.filenameok(filename):
                                yield os.path.join(dirpath, filename), False, None

            hostcount += 1
            lasttime = currtime
            currtime = time.time()
            logging.info("Processed %s of %s (hosttime %s, listdirtime %s, yieldtime %s) total %s estimated completion %s",
                         hostcount, len(hosts), currtime-lasttime, listdirtime, yieldtime, currtime - starttime,
                         datetime.fromtimestamp(starttime) + timedelta(seconds=(currtime - starttime) / hostcount * len(hosts)))

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

def getoptions():
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
    opts = getoptions()
    config = Config(opts['config'])
    outputFilename = opts['output']

    ##########
    if opts['config'] != None:
        print('CONFIG_DIR = ' + os.path.abspath(opts['config']))
    ##########

    data = []

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
                if len(data) > 100:
                    break
                count += 1
                if count % 1000 == 0:
                    print('count = ' + str(count))
                ##########

    HardwareStagingTransformer(data, replacementPath=opts['replace'], outputFilename=outputFilename)

    # with open(outputFilename, "w") as outFile:
    #     outFile.write(json.dumps(data, indent=4, separators=(',', ': ')))

if __name__ == "__main__":
    getHardwareInfo()
    