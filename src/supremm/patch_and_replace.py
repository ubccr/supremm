#!/usr/bin/env python
""" Script that patches missing data in a staging table
    and runs replacement on the data

    @author Max Dudek <maxdudek@gmail.com>
"""

import json
import os
import sys
import fnmatch
import logging
import argparse
from re import sub, search
from supremm.scripthelpers import setuplogger
from get_hardware_info import getStagingColumns

# TODO: Remove
from math import ceil

# Initialize staging columns and build a dictionary mapping column names to index
STAGING_COLUMNS = getStagingColumns()
columnToIndex = {}
for i in range(len(STAGING_COLUMNS)):
    columnToIndex[STAGING_COLUMNS[i]] = i

def loadJson(filename):
    with open(filename, 'r') as inFile:
        return json.load(inFile)

class StagingPatcher(object):

    def __init__(self, stagingData, maxgap=-1, mode='gpu'):

        modes = {
            'gpu': {
                'indicator_column': 'gpu_device_count',                         # The column used to test if data is missing
                'indicator_value': 0,                                           # The value the indicator column takes if data is missing
                'columns_to_patch': ['gpu_device_count', 'gpu_device_name'],    # The list of columns which need to be patched
            },
            'ib': {
                'indicator_column': 'ib_device_count',
                'indicator_value': 0,
                'columns_to_patch': ['ib_device_count', 'ib_device', 'ib_ca_type', 'ib_ports'],
            },
        }

        if mode not in modes:
            logging.error('The patching mode "%s" is not a valid mode', mode)
            return
        
        logging.info('Patching data using mode "%s"', mode)

        settings = modes[mode]
        self.indicatorColumn = settings['indicator_column']
        self.indicatorValue = settings['indicator_value']
        self.columnsToPatch = settings['columns_to_patch']

        self.indexsToPatch = [columnToIndex[c] for c in self.columnsToPatch]
        self.indicatorIndex = columnToIndex[self.indicatorColumn]

        self.stagingData = stagingData
        self.maxgap = maxgap
    
        self.patch()

    def patch(self):

        # Sort the data by hostname, then by timestamp
        self.stagingData.sort(key=lambda x: (x[columnToIndex['hostname']], x[columnToIndex['record_time_ts']]))

        # Initialize state variables
        self.lastIndex = None       # The last index of the current hostname to have complete data (None if not yet seen)
        self.currentHostname = None # The current hostname in the iteration

        self.hostnameIndex = columnToIndex['hostname']
        self.timestampIndex = columnToIndex['record_time_ts']

        for i in range(len(self.stagingData)):
            self.currentIndex = i

            if self.patchingShouldOccur():
                self.fillInMissingData()
            self.resetState()

    def patchingShouldOccur(self):
        """ Returns True if patching needs to occur at the current index """
        #TODO Patch first row 
        index = self.currentIndex
        row = self.stagingData[index]

        # Check hostname (make sure it is the same as the current hostname)
        if self.hostnameChanged():
            return False

        # Check if currently tracking
        if self.lastIndex == None:
            return False
        
        # Check if data is missing (can't patch if it still is)
        if self.isMissingData():
            return False
        
        # Check if it's been too long since the data was last there
        if self.maxTimeExceeded() and self.getGapLength() > 2:
            return False
        
        # Check if there's actually a gap of 2 or greater
        if self.getGapLength() <= 1:
            return False
        
        return True

    def getGapLength(self):
        """ Return the number of records read since the last record with data """
        return self.currentIndex - self.lastIndex
        
    def hostnameChanged(self):
        """ Returns True if the hostname is different than the last row """
        row = self.stagingData[self.currentIndex]
        return row[self.hostnameIndex] != self.currentHostname

    def maxTimeExceeded(self):
        """ Returns true if the time difference between the current row and the row specified by lastIndex
        (i.e., the last row to contain data) is greater that the max gap in data allowed (specified by self.maxgap)"""

        # If maxgap is negative, the time is never exceeded (no maximum)
        if self.maxgap < 0:
            return False

        row = self.stagingData[self.currentIndex]

        SECONDS_PER_DAY = 86400
        maxSeconds = self.maxgap * SECONDS_PER_DAY
        timeGap = row[self.timestampIndex] - self.stagingData[self.lastIndex][self.timestampIndex]
        return timeGap > maxSeconds

    def isMissingData(self):
        """ Returns true if the row is missing data, false if data is intact """
        row = self.stagingData[self.currentIndex]
        return row[self.indicatorIndex] == self.indicatorValue

    def fillInMissingData(self):
        index = self.currentIndex
        correctRow = self.stagingData[self.lastIndex] # The last row to have full data

        # Missing data rows
        for j in range(self.lastIndex+1, index):
            row = self.stagingData[j]
            # Missing data columns
            for c in self.indexsToPatch:
                row[c] = correctRow[c]

        # Debug message
        numberOfRows = index - self.lastIndex - 1
        if numberOfRows > 1:
            logging.debug('Patched %d rows', numberOfRows)

    def resetState(self):
        """ Resets the iteration state based on data at index """
        row = self.stagingData[self.currentIndex]
        
        if self.isMissingData():
            if self.hostnameChanged():
                # Reset if the hostname changed, otherwise stay the same
                self.lastIndex = None
        else:
            self.lastIndex = self.currentIndex

        self.currentHostname = row[self.hostnameIndex]

def replaceData(stagingData, replacementPath):
    # Look for replacement file
    if (replacementPath is not None) and os.path.isfile(os.path.join(replacementPath, 'replacement_rules.json')):
        replacementFile = os.path.join(replacementPath, 'replacement_rules.json')
        replacementRules = loadJson(replacementFile)
    else:
        logging.warning('No replacement_rules.json file found in directory %s. Replacement will not be applied to staging columns.', replacementPath)
        return stagingData
    
    logging.info('Applying replacement rules to staging columns using file %s...', replacementFile)

    for row in stagingData:
        for rule in replacementRules:
            # Check if conditions are true
            conditionsMet = True
            if 'conditions' in rule:
                for condition in rule['conditions']:
                    assert 'column' in condition, 'Conditions must contain a "column" entry'
                    value = row[columnToIndex[condition['column']]]
                    reverse = condition.get('reverse', False) # If 'reverse' is true, then the condition must be FALSE to replace
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

    return stagingData

def getOptions():
    """ process comandline options """
    parser = argparse.ArgumentParser()

    parser.add_argument("-i", "--input", help="Path to input directory", required=True)

    parser.add_argument("-f", "--filepattern", default="*.json", help="The Unix filepattern of input files")

    parser.add_argument('-o', '--output', default='hardware_staging.json', help='Specify the name and path of the output json file')

    parser.add_argument("-P", "--patch", action="store_true", help="Patch the data")

    parser.add_argument("-M", "--maxgap", type=int, default=40, help="The maximum length of time to patch over, in days (-1 for no gap)")

    parser.add_argument("-t", "--truncate", action="store_true", help="Only output the last M days of data, where M is the maxgap")

    parser.add_argument("-R", "--replace", help="Specify the path to the repalcement rules directory (if replacement should occur)")

    grp = parser.add_mutually_exclusive_group()
    grp.add_argument('-d', '--debug', dest='log', action='store_const', const=logging.DEBUG, default=logging.INFO,
                    help='Set log level to debug')
    grp.add_argument('-q', '--quiet', dest='log', action='store_const', const=logging.ERROR,
                    help='Only log errors')

    args = parser.parse_args()

    if not (args.replace or args.patch):
        parser.error('No action requested, add --replace or --patch')

    return vars(args)

def main():
    """ Main entry point """
    opts = getOptions()

    setuplogger(opts['log'])

    # Load data from input files
    rawData = []
    inputPath = os.path.abspath(opts['input'])
    for f in os.listdir(inputPath):
        inputFile = os.path.join(inputPath, f)
        if fnmatch.fnmatch(f, opts['filepattern']):
            logging.info('Loading data from %s', inputFile)
            rawData.append(loadJson(inputFile))
    if rawData == []:
        logging.error('No files found using pattern %s', os.path.join(inputPath, opts['filepattern']))
        exit()

    # Check staging columns
    if STAGING_COLUMNS != rawData[0][0]:
        logging.error("Staging columns don't match expected columns.\n\tExpected columns: %s\n\tColumns in file: %s", str(STAGING_COLUMNS), str(rawData[0][0]))
        exit()

    # Combine input files to generate staging data
    stagingData = []
    for inputData in rawData:
        stagingData.extend(inputData[1:])   # Strip header from input data
    del rawData[:]

    if opts['patch']:
        # TODO: Get rid of this (adjust the memory)
        for i in range(len(stagingData)):
            row = stagingData[i]
            mem = columnToIndex['physmem']
            if row[mem] > 4000:
                row[mem] = int(ceil(row[mem] / 1024.0 / 2.0) * 2)
            if row[mem] % 2 != 0:
                row[mem] = int(ceil(row[mem] / 2.0) * 2)

        stagingData = StagingPatcher(stagingData, maxgap=opts['maxgap'], mode='gpu').stagingData
        stagingData = StagingPatcher(stagingData, maxgap=opts['maxgap'], mode='ib').stagingData
    
    if opts['replace'] != None:
        stagingData = replaceData(stagingData, opts['replace'])


    # Truncate
    if opts['truncate'] and opts['maxgap'] > 0:
        logging.info('Truncating data to last %d days...', opts['maxgap'])
        stagingData.sort(key=lambda x: x[columnToIndex['record_time_ts']])  # Sort by timestamp

        lastTimestamp = stagingData[-1][columnToIndex['record_time_ts']]
        SECONDS_PER_DAY = 86400
        earliestTimestamp = lastTimestamp - ((opts['maxgap'] + 1) * SECONDS_PER_DAY)

        for i in range(len(stagingData)):
            timestamp = stagingData[i][columnToIndex['record_time_ts']]
            if timestamp >= earliestTimestamp:
                stagingData = stagingData[i:]   # Truncate
                break

    stagingData.insert(0, STAGING_COLUMNS)   # Add header row back to result

    logging.info('Writing result to %s', opts['output'])    

    # Output staging rows to file
    with open(opts['output'], 'w') as outFile:
        outFile.write(json.dumps(stagingData, indent=4, separators=(',', ': ')))

if __name__ == '__main__':
    main()
