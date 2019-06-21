import os
import json
import sys

def jsonLoad(filename):
    with open(filename, "r") as inFile:
        return json.load(inFile)

def main():
    EXPECTED_OUTPUT_FILE = 'tests/hardware_info_tests/expected_output.json'

    SCRIPT = 'get_hardware_info.py'
    OUT_FILE = 'tests/hardware_info_tests/out.json'
    LOG_FILE = 'tests/hardware_info_tests/hardware_test.log'
    RESOURCE = 'phillips'
    MIN_DATE = '2019-06-17'
    MAX_DATE = '2019-06-18'
    
    COMMAND = '%s -q -o %s -D %s -r %s -m %s -M %s' % (SCRIPT, OUT_FILE, LOG_FILE, RESOURCE, MIN_DATE, MAX_DATE)

    os.system(COMMAND)

    actualOutput = jsonLoad(OUT_FILE)
    expectedOutput = jsonLoad(EXPECTED_OUTPUT_FILE)

    if (actualOutput != expectedOutput):
        sys.exit(1)
    
    # Success - return 0
    sys.exit(0)

if __name__ == '__main__':
    main()
