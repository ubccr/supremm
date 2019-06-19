import os

def main():
    CONFIG_DIR = 'tests/hardware_info_tests'
    OUT_FILE = 'tests/hardware_info_tests/out.json'
    LOG_FILE = 'tests/hardware_info_tests/hardware_test.log'
    SCRIPT = 'src/supremm/get_hardware_info.py'
    COMMAND = 'python %s -q -o %s -D %s -c %s -a' % (SCRIPT, OUT_FILE, LOG_FILE, CONFIG_DIR)

    os.system(COMMAND)



if __name__ == '__main__':
    main()