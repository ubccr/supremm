"""
Script to generate remote host portion of pmlogger control file.

Usage: cat [hostlist] | python gen-pmlogger-control.py

Author: Andrew E. Bruno <aebruno2@buffalo.edu>
"""
import fileinput

pcp_archive_dir = '/data/pcp-logs'
pmlogger_config = 'pmlogger-config.ubccr'

def main():
    for host in fileinput.input():
        host = host.rstrip()
        print("%s          n   n   %s/%s               -c ./%s" % (
                host,
                pcp_archive_dir,
                host,
                pmlogger_config
            )
        )

if __name__ == '__main__':
    main()
