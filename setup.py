#!/usr/bin/env python
""" setup script for SUPReMM job summarization utilities """
from distutils.core import setup, Extension
import sys
import os
from Cython.Distutils import build_ext

# For rpm-based builds want the configuration files to
# go in the standard location
if 'bdist_rpm' in sys.argv or 'RPM_BUILD_ROOT' in os.environ:
    confpath = '/etc/supremm'
else:
    confpath = 'etc/supremm'


setup(name='supremm',
      version='1.0.0',
      description='SUPReMM Job Summarization Utilities',
      long_description='Utilities for generating job-level summary data from host level PCP archives.\nAlso includes template configuration files for running PCP on an HPC system.',
      license='LGPLv3',
      author='Joseph P White',
      author_email='jpwhite4@buffalo.edu',
      url='https://github.com/ubccr/supremm',
      packages=['supremm', 'supremm.plugins', 'supremm.preprocessors'],
      data_files=[(confpath,                         ['config/config.json']),
                  ('share/supremm/templates/slurm',       ['config/templates/slurm/slurm-epilog',  'config/templates/slurm/slurm-prolog']),
                  ('share/supremm/templates/pmlogger',    ['config/templates/pmlogger/control',    'config/templates/pmlogger/pmlogger-supremm.config']),
                  ('share/supremm/templates/pmie',        ['config/templates/pmie/control',        'config/templates/pmie/pmie-supremm.config',
                                                           'config/templates/pmie/pcp-restart.sh', 'config/templates/pmie/procpmda_check.sh']),
                  ('share/supremm/templates/pmda-logger', ['config/templates/pmda-logger/logger.conf']),
                  ('share/supremm/setup/', ['assets/modw_supremm.sql', 'assets/mongo_setup.js'])
      ],
      scripts=['supremm/gen-pmlogger-control.py',
               'supremm/summarize_jobs.py', 
               'supremm/indexarchives.py',
               'supremm/account.py',
               'supremm/supremmconf.py',
               'supremm/supremm_update',
               'supremm/ingest_jobscripts.py'],
      requires=['numpy',
                'MySQLdb',
                'pcp',
                'Cython'],
      cmdclass = {'build_ext': build_ext},
      ext_modules=[Extension("supremm.puffypcp", ["supremm/puffypcp/puffypcp.pyx"], libraries=['pcp'])]
     )
