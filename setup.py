#!/usr/bin/env python
""" setup script for SUPReMM job summarization utilities """
from distutils.core import setup, Extension
import sys
import os
from Cython.Distutils import build_ext

# For rpm-based builds want the configuration files to
# go in the standard location. Also need to rewrite the file list so that
# the config filesa are listed as %config(noreplace)
IS_RPM_BUILD = False
if 'bdist_rpm' in sys.argv or 'RPM_BUILD_ROOT' in os.environ:
    IS_RPM_BUILD = True
    confpath = '/etc/supremm'
    with open('.rpm_install_script.txt', 'w') as fp:
        fp.write('%s %s install -O1 --root=$RPM_BUILD_ROOT --record=INSTALLED_FILES\n' % (sys.executable, os.path.basename(sys.argv[0])))
        fp.write('sed -i \'s#^\\(%s\\)#%%config(noreplace) \\1#\' INSTALLED_FILES\n' % (confpath, ))
else:
    confpath = 'etc/supremm'


setup(name='supremm',
      version='1.0.2',
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
               'supremm/supremm-setup',
               'supremm/ingest_jobscripts.py'],
      requires=['numpy',
                'MySQLdb',
                'pcp',
                'Cython'],
      cmdclass = {'build_ext': build_ext},
      ext_modules=[Extension("supremm.puffypcp", ["supremm/puffypcp/puffypcp.pyx"], libraries=['pcp'])]
     )

if IS_RPM_BUILD:
    os.unlink('.rpm_install_script.txt')
