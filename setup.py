#!/usr/bin/env python
""" setup script for SUPReMM job summarization utilities """
from setuptools import setup, find_packages, Extension
import sys
import os
import numpy

from Cython.Build import cythonize

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


setup(
    name='supremm',
    version='1.1.0',
    description='SUPReMM Job Summarization Utilities',
    long_description='Utilities for generating job-level summary data from host level PCP archives.\nAlso includes template configuration files for running PCP on an HPC system.',
    license='LGPLv3',
    author='Joseph P White',
    author_email='jpwhite4@buffalo.edu',
    url='https://github.com/ubccr/supremm',

    zip_safe=False,
    package_dir={'': 'src'},
    packages=find_packages(where='src'),
    package_data={
        'supremm': ['migrations/1.0-1.1/modw_supremm.sql', 'assets/modw_supremm.sql', 'assets/mongo_setup.js', '*.pxd', '*.pyx'],
        'supremm.pcpcinterface': ['*.pxd', '*.pyx'],
        'supremm.pypmlogextract': ['*.pxd', '*.pyx']
    },
    data_files=[
        (confpath,                         ['config/config.json']),
        ('share/supremm/templates/slurm',       ['config/templates/slurm/slurm-epilog',  'config/templates/slurm/slurm-prolog']),
        ('share/supremm/templates/pmlogger',    ['config/templates/pmlogger/control',    'config/templates/pmlogger/pmlogger-supremm.config']),
        ('share/supremm/templates/pmie',        ['config/templates/pmie/control',        'config/templates/pmie/pmie-supremm.config',
                                                 'config/templates/pmie/pcp-restart.sh', 'config/templates/pmie/procpmda_check.sh']),
        ('share/supremm/templates/pmda-logger', ['config/templates/pmda-logger/logger.conf']),
    ],
    scripts=[
             'src/supremm/supremm_update'
    ],
    entry_points={
        'console_scripts': [
            'gen-pmlogger-control.py = supremm.gen_pmlogger_control:main',
            'summarize_jobs.py = supremm.summarize_jobs:main',
            'summarize_mpi.py = supremm.summarize_mpi:main',
            'indexarchives.py = supremm.indexarchives:runindexing',
            'account.py = supremm.account:runingest',
            'supremmconf.py = supremm.supremmconf:main',
            'supremm-setup = supremm.supremm_setup:main',
            'supremm-upgrade = supremm.supremm_upgrade:main',
            'ingest_jobscripts.py = supremm.ingest_jobscripts:main'

        ]
    },
    install_requires=[
        'numpy',
        'MySQL-python',
        'pcp',
        'Cython',
        'scipy',
        'pymongo',
        'pytz',
        'tzlocal'
    ],
    ext_modules=cythonize([
        Extension("supremm.pcpcinterface.pcpcinterface", ["src/supremm/pcpcinterface/pcpcinterface.pyx"], libraries=["pcp"], include_dirs=[numpy.get_include()]),
        Extension("supremm.pypmlogextract.pypmlogextract", ["src/supremm/pypmlogextract/pypmlogextract.pyx"])
    ])
)

if IS_RPM_BUILD:
    os.unlink('.rpm_install_script.txt')
