#!/usr/bin/env python3
""" setup script for SUPReMM job summarization utilities """
import sys
import os
from setuptools import setup, find_packages, Extension
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
    version='2.0.0',
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
        'supremm': ['assets/modw_supremm.sql', 'assets/*schema.json', '*.pxd', '*.pyx'],
        'supremm.datasource.pcp.pcpcinterface': ['*.pxd', '*.pyx']
    },
    data_files=[
        (confpath,                         ['config/config.json', 'config/prometheus/mapping.json']),
        ('share/supremm/templates/slurm',       ['config/templates/slurm/slurm-epilog',  'config/templates/slurm/slurm-prolog']),
        ('share/supremm/templates/hotproc',       ['config/templates/hotproc/hotproc.conf']),
        ('share/supremm/templates/pmlogger',    ['config/templates/pmlogger/control',    'config/templates/pmlogger/pmlogger-supremm.config'])
    ],
    scripts=[
             'src/supremm/supremm_update'
    ],
    entry_points={
        'console_scripts': [
            'gen-pmlogger-control.py = supremm.gen_pmlogger_control:main',
            'summarize_jobs.py = supremm.summarize_jobs:main',
            'summarize_mpi.py = supremm.summarize_mpi:main',
            'indexarchives.py = supremm.datasource.pcp.indexarchives:runindexing',
            'account.py = supremm.account:runingest',
            'supremmconf.py = supremm.supremmconf:main',
            'supremm-setup = supremm.supremm_setup:main',
            'supremm-upgrade = supremm.supremm_upgrade:main',
            'ingest_jobscripts.py = supremm.ingest_jobscripts:main'

        ]
    },
    install_requires=[
        'numpy',
        'PyMySQL',
        'pcp',
        'Cython',
        'scipy',
        'pymongo',
        'pytz',
        'requests'
    ],
    ext_modules=cythonize([
        Extension("supremm.datasource.pcp.pcpcinterface.pcpcinterface", ["src/supremm/datasource/pcp/pcpcinterface/pcpcinterface.pyx"], libraries=["pcp"], include_dirs=[numpy.get_include()])
    ])
)

if IS_RPM_BUILD:
    os.unlink('.rpm_install_script.txt')
