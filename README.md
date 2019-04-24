SUPReMM Processing Tools
========================

SUPReMM is a comprehensive open-source tool chain that provides resource
monitoring capabilities to users and managers of HPC systems.

The SUPReMM architecture comprises three major components:

* Software that runs directly on HPC compute nodes and periodically collects performance information.
* Software that uses the node-level performance data to generate job-level data.
* An [Open XDMoD][xdmod] module that enables the job-level information to be viewed and analyzed.

This repository contains the software that combines the node-level performance
data to generate job-level summary data.

Full details of the SUPReMM project are available on the [SUPReMM overview page][supremm]
in the Open XDMoD documentation.

This work was sponsored by NSF under grant numbers
[ACI 1203560][nsf-1203560], [ACI 1025159][nsf-1025159] and [ACI 1445806][nsf-1445806] for the XD Metrics Service (XMS) for NSF.

For more information, questions, feedback or bug reports send email to
`ccr-xdmod-help` at `buffalo.edu`.

Want to be notified about SUPReMM package releases and news? Subscribe to the
[XDMoD mailing list][listserv].

Software Build Requirements
---------------------------

This section provides instructions on how to create an RPM or source packages for
software development or debugging. The instructions for installing the released
packages are available on the [main website](https://supremm.xdmod.org/supremm-processing-install.html).

### Centos 7

Install the PCP repository configuration following the instructions on the [pcp packages
page][pcpbintray]. Install the EPEL repository configuration:

    yum install epel-release

Install the build dependencies:

    yum install rpm-build \
                gcc \
                numpy \
                scipy \
                python-devel \
                python2-pip \
                python2-mock \
                python-ctypes \
                python-pymongo \
                MySQL-python \
                Cython \
                python-pcp-3.12.2 \
                pcp-libs-devel-3.12.2

    pip install setuptools==36.4.0

Installation
------------

This project uses the [python setuptools][pydist] for package creation, and
the setup script is known to work with setuptools version 36.4.0 or later.
To install in a conda environment:

    conda create -n supremm python=2.7 cython numpy scipy
    source activate supremm
    python setup.py install 

RPM packages are created using:

    python setup.py bdist_rpm


Contributing
------------

We accept contributions via standard [github pull requests][ghpr].

This project is under active development with new features planned.
Please contact us via the `ccr-xdmod-help` at `buffalo.edu` email address
before you get started so that we can co-operate and avoid duplication of effort.

Overview (for developers)
-------------------------

Full details of how to install and use the software are available on the
[SUPReMM overview page][supremm] in the Open XDMoD documentation. This section
gives a very brief overview of the summarization software for software
developers. As always, the definitive reference is the source code itself.

The summarization software processing flow is approximately as follows:

- Initial setup including parsing configuration files, opening database connections, etc.
- Query an accounting database to get list of jobs to process and list of PCP archives containing data.
- For each job:
    - retrieve the PCP archives that cover the time period that the job ran;
    - extract the relevant datapoints from the PCP archives;
    - run the data through the **preprocessors**;
    - run the data through the **plugins**;
    - collect the output of the **preprocessors** and **plugins** and store in an output database.

**preprocessors** and **plugins** are both python modules that implement a
defined interface. The main difference between a preprocessor and a plugin is
that the preprocessors run first and their output is avialable to the plugin
code.

Each **plugin** is typically responsible for generating a job-level summmary for a PCP metric or group of PCP metrics. Each module
defines:
- an identifier for the output data;
- a list of PCP metrics;
- a mode of operation (either only process the first and last datapoints or process all data);
- an implementation of a processing function that will be called by the framework with the requested datapoints;
- an implementation of a function that will be called at the end to return the results of the analyis.

An example of a **plugin** is one that records the mean and maximum memory
usage for the job. Another example is a **plugin** that checks the temporal
variance of the L1D cache load rate to determine if the job failed prematurely.

The software that retrieves the job information from the accounting database
and writes to the output database is configurable. So, for example, you can
setup the software to write the job summary records to stdout for testing
purposes. The accounting database interface supports multiple accounting
databases (Open XDMoD being the main one).

If you are interested in doing plugin development, then a suggested starting
point is to look at some of the existing plugins. The simplest plugins, such as
the block device plugin (`supremm/plugins/Block.py`) use the framework-provided
implementation. A more complex example is the Slurm cgroup memory processor
(`supremm/plugins/SlurmCgroupMemory.py`) that contains logic to selectively
ignore certain datapoints and to do some non-trivial statistics on the data. 

If you are interested in understanding the full processing workflow, then the
starting point is the main() function in the `summarize_jobs.py` script.

License
-------

The SUPReMM processing tools package is an open source project released under
the [GNU Lesser General Public License ("LGPL") Version 3.0][lgpl3].

[lgpl3]:      http://www.gnu.org/licenses/lgpl-3.0.txt
[xdmod]:      http://xdmod.sourceforge.net/
[supremm]:    http://xdmod.sourceforge.net/supremm-overview.html
[nsf-1203560]:http://www.nsf.gov/awardsearch/showAward?AWD_ID=1203560
[nsf-1025159]:http://www.nsf.gov/awardsearch/showAward?AWD_ID=1025159
[nsf-1445806]:http://www.nsf.gov/awardsearch/showAward?AWD_ID=1445806
[listserv]:   http://listserv.buffalo.edu/cgi-bin/wa?SUBED1=ccr-xdmod-list&A=1
[ghpr]:       https://help.github.com/articles/using-pull-requests/
[pydist]:     https://setuptools.readthedocs.io/en/latest/
[pcpbintray]: https://bintray.com/pcp/
