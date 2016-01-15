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

### Centos 6 & 7

Install the PCP repository configuration following the instructions on the [pcp packages
page][pcpbintray]. Install the EPEL repository configuration:

    yum install epel-release

Install the build dependencies:

    yum install rpm-build pcp-libs-devel gcc python-devel

Installation
------------

This project uses the [python distutils][pydist] for package creation.

    python setup.py install --prefix=PATH_TO_INSTALL_DIR

RPM packages are created using:

    python setup.py bdist_rpm


Contributing
------------

We accept contributions via standard [github pull requests][ghpr].

This project is under active development with new features planned.
Please contact us via the `ccr-xdmod-help` at `buffalo.edu` email address
before you get started so that we can co-operate and avoid duplication of effort.

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
[pydist]:     https://docs.python.org/2.7/distutils/index.html
[pcpbintray]: https://bintray.com/pcp/
