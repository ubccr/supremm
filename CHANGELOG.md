# Changelog

## [1.3.0] - 2019-09-30

### Added
- Added IPMI power timeseries plugin that generates timeseries data from
  IPMI power measurements.
- Add component tests for the performance counter plugins.

### Changed
- Updated RPM dependencies to match the official PCP version in RedHat/Centos 7.
- Update the CI build to test against XDMoD version 8.1.
- Improved support for hardware performance counters in the various performance
  counter plugins.

### Fixed
- Fixed bug in the performance counter preprocessor that caused it to
  incorrectly mark the performance counters as disabled by user.
- Fixed incorrect unit in schema definition for the block device timeseries metric.

## [1.2.0] - 2019-04-24

### Added
- Added GPU power plugin that summarizes the power usage for nvidia GPUs.
- Added IPMI power plugin that summarizes the power usage from impi.
- Added support for scanning the YYYY/MM/HOSTNAME/YYYY-MM-DD directory format for PCP archives.
- Added `--dry-run` option to `indexarchives.py` script (used for testing purposes).

### Changed
- Removed deprecated code that supported python 2.6.
- Updated templates to use the new recommended directory format.
- Updated test harness to work with current plugins.
- Database access code now sets the `local_infile` flag (needed for MySQL driver version 2.0.0 or later).

### Fixed
- Summarization script now only includes job start and end archives that are
  within 30 seconds of the job start and end. This mitigates a bug caused if the
  resource manager reuses the same job id for two different jobs.
- The summarization software now skips corrupt PCP archives and will try to
  continue processing the valid archives. Previously processing would stop.
- If a PCP archive has data missing at a timestep the framework will skip the timestep and
  continue processing the archive. Previously processing would stop.
- The Slurm Proc preprocessor now filters non-unicode characters from command names.
- Improvements to error handing in preprocessors and in the `perfevent` plugin.


## [1.1.0] (2) - 2018-12-07

### Fixed
- Fix dependency list for the RPM build.

## [1.1.0] - 2018-10-31

### Added
- Added support for XDMoD version 8.0.
- Added `--dry-run` option to `summarize_jobs.py` script (used for testing purposes).
- Added extra options to `summarize_jobs.py` to support more fine-grained selection of jobs to process
- Added `supremm-upgrade` script to facilitate database migrations needed for a 1.0.5 to 1.1.0 upgrade.
- Added multiprocessing support to `indexarchives.py`.
- Added option to `indexarchives.py` to estimate the archive timestamp of job level archives from the filename. This dramatically improves
 the performance on parallel filesystems that have large number of files per directory.
- Added plugin that detects periodic patterns in timeseries data.
- Added GPU usage timeseries plugin.
- Added AMD Interlagos support to the plugins that use hardware performance counters.
- Added effective CPU usage metrics to the CPU usage plugin. This generates CPU usage statistics for 
  the subset of CPUs that had any usage during a job.
- Added `summarize_mpi.py` script that uses MPI for process management. This can be used on an HPC cluster to summarize jobs in parallel across multiple compute nodes.
- Added ability to preprocess counter metrics that have < 64 bit range to 64 bit range counters.
- Added ability to call the dynamic library version of `pmlogextract`. This
  mode of operation is intended to be used when running the summarization
  software as an MPI job on a compute resource that does not allow python-based
  MPI software to execute the `fork()` system call.

### Changed
- Updated PCP configuration templates.
- Rewrote the main kernel of the summarization software in Cython. This improves the performance of the software.
- Changed structure of the database tables that store PCP archive metadata. This improves the query performance.
- Changed load balancing algorithm in multiprocessing mode to more evenly distribute work among processes.
- Job summary documents now record the time when they were created.
- Improved performance of the `SlurmProc` preprocessor.
- Changed the process detection algorithm in `SlurmProc` to output processes in frequency order.

### Fixed
- Improved error handling for invalid data in PCP archives in several plugins (#172, #164, #135)
- `indexarchives.py` script no longer exits if an unreadable file or directory is seen.
- Job script parser now handles parsing PBS/Torque job array elements.
- Improved error handling in `summarize_jobs.py` if the connection to the mysql server closes during processing.

### Misc
- Centos 6 and python 2.6 are no longer supported.

## [1.0.5] - 2018-10-26

### Fixed
- Fix issue with the indexarchives script parsing PBS/Torque style job identifiers in PCP log filenames.
  
## [1.0.4] - 2017-11-22

### Fixed
- Update to array indexing for compatibility with numpy >= 1.12.0

## [1.0.3] - 2017-08-01

### Changed
- Updated text content of indexarchives debug message to clarify meaning of ignored archives.

### Fixed
- Fix issue with timeseries documents not being saved with the Centos 6 EPEL
  version of MongoDB (2.4). It is likely that this issue affects newer versions
  of MongoDB too.


## [1.0.2] - 2017-01-26

### Added

- Added support for indexing archive directories with a YYYY/MM/DD format
  directory structure.
- Added a `file` output setting for the outputter. This option is intended to
  be used for debug purposes.
- Added a hardware inventory preprocessor that records the hardware information
  from the pcp archives.
- Added support for per-node metrics for the CPU plugin.
- Added support for per-node memory metrics.
- Added support for load average metrics.

### Changed

- Indexing script defaults to ignoring archives that are less than 10 minutes
  old (based on filename). This reduces the likelyhood of the race condition
  where an archive exists but contains no data. The maxdate command line flag can
  be used to override this default.

### Fixed

- Removed spurious print to stdout in the MongoOutput class
- Improve handling of missing data for the NFS timeseries plugin.
- Improve handling of missing data for the Slurm cgroup memory plugin.
- Fix errors in schema description and add missing metric documentation.
- Allow the output configuration parameter `type` as a synonym for `db_engine`.

## [1.0.1] - 2016-08-16

### Added

- Added interactive setup script that generates a configuration file and sets
  up the MySQL and MongoDB databases.
- Added support for reading MongoDB settings from the XDMoD configuration file.
- Added timeseries metrics for memory bandwidth, block device and total memory usage.
- Added command line options to the archive indexer script to add limiting by
  max date and added ability to log debug messages to a file.

### Changed

- Changed the indexarchive script to use os.listdir() instead of os.walk().
  This has a significant performance improvement when scanning files on
  filesystems that have slow stat() syscalls, such as parallel filesystems or
  network-attached storage.
- Changed the name of the memory usage timeseries metric to make it clearer (now
  that the total memory usage metric has been added).  Also improved the
  documentation of metric to clarify the datasource.

### Fixed

- The CPU plugin now sets the correct error code for short jobs that have
  insufficient CPU information. Previously the CPU metrics would report NaN.
- Fix issue where the SIMD timeseries plugin would not correctly output data
  for the individual nodes and CPUs.
- The SLURM process list plugin now limits the total number processes reported
  to 150. This mitigates an issue where jobs with a huge number of processes
  would result in a summary document that exceeds the MongoDB maximum document
  size.

## [1.0.0] - 2016-05-23

### Added

- Support for Centos/RedHat 6 (with python 2.6).
- Add support cgroup memory statistics for cgroups created by the Slurm cgroup plugin.
- Add NFS metrics plugin.
- Allow preprocessors to generate output that is included in the job summary.
- Added support for PCP metrics that are strings.
- Directory indexer now filters files based on directory name.
- CPU timeseries plots now only include the cores that the job was assigned (if this information is available).

### Changed

- Configuration settings for MongoDB changed to allow connections to databases that require authentication.
- Now uses the archives that are created at job prolog and epilog time to
  determine job time window.

### Fixed

- Fix error where the MySQL database driver settings were incorrectly being
  preserved between different calls to the getdbconnection() function.
- Fix memory leak when pcp library calls threw exceptions.
- Ensure description parameter in process() call always has correct indom
  information even if the indoms have changed during the archive.
- Various error handling improvements for cases where the indom information is
  missing from a PCP archive or disappears from the archive during a job.
- Improve robustness of Slurm cgroup extraction algorithm.


## [0.9.0] - 2016-01-07

Beta version of the SUPReMM package. This is the initial prototype software for
the summarization of SUPReMM data.
