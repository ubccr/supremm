# Changelog

## [Unreleased]

## [1.0.1] - 2016-08-16
=======
### Added

- Option to output summarize_jobs.py to file as it runs and valid json file when it finishes.

### Changed

- puffypcp reimplements much of the static functions from summarize.py which interfaced with
  the PCP libraries. Runs significantly faster.
- Removes pcpfast in favor of implementing direct access to pcp within puffypcp.

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
