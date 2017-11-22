# Changelog

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
