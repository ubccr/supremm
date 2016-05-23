# Changelog

## [Unreleased]

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
