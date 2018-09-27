#!/usr/bin/env mysql

use modw_supremm;

CREATE TABLE `archive_paths` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `filename` varchar(255) COLLATE utf8_unicode_ci NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY (`filename`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;

CREATE TABLE `archives_nodelevel` (
  `archive_id` int(11) NOT NULL,
  `host_id` int(11) NOT NULL,
  `start_time_ts` int(11) NOT NULL,
  `end_time_ts` int(11) NOT NULL,
  PRIMARY KEY (`archive_id`),
  KEY `hosttimes` (`host_id` ASC, `start_time_ts` ASC, `end_time_ts` ASC)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;

CREATE TABLE `archives_joblevel` (
    `archive_id` int(11) NOT NULL,
    `host_id` int(11) NOT NULL,
    `local_job_id_raw` int(11) NOT NULL,
    `start_time_ts` int(11) NOT NULL,
    `end_time_ts` int(11) NOT NULL,
    PRIMARY KEY (`archive_id`),
    KEY `hostjobs` (`host_id` ASC, `local_job_id_raw` ASC)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;


INSERT INTO `archive_paths` SELECT id, filename FROM archive;

INSERT INTO `archives_nodelevel` SELECT id, hostid, FLOOR(start_time_ts), CEILING(end_time_ts) FROM `archive` WHERE jobid IS NULL;

INSERT INTO `archives_joblevel` SELECT id, hostid, CAST(`jobid` AS SIGNED), FLOOR(start_time_ts), CEILING(end_time_ts) FROM `archive` WHERE jobid IS NOT NULL;


