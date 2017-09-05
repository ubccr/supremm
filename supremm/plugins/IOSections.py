#!/usr/bin/env python
from __future__ import print_function

import datetime
import numpy

from supremm.plugin import Plugin
from supremm.errors import ProcessingError
from supremm.statistics import calculate_stats


class IOSections(Plugin):
    """
    Plugin that computes and compares gpfs IO statistics for each
    section of a job.
    """

    SECTIONS = 4
    DISTANCE_THRESHOLD = 60
    MIN_NODES = 1

    name = property(lambda x: "iosections")
    mode = property(lambda x: "all")
    requiredMetrics = property(lambda x: ["gpfs.fsios.read_bytes", "gpfs.fsios.write_bytes"])

    optionalMetrics = property(lambda x: [])
    derivedMetrics = property(lambda x: [])

    def __init__(self, job):
        super(IOSections, self).__init__(job)
        self.starttime = (job.start_datetime - datetime.datetime(1970, 1, 1)).total_seconds()
        self.endtime = (job.end_datetime - datetime.datetime(1970, 1, 1)).total_seconds()
        self.section = (self.endtime - self.starttime) / self.SECTIONS

        self.nodes = {}
        self.section_start_timestamps = [[] for _ in xrange(self.SECTIONS)]

    def process(self, nodemeta, timestamp, data, description):
        n = nodemeta.nodename
        if n not in self.nodes:
            self.section_start_timestamps[0].append(self.starttime)
            self.nodes[n] = {
                "current_marker": self.starttime + self.section,
                "section_start_data": None,
                "section_start_timestamp": self.starttime,
                "section_avgs": [],
                "last_value": [],
                "section_counter": 0,
                "data_error": False
            }

        node_data = self.nodes[n]

        mountpoint_sums = [numpy.sum(x) for x in data]
        node_data["last_value"] = mountpoint_sums
        if node_data["section_start_data"] is None:
            node_data["section_start_data"] = mountpoint_sums

        if timestamp >= node_data["current_marker"] and timestamp < self.endtime:
            if timestamp - node_data["current_marker"] > self.DISTANCE_THRESHOLD:
                node_data["data_error"] = True

            avg_read = (mountpoint_sums[0] - node_data["section_start_data"][0]) / (timestamp - node_data["section_start_timestamp"])
            avg_write = (mountpoint_sums[1] - node_data["section_start_data"][1]) / (timestamp - node_data["section_start_timestamp"])

            node_data["section_avgs"].append((avg_read, avg_write))
            node_data["current_marker"] += self.section
            node_data["section_start_data"] = mountpoint_sums
            node_data["section_start_timestamp"] = timestamp
            node_data["section_counter"] += 1
            self.section_start_timestamps[node_data["section_counter"]].append(timestamp)

        return True

    def results(self):
        nodes_used = 0

        # holds the averages for each node, grouped by section
        section_data = [[] for _ in xrange(self.SECTIONS)]

        # Calculate results for final section
        for node, data in self.nodes.iteritems():
            avg_read = (data["last_value"][0] - data["section_start_data"][0]) / (self.endtime - data["section_start_timestamp"])
            avg_write = (data["last_value"][1] - data["section_start_data"][1]) / (self.endtime - data["section_start_timestamp"])

            data["section_avgs"].append((avg_read, avg_write))

            if len(data["section_avgs"]) == self.SECTIONS and not data["data_error"]:
                # Only includes the nodes which have enough data to be meaningful.
                for i in xrange(self.SECTIONS):
                    section_data[i].append(data["section_avgs"][i])

                nodes_used += 1

        if nodes_used < self.MIN_NODES:
            # If there are no nodes left after removing all that don't have enough data, then
            # there isn't enough data to report anything meaningful for the whole job
            return {"error": ProcessingError.INSUFFICIENT_DATA}

        section_stats_read = []
        section_stats_write = []
        for section in section_data:
            section_rw = zip(*section)
            section_stats_read.append(calculate_stats(section_rw[0]))
            section_stats_write.append(calculate_stats(section_rw[1]))

        # Compute the combined average for the whole "middle" section, which is
        # all sections except the first and last.
        middle_avg_read = sum(sect["avg"] for sect in section_stats_read[1:-1]) / (self.SECTIONS - 2)
        middle_avg_write = sum(sect["avg"] for sect in section_stats_write[1:-1]) / (self.SECTIONS - 2)

        results = {
            "nodes_used": nodes_used,
            "section_stats_read": section_stats_read,
            "section_stats_write": section_stats_write,
            "section_start_timestamps": [calculate_stats(sect) for sect in self.section_start_timestamps],
            "ratio_start_middle_read": ratio(section_stats_read[0]["avg"], middle_avg_read),
            "ratio_start_middle_write": ratio(section_stats_write[0]["avg"], middle_avg_write),
            "ratio_middle_end_read": ratio(middle_avg_read, section_stats_read[3]["avg"]),
            "ratio_middle_end_write": ratio(middle_avg_write, section_stats_write[3]["avg"]),
            "ratio_start_end_read": ratio(section_stats_read[0]["avg"], section_stats_read[3]["avg"]),
            "ratio_start_end_write": ratio(section_stats_write[0]["avg"], section_stats_write[3]["avg"]),
        }

        return results


def ratio(f1, f2):
    """
    Computes a ratio between two numbers, handling special cases appropriately for
    this results format.
    :param f1: the numerator
    :param f2: the denominator
    :return: the ratio between the numbers, an 'inf' float value if the denominator is 0, or 1.0 if both
    numbers are 0.
    """
    if f1 == f2 == 0:
        return 1.0
    elif f2 == 0:
        return float('inf')
    else:
        return f1 / f2
