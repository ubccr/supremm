#!/usr/bin/env python
from __future__ import print_function, division

from datetime import datetime

import numpy as np
from autoperiod import Autoperiod
from autoperiod.helpers import convert_to_rates
from six import iteritems
from six.moves import range

from supremm.errors import ProcessingError
from supremm.plugin import Plugin
from supremm.statistics import calculate_stats


class TimeseriesPatterns(Plugin):
    SECTIONS = 4
    DISTANCE_THRESHOLD = 60
    MIN_NODES = 1

    @property
    def mode(self):
        return "all"

    @property
    def optionalMetrics(self):
        return []

    @property
    def derivedMetrics(self):
        return []

    def __init__(self, job):
        super(TimeseriesPatterns, self).__init__(job)

        EPOCH = datetime(1970, 1, 1)
        self.start_time = (job.start_datetime - EPOCH).total_seconds()
        self.end_time = (job.end_datetime - EPOCH).total_seconds()
        self.section_len = (self.end_time - self.start_time) / self.SECTIONS

        self.nodes = {}
        self.section_start_timestamps = [[] for _ in xrange(self.SECTIONS)]
        self.metricNames = [str.replace(metric, '.', '-') for metric in self.requiredMetrics]

    def process(self, nodemeta, timestamp, data, description):

        # associate each metric with its data point, as tuples of (metric, data)
        # sum across the mountpoints to get one total data point
        metrics = zip(self.metricNames, (np.sum(x) for x in data))

        nodename = nodemeta.nodename
        if nodename not in self.nodes:
            self.section_start_timestamps[0].append(self.start_time)
            self.nodes[nodename] = {
                "current_marker": self.start_time + self.section_len,
                "section_start_data": dict(metrics),
                "section_start_timestamp": self.start_time,
                "section_avgs": {metric: [] for metric in self.metricNames},
                "last_value": None,
                "section_counter": 0,
                "data_error": False,

                "all_times": [],
                "all_data": {metric: [] for metric in self.metricNames}
            }

        node = self.nodes[nodename]

        # we need to store every data point for now to do period analysis
        # hopefully this won't be needed in the future
        node['all_times'].append(timestamp)
        for metric, data in metrics:
            node['all_data'][metric].append(data)

        # always store latest value since it is needed in the results stage
        # and we don't know when the processing will end
        node['last_value'] = metrics

        if timestamp >= node['current_marker'] and timestamp < self.end_time:

            # if the data point is too far from the expected section cutoff
            # (i.e. there is a large gap in the data), mark the node as errored
            node['data_error'] = timestamp - node['current_marker'] > self.DISTANCE_THRESHOLD

            # Store the section average rate and reload for the next section
            for metric, data in metrics:
                avg = (data - node['section_start_data'][metric]) / (timestamp - node['section_start_timestamp'])
                node['section_avgs'][metric].append(avg)
                node['section_start_data'][metric] = data

            node['section_start_timestamp'] = timestamp
            node['section_counter'] += 1
            self.section_start_timestamps[node['section_counter']].append(timestamp)
            node['current_marker'] += self.section_len

        return True

    def results(self):

        metric_data = {
            metric: {
                # Store data for each node (inner array), for each section (outer array)
                'sections': [[] for _ in range(self.SECTIONS)],
                'nodes_used': 0
            }
            for metric in self.metricNames
        }

        for nodename, node in iteritems(self.nodes):
            for metric, data in node['last_value']:

                # calculate last section
                avg = (data - node['section_start_data'][metric]) / (self.end_time - node['section_start_timestamp'])
                node['section_avgs'][metric].append(avg)

                # only use nodes which have enough data
                if len(node['section_avgs'][metric]) == self.SECTIONS and not node['data_error']:
                    metric_data[metric]['nodes_used'] += 1

                    # add node to the section aggregates
                    for i in range(self.SECTIONS):
                        metric_data[metric]['sections'][i].append(node['section_avgs'][metric][i])

        for metric_name, metric in iteritems(metric_data):

            # If a metric didn't have enough viable nodes, report error due to insufficient data
            if metric['nodes_used'] < self.MIN_NODES:
                metric_data[metric_name] = {'error': ProcessingError.INSUFFICIENT_DATA}
                break

            # Use stats across the nodes instead of reporting all node data individually
            metric['sections'] = [calculate_stats(nodes) for nodes in metric['sections']]
            metric['section_start_timestamps'] = [calculate_stats(sect) for sect in self.section_start_timestamps]

        autoperiods = _calculate_autoperiod(self.nodes, self.metricNames)

        for metric in self.metricNames:
            metric_data[metric].update({"autoperiod": autoperiods[metric]})

        return metric_data


def _calculate_autoperiod(nodes, metrics):
    times_interp = None
    summed_values = {metric: None for metric in metrics}

    # Interpolate times and values so sampling interval is constant, and sum nodes
    for nodename, node in iteritems(nodes):
        for metric in metrics:
            if times_interp is None:
                times_interp = np.linspace(min(node['all_times']), max(node['all_times']),
                                           len(node['all_times']))
                summed_values[metric] = np.interp(times_interp, node['all_times'], node['all_data'][metric])
            else:
                if summed_values[metric] is None:
                    summed_values[metric] = np.interp(times_interp, node['all_times'], node['all_data'][metric])
                else:
                    summed_values[metric] += np.interp(times_interp, node['all_times'],
                                                       node['all_data'][metric])

    autoperiods = {}
    for metric in metrics:
        values = summed_values[metric]

        autoperiod = Autoperiod(
            *convert_to_rates(times_interp, values),
            threshold_method='stat'
        ) if not np.allclose(values, 0) else None

        if autoperiod is None or autoperiod.period is None:
            autoperiods[metric] = None
        else:
            ap_data = {
                "period": autoperiod.period,
                "phase_shift_guess": autoperiod.phase_shift_guess
            }

            on_period_block_areas, off_period_block_areas = autoperiod.period_block_areas()
            on_period = calculate_stats(on_period_block_areas)
            off_period = calculate_stats(off_period_block_areas)

            on_period['sum'] = np.sum(on_period_block_areas)
            off_period['sum'] = np.sum(off_period_block_areas)

            ap_data['on_period'] = on_period
            ap_data['off_period'] = off_period

            normalized_score = (on_period['sum'] - off_period['sum']) / (on_period['sum'] + off_period['sum'])
            ap_data['normalized_score'] = normalized_score
            autoperiods[metric] = ap_data

    return autoperiods
