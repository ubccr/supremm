""" X """
import cPickle as pickle
import os
import numpy
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

class PlotInterface(object):

    def plot_timeseries(self, times, values):
        pass

    def plot_sinwave(self, times, sinewave):
        pass

    def plot_area_ratio(self, on_period_area, off_period_area):
        pass

    def plot_periodogram(self, periods, powers, hints, power_threshold, time_threshold):
        pass

    def plot_acf(self, times, acf):
        pass

    def plot_acf_validation(self, times, acf, times1, m1, c1, err1, times2, m2, c2, err2, split_idx, peak_idx):
        pass

    def show(self):
        pass


class ImageOutput(PlotInterface):
    """ Output the timeseries data and the period estimate as plot in png format """
    def __init__(self, jobid, metricname):

        ijobid = int(jobid)
        top = (ijobid / 1000000)
        middle = (ijobid / 1000) % 1000

        try:
            os.makedirs("{}/{}".format(top, middle))
        except OSError:
            pass

        self.outfilename = "{}/{}/{}_{}.png".format(top, middle, jobid, metricname)
        self.fig = plt.figure()

        self.data_ax = plt.subplot(211, xlabel='Elapsed time (s)', ylabel='Data rate (MB/s)')
        self.sine_ax = None

        self.verbose = False

    def plot_timeseries(self, times, values):
        self.data_ax.plot(times, values / 1024.0 / 1024.0, label='Timeseries')

    def plot_sinwave(self, times, sinewave):
        self.sine_ax = plt.subplot(212, xlabel='Elapsed time (s)', ylabel='Data rate (MB/s)')
        self.sine_ax.plot(times, sinewave / 1024.0 / 1024.0, label='Estimate')

    def show(self):
        self.fig.tight_layout()
        self.fig.savefig(self.outfilename, format='png', transparent=True)

    
class Dumper(object):

    def __init__(self, filename='data.dat'):
        self.filename = filename
        self.data = {}
        self.verbose = True

    def plot_timeseries(self, times, values):
        self.data['timeseries'] = (times, values)

    def plot_sinwave(self, times, sinewave):
        self.data['sinewave'] = (times, sinewave)

    def plot_area_ratio(self, on_period_area, off_period_area):
        self.data['area_ratio'] = (on_period_area, off_period_area)

    def plot_periodogram(self, periods, powers, hints, power_threshold, time_threshold):
        self.data['periodogram'] = (periods, powers, hints, power_threshold, time_threshold)

    def plot_acf(self, times, acf):
        self.data['acf'] = (times, acf)

    def plot_acf_validation(self, times, acf, times1, m1, c1, err1, times2, m2, c2, err2, split_idx, peak_idx):
        self.data['acf_validation'] = (times, acf, times1, m1, c1, err1, times2, m2, c2, err2, split_idx, peak_idx)

    def show(self):
        with open(self.filename, 'wb') as fp:
            pickle.dump(self.data, fp)

    def load(self):
        with open(self.filename, 'rb') as fp:
            self.data = pickle.load(fp)

class Plotter(object):

    def __init__(self, title="Autoperiod", filename='output.pdf', figsize=(4, 3), verbose=False):
        self.title = title
        self.filename = filename
        self.fig = plt.figure()
        self.figsize = figsize
        self.verbose = verbose

        self.timeseries_ax = plt.subplot2grid((3, 10), (0, 0), colspan=9, xlabel='Times', ylabel='Values')

        self.area_ratio_ax = plt.subplot2grid((3, 10), (0, 9), colspan=1, xticks=(1, 2), xticklabels=("on", "off"))
        self.area_ratio_ax.get_yaxis().set_visible(False)

        self.periodogram_ax = plt.subplot2grid((3, 10), (1, 0), colspan=10, xlabel='Period', ylabel='Power')

        self.acf_ax = plt.subplot2grid((3, 10), (2, 0), colspan=10, xlabel='Lag', ylabel='Correlation')

        self.time_threshold = None

    def plot_timeseries(self, times, values):
        self.timeseries_ax.plot(times, values, label='Timeseries')
        self.timeseries_ax.legend()

    def plot_sinwave(self, times, sinwave):
        self.timeseries_ax.plot(times, sinwave, label='Estimated Period')
        self.timeseries_ax.legend()

    def plot_area_ratio(self, on_period_area, off_period_area):
        self.area_ratio_ax.bar(1, on_period_area)
        self.area_ratio_ax.bar(2, off_period_area)
        self.area_ratio_ax.legend()

    def plot_periodogram(self, periods, powers, hints, power_threshold, time_threshold):

        self.time_threshold = time_threshold

        self.periodogram_ax.plot(periods, powers, label='Periodogram')
        self.periodogram_ax.scatter([p for i, p in hints], [powers[i] for i, p in hints], c='red', marker='x', label='Period Hints')
        self.periodogram_ax.axhline(power_threshold, color='green', linewidth=1, linestyle='dashed', label='Min Power')
        #self.periodogram_ax.axvline(time_threshold, c='purple', linewidth=1, linestyle='dashed', label='Max Period')
        self.periodogram_ax.legend()
        self.periodogram_ax.set_xlim([0, self.time_threshold])

    def plot_acf(self, times, acf):

        self.acf_ax.plot(times, acf, '-o', lw=0.5, ms=2, label='Autocorrelation')
        if self.time_threshold is not None:
            self.acf_ax.set_xlim([0, self.time_threshold])
        self.acf_ax.legend()

    def plot_acf_validation(self, times, acf, times1, m1, c1, err1, times2, m2, c2, err2, split_idx, peak_idx):
        self.acf_ax.plot(times1, c1 + m1 * times1, c='r', label='Slope: {}, Error: {}'.format(m1, err1))
        self.acf_ax.plot(times2, c2 + m2 * times2, c='r', label='Slope: {}, Error: {}'.format(m2, err2))
        self.acf_ax.scatter(times[split_idx], acf[split_idx], c='y', label='Split point: {}'.format(times[split_idx]))
        self.acf_ax.scatter(times[peak_idx], acf[peak_idx], c='g', label='Peak point: {}'.format(times[peak_idx]))
        self.acf_ax.legend()

    def show(self):

        self.fig.tight_layout()

        if self.filename:
            self.fig.set_size_inches(*self.figsize)
            self.fig.savefig(self.filename, format='pdf', facecolor=self.fig.get_facecolor())

def main():
    """ X """
    d = Dumper('gpfs-fsios-write_bytes_data.dat')
    d.load()

    p = Plotter()
    p.plot_timeseries(*d.data['timeseries'])
    p.plot_sinwave(*d.data['sinewave'])
    p.plot_area_ratio(*d.data['area_ratio'])
    p.plot_periodogram(*d.data['periodogram'])
    p.plot_acf(*d.data['acf'])
    p.plot_acf_validation(*d.data['acf_validation'])

    p.show()
if __name__ == "__main__":
    main()
