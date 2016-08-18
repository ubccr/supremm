class Profile(object):
    """
    Profile class keeps running total of how much time each analytic
    uses overall and tabulates results
    """

    def __init__(self):
        self.times = dict()

    def merge(self, record):
        """Adds data from another instance of Profile to this one"""
        for k in record:
            if k not in self.times:
                self.times[k] = record[k]
            for catigory in record[k]:
                if catigory in self.times[k]:
                    self.times[k][catigory] = self.times[k][catigory] + record[k][catigory]
                else:
                    self.times[k][catigory] = record[k][catigory]

    def add(self, analytic, catigory, val):
        if analytic in self.times:
            if catigory in self.times[analytic]:
                self.times[analytic][catigory] = self.times[analytic][catigory] + val
            else:
                self.times[analytic][catigory] = val
        else:
            self.times[analytic] = dict()
            self.times[analytic][catigory] = val

    def calc_derived(self):
        for k in self.times:
            if 'process' in self.times[k]:
                self.times[k]['extract'] = self.times[k]['process+extract'] - self.times[k]['process']
                self.times[k]['total'] = self.times[k]['process'] + self.times[k]['extract'] + self.times[k]['results']
            else:
                self.times[k]['total'] = self.times[k]['process+extract']

    def ranked(self):
        self.calc_derived()
        total_times = dict()
        for k in self.times:
            total_times[k] = self.times[k]['total']

        for k in sorted(total_times, key=total_times.get, reverse=True):
            yield (k, self.times[k])
