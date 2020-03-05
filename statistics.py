import csv
import datetime
from collections import defaultdict


class Statistics:
    BSD_TIMESTAMP_FORMAT = '%b %d %H:%M:%S'

    def __init__(self):
        self.lines = 0
        self.messages_length = 0
        self.severe_messages = 0
        self.oldest_timestamp = datetime.datetime(2019, 1, 1)
        self.latest_timestamp = datetime.datetime(2019, 12, 31, 23, 59, 59)

    def add_data(self, stats):
        self.lines += stats['lines']
        self.messages_length += stats['messages_length']
        self.severe_messages += stats['severe_messages']

        oldest = stats['oldest_timestamp']
        latest = stats['latest_timestamp']
        if oldest < self.oldest_timestamp:
            self.oldest_timestamp = oldest
        if latest > self.latest_timestamp:
            self.latest_timestamp = latest

    @property
    def average_message_length(self):
        return self.messages_length / self.lines

    def to_csv_line(self, title):
        return [
            title,
            self.severe_messages,
            self.average_message_length,
            self.oldest_timestamp,
            self.latest_timestamp,
        ]

    def to_dict(self):
        return {
            'lines': self.lines,
            'messages_length': self.messages_length,
            'severe_messages': self.severe_messages,
            'oldest_timestamp': self.oldest_timestamp,
            'latest_timestamp': self.latest_timestamp,
        }

    def _bsd_timestamp_to_datetime(self, timestamp):
        return datetime.datetime.strptime(timestamp, self.BSD_TIMESTAMP_FORMAT)

    def _datetime_to_bsd_timestamp(self, timestamp):
        return datetime.datetime.strftime(timestamp, self.BSD_TIMESTAMP_FORMAT)


class StatisticsManager:
    def __init__(self):
        self.total = Statistics()
        self.by_host = defaultdict(lambda: Statistics())

    def add_data(self, hostname, stats, update_total=False):
        self.by_host[hostname].add_data(stats)
        if update_total:
            self.add_total_data(stats)

    def add_total_data(self, stats):
        self.total.add_data(stats)

    def to_dict(self):
        return {
            'total': self.total.to_dict(),
            'by_host': {
                hostname: st.to_dict()
                for hostname, st in self.by_host.items()
            }
        }

    def write_to_file(self, filename):
        with open(filename, 'w') as file:
            w = csv.writer(file)
            w.writerow(['Host', 'ALERT/EMERGENCY messages', 'AVG message length', 'Oldest', 'Newest'])
            w.writerow(self.total.to_csv_line('Total'))
            w.writerow([])
            for host, stats in self.by_host.items():
                w.writerow(stats.to_csv_line(host))
