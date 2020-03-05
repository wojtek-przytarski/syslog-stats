import datetime
import re
import logging
from statistics import StatisticsManager

logger = logging.getLogger('reader')


class ChunkHandler:
    RFC3164_PATTERN = re.compile(r'<(\d{1,3})>(.{3})\s\s?(\d{1,2})\s(\d{2}):(\d{2}):(\d{2})\s(\S*)\s(.*)')
    RFC3164_MONTHS = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']

    def __init__(self):
        logger.info(f'Created ChunkHandler: {self}')

    def get_chunk_stats(self, chunk):
        stats = StatisticsManager()
        for line in chunk:
            line_stats = self.handle_line(line)
            stats.add_data(
                *line_stats,
                update_total=True,
            )
        return stats.to_dict()

    def parse_line(self, line):
        match = self.RFC3164_PATTERN.match(line)
        if not match:
            logger.error(f'No match for syslog {line=}')
            return {}
        return {
            'pri': int(match.group(1)),
            'timestamp': datetime.datetime(
                2019,
                self.RFC3164_MONTHS.index(match.group(2)) + 1,
                int(match.group(3)),
                int(match.group(4)),
                int(match.group(5)),
                int(match.group(6)),
            ),
            'hostname': match.group(7),
            'msg': match.group(8),
        }

    @staticmethod
    def get_severity(pri):
        """
        From  https://www.ietf.org/rfc/rfc3164.txt

            Numerical         Severity
              Code
               0       Emergency: system is unusable
               1       Alert: action must be taken immediately
               2       Critical: critical conditions
               3       Error: error conditions
               4       Warning: warning conditions
               5       Notice: normal but significant condition
               6       Informational: informational messages
               7       Debug: debug-level messages
        :param pri:
        :return: severity_code
        """
        return pri % 8

    def handle_line(self, line):
        log_data = self.parse_line(line)
        stats = {
            'lines': 1,
            'messages_length': len(log_data.get('msg')),
            'severe_messages': 1 if self.get_severity(log_data.get('pri')) <= 1 else 0,
            'oldest_timestamp': log_data.get('timestamp'),
            'latest_timestamp': log_data.get('timestamp'),
        }
        return log_data.get('hostname'), stats
