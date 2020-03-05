import os
import time
import re
import logging
from collections import defaultdict
from multiprocessing import Queue, Pool, cpu_count

from statistics import StatisticsManager

RFC3164_PATTERN = re.compile(r'<(\d{1,3})>(.{15})\s(\S*)\s(.*)')
RFC3164_MONTHS = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']

CHUNKING_PROCESSES = max(1, cpu_count() - 1)


def read_file(filename, chunk_size=1000000):
    pool = Pool()
    for i in range(CHUNKING_PROCESSES):
        pool.apply_async(handle_chunk)
    pool.apply_async(prepare_statistics)

    with open(filename) as file:
        while chunk := file.readlines(chunk_size):
            chunks_queue.put(chunk)
    print('Sending STOP messages')
    for i in range(CHUNKING_PROCESSES):
        chunks_queue.put('STOP_CHUNKS')
    pool.close()
    pool.join()


def handle_chunk():
    stats_queue.put('START_CHUNK_PROC')
    while (chunk := chunks_queue.get()) != 'STOP_CHUNKS':
        stats = StatisticsManager()
        for line in chunk:
            line_stats = handle_line(line)
            stats.add_data(
                *line_stats
            )
        stats_queue.put(stats.to_dict())
        # print('stats in queue')
    stats_queue.put('END_CHUNK_PROC')


def prepare_statistics():
    stats = StatisticsManager()
    print('Starting stats')
    x = 1
    stats_queue.get()
    while x:
        partial_stats = stats_queue.get()
        if partial_stats == 'START_CHUNK_PROC':
            x += 1
        elif partial_stats == 'END_CHUNK_PROC':
            x -= 1
        else:
            stats.add_total_data(partial_stats['total'])
            for host, host_dict in partial_stats['by_host'].items():
                stats.add_data(host, host_dict)
    stats.write_to_file('stats.csv')
    print('Stats saved')


def handle_line(line):
    log_data = parse_line(line)
    stats = {
        'lines': 1,
        'messages_length': len(log_data.get('msg')),
        'severe_messages': 1 if get_severity(log_data.get('pri')) <= 1 else 0,
        'oldest_timestamp': log_data.get('timestamp'),
        'latest_timestamp': log_data.get('timestamp'),
    }
    return log_data.get('hostname'), stats


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


def parse_line(line):
    match = RFC3164_PATTERN.match(line)
    if not match:
        logging.error(f'No match for syslog line: {line}')
        return {}
    return {
        'pri': int(match.group(1)),
        'timestamp': match.group(2),
        'hostname': match.group(3),
        'msg': match.group(4),
    }


if __name__ == '__main__':
    start = time.perf_counter()

    file_name = 'tmp'
    file_size = os.path.getsize(file_name)
    cs = 1048000
    chunks_number = round(file_size / cs)

    stats_queue = Queue(chunks_number + 20)
    chunks_queue = Queue(chunks_number + 20)

    print('Starting... ')
    read_file(file_name, cs)

    finish = time.perf_counter()

    print(f'Done in {finish - start}')
