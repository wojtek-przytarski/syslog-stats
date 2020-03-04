import time
import re
import logging
from collections import defaultdict
from multiprocessing import Queue, Pool, cpu_count


RFC3164_PATTERN = re.compile(r'<(\d{1,3})>(.{15})\s(\S*)\s(.*)')
RFC3164_MONTHS = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']

CHUNKING_PROCESSES = cpu_count() - 2


def read_file(filename, chunk_size=1000000):
    pool = Pool()
    for i in range(CHUNKING_PROCESSES):
        pool.apply_async(handle_chunk)
    pool.apply_async(count_statistics)

    with open(filename) as file:
        while chunk := file.readlines(chunk_size):
            chunks_queue.put(chunk)
    print('Sending STOP messages')

    for i in range(CHUNKING_PROCESSES):
        chunks_queue.put('STOP_CHUNKS')
    pool.close()
    pool.join()


def handle_chunk():
    while (chunk := chunks_queue.get()) != 'STOP_CHUNKS':
        for line in chunk:
            line_stats = handle_line(line)
            severity_queue.put(line_stats)
    severity_queue.put('STOP_STATS')


def handle_line(line):
    log_data = parse_line(line)
    stats = {
        'length': len(log_data.get('msg')),
        'hostname': log_data.get('hostname'),
        'timestamp': len(log_data.get('timestamp')),
        'severe': get_severity(log_data.get('pri')) <= 1,
    }
    return stats


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


def count_statistics():
    total = {
        'severe': 0,
        'msg_length': 0,
        'oldest': 'Jan  1 00:00:00',
        'latest': 'Dec 31 23:59:59',
    }
    severe_by_host = defaultdict(int)
    msg_length_by_host = defaultdict(int)
    oldest_by_host = defaultdict(lambda: 'Jan  1 00:00:00')
    latest_by_host = defaultdict(lambda: 'Dec 31 23:59:59')
    chunks_finished = 0
    while line_stats := severity_queue.get(timeout=1000):
        if line_stats == 'STOP_STATS':
            chunks_finished += 1
            print(f'Got STOP_STATS signal, stopped chunking processes: {chunks_finished}')

        hostname = line_stats['hostname']
        msg_length = line_stats['length']

        total['msg_length'] += msg_length
        msg_length_by_host[hostname] += msg_length
        if line_stats['severe']:
            total['severe'] += 1
            severe_by_host[hostname] += 1
    with open('stats.txt', 'w') as f:
        f.writelines([
            f'Finished with stats {total}',
            f'Finished with stats by host {msg_length_by_host}',
            f'Finished with stats by host {severe_by_host}'
        ])


if __name__ == '__main__':
    start = time.perf_counter()
    severity_queue = Queue()
    chunks_queue = Queue()
    print('Starting...')
    read_file('syslogSmall', 2)

    finish = time.perf_counter()

    print(f'Done in {finish - start}')
