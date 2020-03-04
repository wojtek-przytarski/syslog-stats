import os
import time
import re
import logging
from collections import defaultdict
from multiprocessing import Queue, Pool, cpu_count


RFC3164_PATTERN = re.compile(r'<(\d{1,3})>(.{15})\s(\S*)\s(.*)')
RFC3164_MONTHS = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']

CHUNKING_PROCESSES = cpu_count() - 1


def read_file(filename, chunk_size=1000000):
    pool = Pool()
    for i in range(CHUNKING_PROCESSES):
        pool.apply_async(handle_chunk)
    pool.apply_async(prepare_statistics)

    with open(filename) as file:
        while chunk := file.readlines(chunk_size):
            print('.', end='')
            chunks_queue.put(chunk)
    print('Sending STOP messages')
    for i in range(CHUNKING_PROCESSES):
        print(f'STOP #{i}')
        chunks_queue.put('STOP_CHUNKS')
    pool.close()
    pool.join()


def handle_chunk():
    stats_queue.put('START_CHUNK_PROC')
    while (chunk := chunks_queue.get()) != 'STOP_CHUNKS':
        stats = {
            'severe': 0,
            'msg_length': 0,
            'lines': 0,
            'oldest': 'Jan  1 00:00:00',
            'latest': 'Dec 31 23:59:59',
            'by_host': {}
        }
        for line in chunk:
            stats['lines'] += 1

            line_stats = handle_line(line)
            msg_length = line_stats['length']
            severe = line_stats['severe']
            hostname = line_stats['hostname']
            timestamp = line_stats['timestamp']

            stats['msg_length'] += msg_length
            stats['severe'] += severe
            if stats['by_host'].get(hostname):
                stats['by_host'][hostname]['msg_length'] += msg_length
                stats['by_host'][hostname]['severe'] += severe
            else:
                stats['by_host'][hostname] = {
                    'msg_length': msg_length,
                    'severe': severe,
                }
        stats_queue.put(stats)
    stats_queue.put('END_CHUNK_PROC')


def prepare_statistics():
    stats = {
        'lines': 0,
        'severe': 0,
        'msg_length': 0,
        # 'oldest': 'Jan  1 00:00:00',
        # 'latest': 'Dec 31 23:59:59',
        'by_host': defaultdict(lambda: {
            'severe': 0,
            'msg_length': 0,
            'oldest': 'Jan  1 00:00:00',
            'latest': 'Dec 31 23:59:59',
        })
    }
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
            stats['lines'] += partial_stats['lines']
            stats['msg_length'] += partial_stats['msg_length']
            stats['severe'] += partial_stats['severe']
            for host, host_dict in partial_stats['by_host'].items():
                stats['by_host'][host]['severe'] += host_dict['severe']
                stats['by_host'][host]['msg_length'] += host_dict['msg_length']
    with open('stats.txt', 'w') as file:
        file.writelines([
            f'Total ALERT and EMERGENCY lines: {stats["severe"]}\n',
            f'Average msg length: {stats["msg_length"]/stats["lines"]}\n',
        ])


def handle_line(line):
    log_data = parse_line(line)
    stats = {
        'length': len(log_data.get('msg')),
        'hostname': log_data.get('hostname'),
        'timestamp': len(log_data.get('timestamp')),
        'severe': 1 if get_severity(log_data.get('pri')) <= 1 else 0,
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


if __name__ == '__main__':
    start = time.perf_counter()

    file_name = 'syslog1GB'
    file_size = os.path.getsize(file_name)
    chunks_number = 100

    stats_queue = Queue(chunks_number + 20)
    chunks_queue = Queue(chunks_number + 20)

    print('Starting... ')
    read_file(file_name, round(file_size / chunks_number))

    finish = time.perf_counter()

    print(f'Done in {finish - start}')
