import time
import re
import logging
from multiprocessing import Queue, Pool, cpu_count


RFC3164_PATTERN = re.compile(r'<(\d{1,3})>(.{15})\s(\S*)\s(.*)')
RFC3164_MONTHS = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']


def read_file(filename, chunk_size=1000000):
    pool = Pool()
    cores = cpu_count()
    for i in range(cores - 1):
        pool.apply_async(handle_chunk)

    with open(filename) as file:
        while chunk := file.readlines(chunk_size):
            chunks_queue.put(chunk)
    print('Sending STOP messages')

    for i in range(cores - 1):
        chunks_queue.put('STOP')
    pool.close()
    pool.join()


def handle_chunk():
    severe_messages = 0
    while (chunk := chunks_queue.get()) != 'STOP':
        for line in chunk:
            line_stats = handle_line(line)
            if line_stats.get('severity') <= 1:
                severe_messages += 1
    severity_queue.put(severe_messages)


def handle_line(line):
    log_data = parse_line(line)
    stats = {
        'length': len(log_data.get('msg')),
        'timestamp': len(log_data.get('timestamp')),
        'severity': get_severity(log_data.get('pri')),
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
    severity_queue = Queue()
    chunks_queue = Queue()
    print('Starting...')
    read_file('syslogLarge')
    print('Counting severe messages...')
    severity_messages = 0
    while not severity_queue.empty():
        severity_messages += severity_queue.get()

    finish = time.perf_counter()

    print(f'Done {severity_messages} in {finish - start}')
