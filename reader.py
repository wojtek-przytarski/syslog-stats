import time
import re
import logging
from multiprocessing import Queue, Pool


RFC3164_PATTERN = re.compile(r'<(\d{1,3})>(.{15})\s(\S*)\s(.*)')
RFC3164_MONTHS = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']


def read_file(filename, chunk_size=5000):
    pool = Pool()
    with open(filename) as file:
        count = 0
        chunk = []
        for line in file:
            chunk.append(line)
            count += 1
            if count % chunk_size == 0:
                pool.apply_async(handle_chunk, args=(chunk,))
                chunk = []
        pool.apply_async(handle_chunk, args=(chunk,))
        print(count)
    pool.close()
    pool.join()


def handle_chunk(chunk):
    severe_messages = 0
    for line in chunk:
        line_stats = handle_line(line)
        if line_stats.get('severity') <= 1:
            severe_messages += 1
    print(severe_messages)
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
    print('Starting...')
    read_file('syslog1GB')

    severity_messages = 0
    while not severity_queue.empty():
        severity_messages += severity_queue.get()

    finish = time.perf_counter()

    print(f'Done {severity_messages} in {finish - start}')
