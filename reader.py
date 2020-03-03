import time
import re
import logging
from multiprocessing import Queue, Pool


RFC3164_PATTERN = re.compile(r'<(\d{1,3})>(.{15})\s(\S*)\s(.*)')
RFC3164_MONTHS = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']


def read_file(filename):
    pool = Pool()
    with open(filename) as file:
        count = 0
        for line in file:
            pool.apply_async(handle_line, args=(line,))
            count += 1
        print(count)
    pool.close()
    pool.join()


def handle_line(line):
    log_data = parse_line(line)
    stats = {
        'length': len(log_data.get('msg')),
        'timestamp': len(log_data.get('timestamp')),
    }
    if get_severity(log_data.get('pri')) <= 1:
        severity_queue.put(1)


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
    read_file('syslogSmall')

    severity_messages = 0
    while not severity_queue.empty():
        severity_queue.get()
        severity_messages += 1

    finish = time.perf_counter()

    print(f'Done {severity_messages} in {finish - start}')
