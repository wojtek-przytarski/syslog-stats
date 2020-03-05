import os
import sys
import time
import logging
from multiprocessing import Queue, Pool, cpu_count

from chunk_handler import ChunkHandler
from statistics import StatisticsManager


CHUNKING_PROCESSES = max(1, cpu_count() - 1)
logger = logging.getLogger('reader')
fh = logging.FileHandler('reader.log')
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
fh.setFormatter(formatter)
logger.addHandler(fh)
logger.setLevel(logging.DEBUG)

stats_queue = Queue()
chunks_queue = Queue()


def read_file(filename, chunk_size=1000000):
    pool = Pool()
    for i in range(CHUNKING_PROCESSES):
        pool.apply_async(handle_chunk)
    pool.apply_async(prepare_statistics)

    with open(filename) as file:
        while chunk := file.readlines(chunk_size):
            chunks_queue.put(chunk)
    logger.info('Sending STOP messages')
    for i in range(CHUNKING_PROCESSES):
        chunks_queue.put('STOP_CHUNKS')
    pool.close()
    pool.join()


def handle_chunk():
    handler = ChunkHandler()
    logger.info(f'Started chunk process {handler=}')
    stats_queue.put('START_CHUNK_PROC')
    while (chunk := chunks_queue.get()) != 'STOP_CHUNKS':
        stats = handler.get_chunk_stats(chunk)
        stats_queue.put(stats)
    stats_queue.put('END_CHUNK_PROC')
    logger.info('Stopped chunk process')


def prepare_statistics():
    stats = StatisticsManager()
    logger.info('Started statistics process')
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
    logger.info(f'Statistics saved: {stats.to_dict()}')
    print(f'Statistics saved to file stats.csv')


def run(filepath):
    file_size = os.path.getsize(filepath)
    cs = 1048576
    logger.info(f'Starting process {filepath=}, {file_size=}, {cs=}')

    start = time.perf_counter()
    read_file(filepath, cs)
    finish = time.perf_counter()
    logger.info(f'Done in {finish - start} sec')
    print(f'Done in {finish - start}')


if __name__ == '__main__':
    file_name = sys.argv[1]
    run(file_name)
