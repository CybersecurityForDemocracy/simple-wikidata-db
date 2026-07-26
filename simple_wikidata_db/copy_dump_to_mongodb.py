""" Wikidata Dump Processor

This script preprocesses the raw Wikidata dump (in JSON format) and sorts triples into 8 "tables": labels, descriptions, aliases, entity_rels, external_ids, entity_values, qualifiers, and wikipedia_links. See the README for more information on each table.

Example command:

python3 preprocess_dump.py \
    --input_file /lfs/raiders8/0/lorr1/wikidata/raw_data/latest-all.json.gz \
    --out_dir data/processed

"""

import logging
import argparse
import multiprocessing
from multiprocessing import Queue, Process
from pathlib import Path
import time
from typing import Annotated
import enum

import typer

from simple_wikidata_db.preprocess_utils.reader_process import count_lines, read_data
from simple_wikidata_db.preprocess_utils.mongodb_worker_process import process_data
from simple_wikidata_db.preprocess_utils.mongodb_writer_process import write_data

APP = typer.Typer()

# time.strftime (which logging uses to format asctime) does not have a directive for microseconds,
# so we use this date format and %(asctime)s,%(msecs)d to get the microseconds in the record
DEFAULT_LOG_FORMAT = (
    "%(asctime)s,%(msecs)d %(name)s %(filename)s:%(lineno)s %(levelname)s %(message)s"
)
# This format is similar to above with addition of function name
DEBUG_LOG_FORMAT = (
    "%(asctime)s,%(msecs)d %(name)s %(filename)s:%(lineno)s->%(funcName)s() %(levelname)s "
    "%(message)s"
)


@APP.command()
def main(
    input_file: Annotated[
        Path,
        typer.Argument(
            exists=True,
            file_okay=True,
            dir_okay=False,
            readable=True,
            resolve_path=True,
            help="gzip or bz2 wikidata entities json file.",
        ),
    ],
    uri: Annotated[str, typer.Option(help="uri for mongodb")],
    database: Annotated[str, typer.Option(help="database for mongodb")],
    collection: Annotated[str, typer.Option(help="collection for mongodb")],
    processes: Annotated[
        int, typer.Option(help="number of concurrent processes to spin off. ")
    ] = 0,
    num_lines_read: Annotated[
        int,
        typer.Option(help="Terminate after num_lines_read lines are read.  Useful for debugging."),
    ] = -1,
    num_lines_in_dump: Annotated[
        int, typer.Option(help="Number of lines in dump. If -1, we will count the number of lines.")
    ] = -1,
    debug: Annotated[bool, typer.Option(help="enable debug logging")] = False,
    index_field_list: Annotated[
        list[str] | None,
        typer.Option("--index", help="create single item, unique, index for this field"),
    ] = None,
):
    start = time.time()

    logging.basicConfig(
        format=DEBUG_LOG_FORMAT if debug else DEFAULT_LOG_FORMAT,
        datefmt="%Y-%m-%d %H:%M:%S",
        level=logging.DEBUG if debug else logging.INFO,
    )

    logging.info(
        "Will write entities from %s to URI: %s database: %s collection: %s",
        input_file,
        uri,
        database,
        collection,
    )

    max_lines_to_read = num_lines_read
    if num_lines_in_dump <= 0:
        logging.info("Counting lines")
        total_num_lines = count_lines(input_file, max_lines_to_read)
    else:
        total_num_lines = num_lines_in_dump

    if processes == 0:
        processes = multiprocessing.cpu_count()
    logging.info("Starting %d processes", processes)
    maxsize = 10 * processes

    # Queues for inputs/outputs
    output_queue = Queue(maxsize=maxsize)
    work_queue = Queue(maxsize=maxsize)

    # Processes for reading/processing/writing
    num_lines_read = multiprocessing.Value("i", 0)
    read_process = Process(
        target=read_data, args=(input_file, num_lines_read, max_lines_to_read, work_queue)
    )

    read_process.start()

    write_process = Process(
        target=write_data,
        args=(uri, database, collection, index_field_list, total_num_lines, output_queue),
    )
    write_process.start()

    work_processes = []
    for _ in range(max(1, processes - 2)):
        work_process = Process(target=process_data, args=(work_queue, output_queue))
        work_process.daemon = True
        work_process.start()
        work_processes.append(work_process)

    read_process.join()
    logging.info("Done! Read %s lines", num_lines_read.value)
    # Cause all worker process to quit
    for work_process in work_processes:
        work_queue.put(None)
    # Now join the work processes
    for work_process in work_processes:
        work_process.join()
    output_queue.put(None)
    write_process.join()

    logging.info("Finished processing %s in %s s", num_lines_read.value, time.time() - start)


if __name__ == "__main__":
    APP()
