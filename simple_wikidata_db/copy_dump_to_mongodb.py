"""Wikidata Dump Processor

This script preprocesses the raw Wikidata dump (in gz or bz2 compressed JSON format) and copies
those to mongodb

python3 -m simple_wikidata_db.copy_dump_to_mongodb
/lfs/raiders8/0/lorr1/wikidata/raw_data/latest-all.json.gz --uri 'mongodb://127.0.0.1:999/'
--database 'wikidata' --collection 'entities' --index 'id'
"""

import logging
import multiprocessing
import time
from multiprocessing import Process, Queue
from pathlib import Path
from typing import Annotated

import orjson
import typer
from pymongo import MongoClient
from tqdm import tqdm

from simple_wikidata_db.preprocess_utils.mongodb_writer_process import write_data
from simple_wikidata_db.preprocess_utils.reader_process import count_lines, read_data

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
    unique_index_field_list: Annotated[
        list[str] | None,
        typer.Option("--unique-index", help="create single item, unique, index for this field"),
    ] = None,
    text_index_field_list: Annotated[
        list[str] | None,
        typer.Option("--text-index", help="create single item text index for this field"),
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

    maxsize = 100

    # Queue for reader output -> writer input
    output_queue = Queue(maxsize=maxsize)

    num_lines_read = multiprocessing.Value("i", 0)
    read_process = Process(
        target=read_data, args=(input_file, num_lines_read, max_lines_to_read, output_queue)
    )

    read_process.start()

    write_process = Process(
        target=write_data,
        args=(uri, database, collection, unique_index_field_list, text_index_field_list, total_num_lines, output_queue),
    )
    write_process.start()

    read_process.join()
    logging.info("Done! Read %s lines", num_lines_read.value)

    output_queue.put(None)
    write_process.join()

    logging.info("Finished processing %s in %s s", num_lines_read.value, time.time() - start)


@APP.command(
    help=(
        "Add sqid data to existing wikidata entities in mongodb. adds new top level key |sqid| "
        "mapping to sqid data. expects JSON from https://sqid.toolforge.org/#/"
    )
)
def sqid(
    input_file: Annotated[
        Path,
        typer.Argument(
            exists=True,
            file_okay=True,
            dir_okay=False,
            readable=True,
            resolve_path=True,
            help="sqid hierarchy JSON file",
        ),
    ],
    uri: Annotated[str, typer.Option(help="uri for mongodb")],
    database: Annotated[str, typer.Option(help="database for mongodb")],
    collection: Annotated[str, typer.Option(help="collection for mongodb")],
    debug: Annotated[bool, typer.Option(help="enable debug logging")] = False,
):
    logging.basicConfig(
        format=DEBUG_LOG_FORMAT if debug else DEFAULT_LOG_FORMAT,
        datefmt="%Y-%m-%d %H:%M:%S",
        level=logging.DEBUG if debug else logging.INFO,
    )

    logging.info(
        "Will add sqid data from %s to URI: %s database: %s collection: %s",
        input_file,
        uri,
        database,
        collection,
    )

    client = MongoClient(uri)
    database = client.get_database(database)
    collection_client = database.get_collection(collection)

    modified_count = 0

    sqid_hiearchy = orjson.loads(input_file.read_bytes())
    logging.info("Got %d entires from %s", len(sqid_hiearchy), input_file)
    for qid, value in tqdm(sqid_hiearchy.items()):
        update_result = collection_client.update_one(
            {"id": f"Q{qid}"}, {"$set": {"sqid": value}}, upsert=False
        )
        if update_result:
            logging.debug("qid %s update result: %s", qid)
            modified_count += update_result.modified_count
        else:
            logging.warning("qid %s update result false ", qid)

    logging.info("Of %d entries modified %d", len(sqid_hiearchy), modified_count)


if __name__ == "__main__":
    APP()
