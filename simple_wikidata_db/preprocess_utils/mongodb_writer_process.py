import time
import logging
from dataclasses import dataclass, field
from multiprocessing import Queue

from pymongo import MongoClient
from pymongo.collection import Collection
import orjson as json

from typing import Any

TIME_ESTIMATE_REPORT_NUM_LINES = 200_000

@dataclass
class MongoDbWriter:
    uri: str
    database_name: str
    collection_name: str
    expected_total_num_lines: int
    last_time_estimate_report_time: float = field(init=False)
    client: MongoClient = field(init=False)
    collection_client: Collection = field(init=False)
    num_lines_written: int = field(default=0)

    def __post_init__(self):
        self.client = MongoClient(self.uri)
        database = self.client.get_database(self.database_name)
        self.collection_client = database.get_collection(self.collection_name)
        self.last_time_estimate_report_time = time.time()

    def _report_time_elaspsed_and_restart_timer(self):
        time_elapsed = time.time() - self.last_time_estimate_report_time
        estimated_time = time_elapsed * (self.total_num_lines - self.num_lines_written) / (TIME_ESTIMATE_REPORT_NUM_LINES*3600)
        logging.info("%d/%d lines written in %.2f s. Estimated time to completion is %.2f hours.", self.num_lines_written, self.total_num_lines, time_elapsed, estimated_time)
        self.last_time_estimate_report_time = time.time()

    def write(self, json_obj_list: list[dict[str, Any]]):
        logging.debug("insert_many: %r")
        self.collection_client.insert_one(json_obj_list)
        self.num_lines_written += 1
        if self.num_lines_written % TIME_ESTIMATE_REPORT_NUM_LINES == 0:
            self._report_time_elaspsed_and_restart_timer()

    def close(self):
        self.client.close()

def write_data(uri: str, database_name: str, collection_name: str, expected_total_num_lines: int, output_queue: Queue):
    """
    Reads the json objects from output queue and writes them to mongo db URI database_name collection_name
    :param uri: mongo db connection uri to write to
    :param database_name: mongo db database name to write to
    :param collection_name: mongo db collection name to write to
    :param expected_total_num_lines: number of expected lines input. used for estimated time to
    completion.
    :param work_queue: Queue to push the data to.
    """
    writer = MongoDbWriter(uri=uri, database_name=database_name, collection_name=collection_name,
                           expected_total_num_lines=expected_total_num_lines)
    while True:
        json_object = output_queue.get()
        logging.debug("write_data received: %r", json_object)
        if json_object is None:
            logging.info("writer process received None from queue. quiting")
            break
        writer.write(json_object)
    writer.close()

