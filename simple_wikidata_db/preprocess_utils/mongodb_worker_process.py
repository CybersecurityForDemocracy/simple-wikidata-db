from collections import defaultdict
from multiprocessing import Queue

import orjson


def add_qid_as_int(input_data):
    qid = input_data["id"]
    input_data["qid"] = int(qid[1:])
    return input_data


def process_data(work_queue: Queue, out_queue: Queue):
    while True:
        json_str = work_queue.get()
        if json_str is None:
            break
        if len(json_str) == 0:
            continue
        #  out_queue.put(process_json(orjson.loads(json_str), language_id))
        json_obj = orjson.loads(json_str)
        out_queue.put(add_qid_as_int(json_obj))
    return
