"""Assortment of useful utility functions 
"""

import os
import orjson as json
from pathlib import Path


def jsonl_generator(fname):
    """ Returns generator for jsonl file """
    for line in open(fname, 'r'):
        line = line.strip()
        if len(line) < 3:
            d = {}
        elif line[:-1] == ',':
            d = json.loads(line[:-1])
        else:
            d = json.loads(line)
        yield d

def get_batch_files(fdir):
    """ Returns paths to files in fdir """
    filenames = [p.resolve() for p in Path(fdir).iterdir()]
    print(f"Fetched {len(filenames)} files from {fdir}")
    return filenames

