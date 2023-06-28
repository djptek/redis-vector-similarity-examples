#!/usr/bin/python3
"""Demo code for Redis Vector Similarity with JSON
Warning: this code can delete keys and indexes, * USE AT OWN RISK *. Read
Vectors from a csv file and index to Redis then query by similarity. Please
define your Redis connection by setting env variables: [RS_HOST, RS_PORT,
RS_AUTH]. Install pre-reqs with: /usr/bin/pip3 install numpy
"""
import argparse
import csv
import os
import redis
import numpy as np

from redis.commands.search.query import Query
from redis.commands.search.field import TagField, VectorField
from redis.commands.search.indexDefinition import IndexDefinition, IndexType


def read_rows():
    """Read csv lines from file into an array of JSON objects containing rows"""
    # hard code to bikes.csv for first attempt
    rows = []
    with open(file=IN_FILE, mode="r", encoding="UTF-8") as in_file:
        reader = csv.reader(in_file)
        headers = next(reader)
        for row in reader:
            rows.append(
                {
                    headers[0]: row[0],
                    headers[1]: row[1],
                    VECTOR_FIELD: np.array(
                        list([row[2], row[3], row[4]]), dtype=np.float64
                    ),
                }
            )
    return rows, len(rows[0]), headers


def del_keys_by_uid():
    """Remove keys matching UIDs of keys to be added"""
    for row in CSV_ROWS:
        print(
            f"""> DEL {
                row[CSV_HEADERS[0]]}\n{
                    MY_REDIS.delete(row[CSV_HEADERS[0]])}"""
        )


def drop_index():
    """Drop index by name advise if it wasn't there"""
    try:
        MY_REDIS.ft(INDEX_NAME).dropindex()
    except redis.exceptions.ResponseError:
        print("First run vs this Endpoint")


def add_rows():
    """Traverse array of rows and set these as JSON in Redis"""
    pipeline = MY_REDIS.pipeline()
    for i, row in enumerate(CSV_ROWS):
        print(row)
        # hard code key to uid
        key = f"{KEY_PREFIX}:{row[CSV_HEADERS[0]]}"
        if INDEX_TYPE == IndexType.JSON:
            pipeline.json().set(
                key,
                "$",
                {
                    CSV_HEADERS[1]: row[CSV_HEADERS[1]],
                    VECTOR_FIELD: row[VECTOR_FIELD].tolist(),
                },
            )
        else:
            pipeline.hset(
                name=key,
                mapping={
                    CSV_HEADERS[0]: row[0],
                    CSV_HEADERS[1]: row[1],
                    VECTOR_FIELD: row[VECTOR_FIELD].tobytes(),
                },
            )
        if i % MAX_PIPELINE == 0:
            # print(f"{'HSET' if INDEX_TYPE == IndexType.HASH else 'JSON.SET'} {key} ...")
            pipeline.execute()
    pipeline.execute()


def search_rows():
    """Traverse array of rows and search Redis for these by vector similarity vs index idx"""
    vs_query = f"[KNN {min(len(CSV_ROWS), MAX_HITS)} @{VECTOR_FIELD} $blob AS score]"
    step = max(1, len(CSV_ROWS) // MAX_QUERIES)
    for i in range(0, min(step * MAX_QUERIES, len(CSV_ROWS)), step):
        row = CSV_ROWS[i]
        print(f"\nSearching {INDEX_NAME} by Vector Similarity to row {i} {row}")
        blob = row[VECTOR_FIELD].tobytes()
        tag = row[CSV_HEADERS[1]]
        pf_query = f"(@{CSV_HEADERS[1]}:{{{tag}}})"
        print(
            MY_REDIS.ft(INDEX_NAME)
            .search(
                query=Query(f"{pf_query}=>{vs_query}")
                .sort_by(field="score", asc=True)
                .return_fields("id", "score", CSV_HEADERS[1], VECTOR_FIELD)
                .dialect(2),
                query_params={"blob": blob},
            )
            .docs
        )


def get_args():
    """Parse args to return index_type, in_file, max_hits"""
    parser = argparse.ArgumentParser(
        description="""
    Warning: can and will delete keys and indexes, **** USE AT OWN RISK ****. 
    Reads Vectors from a csv and indexes to Redis for query by similarity. 
    Define Redis connection by env variables: [RS_HOST, RS_PORT, RS_AUTH]. 
    Install pre-reqs with: /usr/bin/pip3 install numpy"""
    )
    parser.add_argument("in_file", type=str, help="csv file containing input data")
    parser.add_argument(
        "-n",
        dest="max_hits",
        type=int,
        help="maximum number of similar rows to return",
        default=10,
    )
    parser.add_argument(
        "-q",
        dest="max_queries",
        type=int,
        help="maximum number of queries to run",
        default=10,
    )
    parser.add_argument(
        "-j",
        dest="index_type",
        action="store_const",
        const=IndexType.JSON,
        default=IndexType.HASH,
        help="use index type JSON instead of default HASH",
    )
    parser.add_argument(
        "-c",
        dest="distance_metric",
        action="store_const",
        const="COSINE",
        default="L2",
        help="use DISTANCE_METRIC COSINE instead of default L2",
    )

    args = parser.parse_args()
    return (
        args.index_type,
        args.in_file,
        args.max_hits,
        args.max_queries,
        args.distance_metric,
    )


###
INDEX_TYPE, IN_FILE, MAX_HITS, MAX_QUERIES, DISTANCE_METRIC = get_args()
VECTOR_FIELD = "vector"
KEY_PREFIX = IN_FILE[:-4]
INDEX_NAME = f"idx:hybrid:{KEY_PREFIX}"
MAX_PIPELINE = 10000


# collect input csv and calculate vector DIM
CSV_ROWS, DIM, CSV_HEADERS = read_rows()

# connect to Redis
MY_REDIS = redis.Redis(
    host=os.getenv("RS_HOST"), port=os.getenv("RS_PORT"), password=os.getenv("RS_AUTH")
)

# tidy up from previous run(s)
# del_keys_by_prefix()
del_keys_by_uid()
drop_index()

# create Vector Similarity index
## TagField(
##        name=CSV_HEADERS[0] if INDEX_TYPE == IndexType.HASH else f"$.{CSV_HEADERS[0]}",
##        as_name=CSV_HEADERS[0],
##    ),

SCHEMA = (
    TagField(
        name=CSV_HEADERS[1] if INDEX_TYPE == IndexType.HASH else f"$.{CSV_HEADERS[1]}",
        as_name=CSV_HEADERS[1],
    ),
    VectorField(
        name=VECTOR_FIELD if INDEX_TYPE == IndexType.HASH else f"$.{VECTOR_FIELD}",
        algorithm="FLAT",
        attributes={
            "TYPE": "FLOAT64",
            "DIM": DIM,
            "DISTANCE_METRIC": DISTANCE_METRIC,
        },
        as_name=VECTOR_FIELD,
    ),
)
MY_REDIS.ft(INDEX_NAME).create_index(
    fields=SCHEMA,
    definition=IndexDefinition(index_type=INDEX_TYPE, prefix=[f"{KEY_PREFIX}:"]),
)

# index the data as rows
add_rows()

# run vector similarity searches over the data
search_rows()
