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
from redis.commands.search.field import VectorField
from redis.commands.search.indexDefinition import IndexDefinition, IndexType


def read_vectors():
    """Read csv lines from file into an array of JSON objects containing vectors"""
    vectors = []
    with open(file=IN_FILE, mode="r", encoding="UTF-8") as in_file:
        reader = csv.reader(in_file)
        for row in reader:
            vectors.append(np.array(list(row), dtype=np.float64))
    return vectors, len(vectors[0])


def del_keys_by_prefix(redis_instance):
    """EVAL Lua to delete a set of keys by prefix * USE AT OWN RISK *"""
    print(
        'Deleted {} keys with prefix "{}"'.format(
            redis_instance.eval(
                """local i = 0 for _,v in ipairs(redis.call('KEYS', ARGV[1])) 
do i = i + redis.call('DEL', v) end return i""",
                0,
                f"{KEY_PREFIX}*",
            ),
            KEY_PREFIX,
        )
    )


def drop_index(redis_instance):
    """Drop index by name advise if it wasn't there"""
    try:
        redis_instance.ft(INDEX_NAME).dropindex()
    except redis.exceptions.ResponseError:
        print("First run vs this Endpoint")


def json_set(pipeline, vector, key):
    """JSON.SET a vector to Redis"""
    j = {VECTOR_FIELD: vector.tolist()}
    print(f"> JSON.SET {key} $ {j}")
    pipeline.json().set(key, "$", j)


def blob_hset(pipeline, vector, key):
    """HSET a vector to Redis"""
    vector_bytes = vector.tobytes()
    print(f"HSET {key} {VECTOR_FIELD} {repr(vector_bytes)[2:-1]}")
    pipeline.hset(name=key, mapping={VECTOR_FIELD: vector_bytes})


def add_vectors(redis_instance, vectors):
    """Traverse array of vectors and set these as JSON in Redis"""
    pipe = redis_instance.pipeline()
    for i, vector in enumerate(vectors):
        key = f"{KEY_PREFIX}{i}"
        if INDEX_TYPE == IndexType.JSON:
            json_set(pipeline=pipe, vector=vector, key=key)
        else:
            blob_hset(pipeline=pipe, vector=vector, key=key)
        if i % MAX_PIPELINE == 0:
            pipe.execute()
    pipe.execute()


def search_vectors(redis_instance, vectors):
    """Traverse array of vectors and search Redis for these by vector similarity vs index idx"""
    vs_query = (
        f"*=>[KNN {min(len(vectors) + 1, MAX_HITS)} @{VECTOR_FIELD} $blob AS score]"
    )
    for vector in vectors:
        print(f"\nSearching {INDEX_NAME} by Vector Similarity to {vector}")
        blob = vector.tobytes()
        print(
            redis_instance.ft(INDEX_NAME)
            .search(
                query=Query(vs_query)
                .sort_by(field="score", asc=True)
                .return_fields("id", "score", VECTOR_FIELD)
                .dialect(DIALECT),
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
        help="maximum number of similar vectors to return",
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

    return args.index_type, args.in_file, args.max_hits, args.distance_metric


###
INDEX_TYPE, IN_FILE, MAX_HITS, DISTANCE_METRIC = get_args()
VECTOR_FIELD = "vector"
INDEX_NAME = f"idx:{INDEX_TYPE.name.lower()}:vectors"
KEY_PREFIX = f"{VECTOR_FIELD}:"
MAX_PIPELINE = 10000
DIALECT = 2

# collect input csv and calculate vector DIM
my_vectors, dim = read_vectors()

# connect to Redis
my_redis = redis.Redis(
    host=os.getenv("RS_HOST"), port=os.getenv("RS_PORT"), password=os.getenv("RS_AUTH")
)

# tidy up from previous run(s)
del_keys_by_prefix(redis_instance=my_redis)
drop_index(redis_instance=my_redis)

# create Vector Similarity index
my_redis.ft(INDEX_NAME).create_index(
    fields=(
        VectorField(
            name=VECTOR_FIELD if INDEX_TYPE == IndexType.HASH else f"$.{VECTOR_FIELD}",
            algorithm="FLAT",
            attributes={
                "TYPE": "FLOAT64",
                "DIM": dim,
                "DISTANCE_METRIC": DISTANCE_METRIC,
            },
            as_name=VECTOR_FIELD,
        )
    ),
    definition=IndexDefinition(index_type=INDEX_TYPE, prefix=[KEY_PREFIX]),
)

# index the data as vectors
add_vectors(
    redis_instance=my_redis,
    vectors=my_vectors,
)

# run vector similarity searches over the data
search_vectors(
    redis_instance=my_redis,
    vectors=my_vectors,
)
