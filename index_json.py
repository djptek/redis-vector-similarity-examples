"""Demo code for Redis Vector Similarity with JSON
Warning: this code can delete keys and indexes, use at your own risk

Please define your redis connection by defining the appropriate env variables
RS_HOST
RS_PORT
RS_AUTH
"""

import csv
import os
import redis
import numpy as np

from redis.commands.search.query import Query
from redis.commands.search.field import VectorField
from redis.commands.search.indexDefinition import IndexDefinition, IndexType


def read_vectors(in_file):
    """Read csv lines from file into an array of JSON objects containing vectors"""
    vectors = []
    with open(file=in_file, mode="r", encoding="UTF-8") as infile:
        reader = csv.reader(infile)
        for row in reader:
            vectors.append(np.array(list(row), dtype=np.float32))
    return vectors, len(vectors[0])


def del_keys_by_prefix(redis_instance, prefix):
    """EVAL Lua to delete a set of keys by prefix * USE AT OWN RISK *"""
    print(
        'Deleted {} keys with prefix "{}"'.format(
            redis_instance.eval(
                """local i = 0 for _,v in ipairs(redis.call('KEYS', ARGV[1])) do i 
= i + redis.call('DEL', v) end return i""",
                0,
                f"{prefix}*",
            ),
            prefix,
        )
    )


def drop_index(redis_instance, name):
    """Drop index by name advise if it wasn't there"""
    try:
        redis_instance.ft(name).dropindex()
    except redis.exceptions.ResponseError:
        print("First run vs this Endpoint")


def create_index(redis_instance, name, idx_type, vector_field, dim):
    """Create an index suitable to index Vectors of length read from csv"""
    print(
        redis_instance.ft(name).create_index(
            fields=(
                VectorField(
                    name=f"$.{vector_field}",
                    algorithm="FLAT",
                    attributes={"TYPE": "FLOAT32", "DIM": dim, "DISTANCE_METRIC": "L2"},
                    as_name=vector_field,
                )
            ),
            definition=IndexDefinition(index_type=idx_type, prefix=[KEY_PREFIX]),
        )
    )


def index_vectors(redis_instance, vectors, vector_field, key_prefix):
    """Traverse array of vectors and set these as JSON in Redis"""
    for i, vector in enumerate(vectors):
        j = {vector_field: vector.tolist()}
        print(f"> JSON.SET {key_prefix}{i} $ {j}")
        print(redis_instance.json().set(f"{key_prefix}{i}", "$", j))


def search_vectors(redis_instance, idx, vectors, vector_field, max_hits):
    """Traverse array of vectors and search Redis for these by vector similatory vs index idx"""
    vs_query = (
        f"*=>[KNN {min(len(vectors) + 1, max_hits)} @{vector_field} $blob AS score]"
    )
    dbg_query = f"> FT.SEARCH {INDEX_NAME} '{vs_query}' SORTBY score PARAMS 2 blob {{}} DIALECT 2"
    for vector in vectors:
        print(f"Searching {repr(vector)}")
        blob = vector.tobytes()
        print(dbg_query.format(repr(blob)[2:-1]))
        print(
            redis_instance.ft(idx)
            .search(
                query=Query(vs_query)
                .sort_by(field="score", asc=True)
                .return_fields("id", "score", f"$.{vector_field}")
                .dialect(2),
                query_params={"blob": blob},
            )
            .docs
        )


# Set INDEX_TYPE to IndexType.JSON or IndexType.HASH
INDEX_TYPE = IndexType.JSON
VECTOR_FIELD = "vector"
INDEX_NAME = f"idx:{INDEX_TYPE.name.lower()}:vectors"
KEY_PREFIX = f"{VECTOR_FIELD}:"
MAX_RESULTS = 10

my_vectors, schema_dimension = read_vectors(in_file="data.csv")

my_redis = redis.Redis(
    host=os.getenv("RS_HOST"), port=os.getenv("RS_PORT"), password=os.getenv("RS_AUTH")
)

del_keys_by_prefix(redis_instance=my_redis, prefix=KEY_PREFIX)
drop_index(redis_instance=my_redis, name=INDEX_NAME)
create_index(
    redis_instance=my_redis,
    name=INDEX_NAME,
    idx_type=INDEX_TYPE,
    vector_field=VECTOR_FIELD,
    dim=schema_dimension,
)
index_vectors(
    redis_instance=my_redis,
    vectors=my_vectors,
    vector_field=VECTOR_FIELD,
    key_prefix=KEY_PREFIX,
)
search_vectors(
    redis_instance=my_redis,
    idx=INDEX_NAME,
    vectors=my_vectors,
    vector_field=VECTOR_FIELD,
    max_hits=MAX_RESULTS,
)
