import csv
import json
import numpy as np
import os
import redis
#from redis.commands.search.field import TextField
#from redis.commands.search.field import NumericField
from redis.commands.search.field import VectorField
from redis.commands.search.query import Query
from redis.commands.search.indexDefinition import IndexDefinition, IndexType

rs_host = os.getenv('RS_HOST')
rs_port = os.getenv('RS_PORT')
rs_pass = os.getenv('RS_AUTH')

vectors = []
vector_field = 'vec'

with open('data.csv', 'r') as infile:
    reader = csv.reader(infile)
    for row in reader:
        #vectors.append(np.array(list(row), dtype=np.float32).tobytes())
        vectors.append(np.array(list(row), dtype=np.float32))

dimensions = len(vectors[0])
print("DIM {}".format(dimensions))
index_name = 'idx:blob:vectors' 
key_prefix = 'vector:'
del_keys_lua = """local i = 0 for _,v in ipairs(redis.call('KEYS', ARGV[1])) do i 
= i + redis.call('DEL', v) end return i"""
index_defn = IndexDefinition(
    index_type=IndexType.HASH, 
    prefix=[key_prefix], )
schema = (
    VectorField(
        name = vector_field,
        algorithm = 'FLAT', 
        attributes = {
            'TYPE': 'FLOAT32', 
            'DIM': dimensions, 
            'DISTANCE_METRIC': 'L2'})
)

r = redis.Redis(
    host = rs_host,
    port = rs_port,
    password = rs_pass
)

print('Deleted {} keys with prefix "{}"'.format(
    r.eval(
        del_keys_lua,
        0,
        "{}*".format(key_prefix)),
    key_prefix))

try:
   r.ft(index_name).dropindex()
except redis.exceptions.ResponseError:
   print('First run vs this Endpoint')

r.ft(index_name).create_index(
    fields = schema,
    definition = index_defn)

print(r.ft(index_name).info())

p = r.pipeline()

for i, vector in enumerate(vectors):
    vector_bytes = vector.tobytes()
    print('HSET {}{} {} {}'.format(key_prefix, i, vector_field, vector_bytes))
    p.hset(
        name = '{}{}'.format(key_prefix, i),
        mapping = {vector_field:vector_bytes})

print(p.execute())

# now go find one...

query = (
    Query("*=>[KNN {} @{} $blob as score]".format(len(vectors)+1, vector_field))
        .sort_by(field = "score", asc = True)
        .return_fields("id", "score")
        .dialect(2)
)

for i, vector in enumerate(vectors):
    print(vector)
    blob = vector.tobytes()
    query_params = {
        "blob": blob
    }
    print('Matching {}{} blob {}'.format(key_prefix, i, blob))
    print(r.ft(index_name).search(query, query_params).docs)

# FT.SEARCH my_idx "*=>[KNN 10 @vec $BLOB]" PARAMS 2 BLOB "\x00\x000C\x00\x00\xe0@\x00\x00\xe0A" DIALECT 2