import csv
import os
import redis
import numpy as np

from redis.commands.search.query import Query
from redis.commands.search.field import VectorField
from redis.commands.search.indexDefinition import IndexDefinition, IndexType

rs_host = os.getenv('RS_HOST')
rs_port = os.getenv('RS_PORT')
rs_pass = os.getenv('RS_AUTH')

del_keys_lua = """local i = 0 for _,v in ipairs(redis.call('KEYS', ARGV[1])) do i 
= i + redis.call('DEL', v) end return i"""

vectors = []
vector_field = 'vec'
                    
with open('data.csv', 'r') as infile:
    reader = csv.reader(infile)
    for row in reader:
        vectors.append({vector_field:np.array(list(row), dtype=np.float32)})

print(vectors)
dimensions = len(vectors[0][vector_field])
print("DIM {}".format(dimensions))

index_name = 'idx:json:vectors' 
json_prefix = '$.'
key_prefix = 'vector:'
index_defn = IndexDefinition(
    index_type=IndexType.JSON, 
    prefix=[key_prefix])
schema = (
    VectorField(
        name = '{}{}'.format(
            json_prefix,
            vector_field),
        algorithm = 'FLAT', 
        attributes = {
            'TYPE': 'FLOAT32',
            'DIM': dimensions, 
            'DISTANCE_METRIC': 'L2'},
        as_name = vector_field)
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
    schema,
    definition = index_defn)

print(r.ft(index_name).info())

for i, vector in enumerate(vectors):
    j = {vector_field:vector[vector_field].tolist()}
    print(
        '> JSON.SET {}{} $ {}'.format(
            key_prefix, 
            i, 
            j))
    r.json().set(
        '{}{}'.format(
            key_prefix, 
            i), 
        '$',
        j)

## FT.CREATE idx:json:cli ON JSON PREFIX 1 vector: SCHEMA $.vec AS vec VECTOR FLAT 6 DIM 3 DISTANCE_METRIC L2 TYPE FLOAT32
## FT.SEARCH idx:json:cli '*=>[KNN 5 @vec $blob AS score]' SORTBY score PARAMS 2 blob \x00\x00\x00 DIALECT 2
## FT.SEARCH idx:json:cli '*=>[KNN 5 @vec $blob AS score]' SORTBY score PARAMS 2 blob \x27\x02\x1b DIALECT 2
## > FT.SEARCH idx:json:vectors '*=>[KNN 5 @vec $blob AS score]' SORTBY score PARAMS 2 blob \x000027\x000002\x00001b DIALECT 2

count = len(vectors)+1

q = "*=>[KNN {} @{} $blob AS score]".format(count, vector_field)
query = (
    Query(q)
     .sort_by(field = "score", asc = True)
     .return_fields("id", "score", "$.vec")
     .dialect(2)
)

s = "> FT.SEARCH {} '*=>[KNN {} @vec $blob AS score]' SORTBY score PARAMS 2 blob {{}} DIALECT 2".format(index_name, count)
for i, vector in enumerate(vectors):
    blob = vector[vector_field].tobytes()
    query_params = { 
        "blob": blob
    }    
    print(s.format(repr(blob)[2:-1]))
    print(r.ft(index_name).search(query, query_params).docs)

