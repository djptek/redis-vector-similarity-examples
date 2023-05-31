import csv
import json
import os
import re
import redis
import sys
import numpy as np
#from redis.commands.search.field import TextField
#from redis.commands.search.field import NumericField
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

def array_of_int_to_blob(a, bytes):
    # a is array e.g. [1, 2, 3]
    # bytes is target number of bytes
    prefix = '\\x'
    match (bytes+len(prefix)):
        case 3:
            dt = np.int8
        case 4:
            dt = np.int16
        case 6:
            dt = np.int32
        case 10:
            dt = np.int64
        case 18:
            dt = np.int128
        case 34:
            dt = np.int256
        case _:
            sys.exit(
                'numpy can\'t fit integers of size {} bytes into blob'.format(
                    bytes))
    return re.sub(r'\'','',re.sub(r'^b','',str(
        np.array(
            a, 
            dtype=dt).tobytes())))

def vector_type(bytes):
    match bytes:
        case 2:
            return 'FLOAT64'
        case 1:
            return 'FLOAT32'
        case _:
            sys.exit(
                'can\'t fit integers of size {} bytes into redis blob'.format(
                    bytes))

with open('data.csv', 'r') as infile:
    reader = csv.reader(infile)
    for row in reader:
        vectors.append({vector_field:list(map(int,row))})

print(vectors)
dimensions = len(vectors[0][vector_field])
print("DIM {}".format(dimensions))

bytes = 2
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
            'TYPE': vector_type(bytes),
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
    # JSON.SET vector:0 $ '{"vec": [176, 7, 28]}'
    print(
        'JSON.SET {}{} $ \'{{"{}": {}}}'.format(
            key_prefix, 
            i, 
            vector_field, 
            vector[vector_field]))
    r.json().set(
        '{}{}'.format(
            key_prefix, 
            i), 
        '$',
        vector)

## FT.CREATE idx:json:cli ON JSON PREFIX 1 vector: SCHEMA $.vec AS vec VECTOR FLAT 6 DIM 3 DISTANCE_METRIC L2 TYPE FLOAT32
## FT.SEARCH idx:json:cli '*=>[KNN 5 @vec $blob AS score]' SORTBY score PARAMS 2 blob \x00\x00\x00 DIALECT 2
## FT.SEARCH idx:json:cli '*=>[KNN 5 @vec $blob AS score]' SORTBY score PARAMS 2 blob \x27\x02\x1b DIALECT 2
## > FT.SEARCH idx:json:vectors '*=>[KNN 5 @vec $blob AS score]' SORTBY score PARAMS 2 blob \x000027\x000002\x00001b DIALECT 2

#count = len(vectors)+1
count = 3
query = (
    Query("*=>[KNN {} @{} $blob AS score]".format(count, vector_field))
     .sort_by(field = "score", asc = False)
     .return_fields("id", "score", "$.vec")
     .dialect(2)
)

for i, vector in enumerate(vectors):
    blob = array_of_int_to_blob(vector[vector_field], bytes)
    query_params = {
        "blob": blob
    }    
    print('Matching {}{} {} blob: {}'.format(key_prefix, i, vector, blob))
    print(r.ft(index_name).search(query, query_params).docs)

