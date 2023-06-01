import csv
import json
import os
import redis
import numpy as np

from redis.commands.search.query import Query
from redis.commands.search.field import VectorField
from redis.commands.search.indexDefinition import IndexDefinition, IndexType

rs_host = os.getenv('RS_HOST')
rs_port = os.getenv('RS_PORT')
rs_pass = os.getenv('RS_AUTH')

def read_vectors(file, field):
    vectors = []
    with open('data.csv', 'r') as infile:
        reader = csv.reader(infile)
        for row in reader:
            vectors.append(
                 {vector_field:np.array(
                    list(row), 
                    dtype=np.float32)})
    return vectors, len(vectors[0][vector_field])

def del_keys_by_prefix(r, prefix):
    print('Deleted {} keys with prefix "{}"'.format(
        r.eval(
            """local i = 0 for _,v in ipairs(redis.call('KEYS', ARGV[1])) do i 
= i + redis.call('DEL', v) end return i""",
            0,
         "{}*".format(key_prefix)),
        key_prefix))

def drop_index(name):
    try:
        r.ft(index_name).dropindex()
    except redis.exceptions.ResponseError:
        print('First run vs this Endpoint')

def create_index(name, vf, d, r):
    print(r.ft(name).create_index(
        fields = (
            VectorField(
                name = '$.{}'.format(vf),
                algorithm = 'FLAT', 
                attributes = {
                    'TYPE': 'FLOAT32',
                    'DIM': d, 
                    'DISTANCE_METRIC': 'L2'},
                as_name = vector_field)),
        definition = 
            IndexDefinition(
                index_type=IndexType.JSON, 
                prefix=[key_prefix])))
    
def index_vectors(vs, vf, kp):
    for i, vector in enumerate(vs):
        j = {vf:vector[vector_field].tolist()}
        print(
            '> JSON.SET {}{} $ {}'.format(
                kp, 
                i, 
                j))
        print(r.json().set(
            '{}{}'.format(
                kp, 
                i), 
            '$',
            j))
        
def search_vectors(idx, vs, vf, m):
    vs_query = "*=>[KNN {} @{} $blob AS score]".format(
        min(len(vs)+1, m), 
        vector_field)
    q = (
        Query(vs_query)
        .sort_by(field = "score", asc = True)
        .return_fields("id", "score", "$.vec")
        .dialect(2))
    dbg_query = "> FT.SEARCH {} \'{}\' {} {{}} DIALECT 2".format(
        index_name, 
        vs_query,
        "SORTBY score PARAMS 2 blob")
    for vector in vectors:
        print("Searching {}".format(repr(vector)))
        blob = vector[vf].tobytes()
        print(dbg_query.format(repr(blob)[2:-1]))
        print(r.ft(idx).search(
            query = q, 
            query_params = {"blob": blob}).docs)

vector_field = 'vec'
index_name = 'idx:json:vectors' 
key_prefix = 'vector:'
max_results = 10

vectors, dimensions = read_vectors(
    file = 'data.csv',
    field = vector_field)

r = redis.Redis(
    host = rs_host,
    port = rs_port,
    password = rs_pass
)

del_keys_by_prefix(r, key_prefix)
drop_index(name = index_name)
create_index(
    name = index_name,
    vf = vector_field,
    d = dimensions,
    r = r)
index_vectors(
    vs = vectors, 
    vf = vector_field, 
    kp = key_prefix)
search_vectors(
    idx = index_name,
    vs = vectors,
    vf = vector_field,
    m = max_results)
