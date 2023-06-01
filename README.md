
# redis-vector-similarity-examples

# redis-vector-similarity-examples

## usage

`./vector_similarity_demo.py [-h] [-j] in_file`

Warning: can and will delete keys and indexes, **** USE AT OWN RISK ****.
Reads Vectors from a csv and indexes to Redis for query by similarity. Define
Redis connection by env variables: [RS_HOST, RS_PORT, RS_AUTH]. Install pre-
reqs with: /usr/bin/pip3 install numpy

positional arguments:
  in_file     csv file containing input data

optional arguments:
  -h, --help  show this help message and exit
  -j          use index type JSON instead of default HASH

## Examples

### Create Index, add 6 HASH type keys each containing a 3D vector & retrieve 1st match for these vectors, matches original key.

```
% ./vector_similarity_demo.py datasets/dates.csv -n 1
Deleted 7 keys with prefix "vector:"
HSET vector:0 vector \x00\x00\x00\x00\x00\x00\xf0\xbf\x00\x00\x00\x00\x00\x00\x14\xc0\x00\x00\x00\x00\x00\x00\x08\xc0
HSET vector:1 vector \x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xc0\x00\x00\x00\x00\x00\x00\x14\xc0
HSET vector:2 vector \x00\x00\x00\x00\x00\x80@@\x00\x00\x00\x00\x00\x00\x14@\x00\x00\x00\x00\x00\x008@
HSET vector:3 vector \x00\x00\x00\x00\x00\x00B@\x00\x00\x00\x00\x00\x00\x08@\x00\x00\x00\x00\x00\x00<@
HSET vector:4 vector \x00\x00\x00\x00\x00\x80C@\x00\x00\x00\x00\x00\x00\x00@\x00\x00\x00\x00\x00\x00;@
HSET vector:5 vector \x00\x00\x00\x00\x00\x00\xf0?\x00\x00\x00\x00\x00\x00\x00@\x00\x00\x00\x00\x00\x00\x08@

Searching idx:hash:vectors by Vector Similarity to [-1. -5. -3.]
[Document {'id': 'vector:0', 'payload': None, 'score': '0', 'vector': '\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x14\x00\x00\x00\x00\x00\x00\x08'}]

Searching idx:hash:vectors by Vector Similarity to [ 0. -2. -5.]
[Document {'id': 'vector:1', 'payload': None, 'score': '0', 'vector': '\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x14'}]

Searching idx:hash:vectors by Vector Similarity to [33.  5. 24.]
[Document {'id': 'vector:2', 'payload': None, 'score': '0', 'vector': '\x00\x00\x00\x00\x00@@\x00\x00\x00\x00\x00\x00\x14@\x00\x00\x00\x00\x00\x008@'}]

Searching idx:hash:vectors by Vector Similarity to [36.  3. 28.]
[Document {'id': 'vector:3', 'payload': None, 'score': '0', 'vector': '\x00\x00\x00\x00\x00\x00B@\x00\x00\x00\x00\x00\x00\x08@\x00\x00\x00\x00\x00\x00<@'}]

Searching idx:hash:vectors by Vector Similarity to [39.  2. 27.]
[Document {'id': 'vector:4', 'payload': None, 'score': '0', 'vector': '\x00\x00\x00\x00\x00C@\x00\x00\x00\x00\x00\x00\x00@\x00\x00\x00\x00\x00\x00;@'}]

Searching idx:hash:vectors by Vector Similarity to [1. 2. 3.]
[Document {'id': 'vector:5', 'payload': None, 'score': '0', 'vector': '\x00\x00\x00\x00\x00\x00?\x00\x00\x00\x00\x00\x00\x00@\x00\x00\x00\x00\x00\x00\x08@'}]
```

### Create Index, add 6 JSON type keys each containing a 3D vector & retrieve top 2 matches for these vectors.

```
% ./vector_similarity_demo.py datasets/dates.csv -j -n 2
Deleted 6 keys with prefix "vector:"
> JSON.SET vector:0 $ {'vector': [-1.0, -5.0, -3.0]}
Pipeline<ConnectionPool<Connection<host=redis-16189.c1.us-central1-2.gce.cloud.redislabs.com,port=16189,db=0>>>
> JSON.SET vector:1 $ {'vector': [0.0, -2.0, -5.0]}
Pipeline<ConnectionPool<Connection<host=redis-16189.c1.us-central1-2.gce.cloud.redislabs.com,port=16189,db=0>>>
> JSON.SET vector:2 $ {'vector': [33.0, 5.0, 24.0]}
Pipeline<ConnectionPool<Connection<host=redis-16189.c1.us-central1-2.gce.cloud.redislabs.com,port=16189,db=0>>>
> JSON.SET vector:3 $ {'vector': [36.0, 3.0, 28.0]}
Pipeline<ConnectionPool<Connection<host=redis-16189.c1.us-central1-2.gce.cloud.redislabs.com,port=16189,db=0>>>
> JSON.SET vector:4 $ {'vector': [39.0, 2.0, 27.0]}
Pipeline<ConnectionPool<Connection<host=redis-16189.c1.us-central1-2.gce.cloud.redislabs.com,port=16189,db=0>>>
> JSON.SET vector:5 $ {'vector': [1.0, 2.0, 3.0]}
Pipeline<ConnectionPool<Connection<host=redis-16189.c1.us-central1-2.gce.cloud.redislabs.com,port=16189,db=0>>>

Searching idx:json:vectors by Vector Similarity to [-1. -5. -3.]
[Document {'id': 'vector:0', 'payload': None, 'score': '0', 'vector': '[-1.0,-5.0,-3.0]'}, Document {'id': 'vector:1', 'payload': None, 'score': '14', 'vector': '[0.0,-2.0,-5.0]'}]

Searching idx:json:vectors by Vector Similarity to [ 0. -2. -5.]
[Document {'id': 'vector:1', 'payload': None, 'score': '0', 'vector': '[0.0,-2.0,-5.0]'}, Document {'id': 'vector:0', 'payload': None, 'score': '14', 'vector': '[-1.0,-5.0,-3.0]'}]

Searching idx:json:vectors by Vector Similarity to [33.  5. 24.]
[Document {'id': 'vector:2', 'payload': None, 'score': '0', 'vector': '[33.0,5.0,24.0]'}, Document {'id': 'vector:3', 'payload': None, 'score': '29', 'vector': '[36.0,3.0,28.0]'}]

Searching idx:json:vectors by Vector Similarity to [36.  3. 28.]
[Document {'id': 'vector:3', 'payload': None, 'score': '0', 'vector': '[36.0,3.0,28.0]'}, Document {'id': 'vector:4', 'payload': None, 'score': '11', 'vector': '[39.0,2.0,27.0]'}]

Searching idx:json:vectors by Vector Similarity to [39.  2. 27.]
[Document {'id': 'vector:4', 'payload': None, 'score': '0', 'vector': '[39.0,2.0,27.0]'}, Document {'id': 'vector:3', 'payload': None, 'score': '11', 'vector': '[36.0,3.0,28.0]'}]

Searching idx:json:vectors by Vector Similarity to [1. 2. 3.]
[Document {'id': 'vector:5', 'payload': None, 'score': '0', 'vector': '[1.0,2.0,3.0]'}, Document {'id': 'vector:1', 'payload': None, 'score': '81', 'vector': '[0.0,-2.0,-5.0]'}]
```

## Notes
Input CSV contains integers, however, using Float solves
- potential ambiguity around 2s complement for negatives
- issues with Python bytes in printable range