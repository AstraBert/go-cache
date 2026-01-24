# go-cache

Single and distributed key-value store built with Go.

## Main features (single node)

- [X] TTL-based caching
- [X] `POST /cache` and `GET /cache/{key}` endpoints.
- [X] Containerized image with Docker  
- [X] Persisten Disk caching: entries are first written to disk and then synced to memory (using concurrent workers with mutexes). In this way, if the server crashes, the disk will always have the latest version of stored key-value pairs
- [X] TTL- and key-based deduplication: as a rule, the cache accepts multiple `POST /cache` request with the same key and different values, holding in memory only the newest one. The disk, tho, could potentially hold all of them: that's why we deduplicate every second (using a concurrent worker). The deduplication worker gets rid also of the expired entries.
- [X] Max cache size with TTL-based eviction policy
- [X] Rate limiting

### Usage

Build Docker image:

```bash
cd single-node/
docker build . -t single-node-cache
```

Run:

```bash
docker run -p 8000:8000 \
    --env MAX_CACHE_SIZE=1000 \
    --env CACHE_RATE_LIMIT_GET=30000 \
    --env CACHE_RATE_LIMIT_SET=3000 \
    single-node-cache
```

As you can see, you can set:

- the maximum size of the cache (`MAX_CACHE_SIZE`). Setting the maximum size to 0 or less will result in an infinite size.
- the maximum number of POST requests to the cache in a minute (`CACHE_RATE_LIMIT_SET`)
- the maximum number of GET requests to the cache in a minute (`CACHE_RATE_LIMIT_GET`)

Send requests:

```python
import requests

body = {"key": "hello", "value": "world", "ttl": 10}
response = requests.post("http://localhost:8000/cache", json=body)
print(response.status_code) # should print 201
response = requests.get("http://localhost/cache/hello")
print(response.json()) # should print {'value': 'world'}
```

## Future work

This repository will soon contain a distributed key-value store built on top of the [RAFT consensus algorithm](https://raft.github.io).
