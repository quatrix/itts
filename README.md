# Islands In The Stream

# What?

merged events with timestamp and status (slices) to continues segments in time of the same status 
from this (slices):

![slices](/svgs/slices.svg)

to this (segments):

![segments](/svgs/segments.svg)

* this is done continously, on every insert of a new slice (or status update of an existing slice) segments are adjusted.
* internally it's implmented on Redis Sorted Sets where timestamp used as score, so time ranges queries are cheap.

# Why?

it could be an interesting way of monitoring/visualizing progress that has time grouping meaning.

for example if we have a worker that executes tasks we might want to know not only number of tasks done but also if we have segments of done, i.e day/week/month of done.

# Example

```python
from itts import ITTS, SliceStatus
from redis import Redis

t = ITTS(Redis(), 'some_key')

# inserting slices

t.insert_slice(timestamp=5, status=SliceStatus.PENDING)
t.insert_slice(timestamp=6, status=SliceStatus.DONE)
t.insert_slice(timestamp=7, status=SliceStatus.PENDING)
t.insert_slice(timestamp=10, status=SliceStatus.DONE)
t.insert_slice(timestamp=15, status=SliceStatus.DONE)

# get a range of segments

t.get_segments(start=5, end=20)
```

# Preformance

Tested on a Apple Silicon Macbook Pro with Redis running locally (not in docker)

Both tests inserting slices in random order


## status is the same across silces
```
[requests: 20000] total time: 1.33 seconds | rate: 14995.78 requests/second
```

## status is randomized (pending or done)
```
[requests: 20000] total time: 1.67 seconds | rate: 12009.92 requests/second
```

# Future work

* segments can be used to `reduce()` other things from slices, for example counting them.
