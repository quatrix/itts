# Islands in the stream

# What?


# Why?


# Limitations

In the current implementation when a slice is inserted it tries to find
the relevant segments and update just them. this works fine while we have
a bunch of slices with different statuses as it will create many segments.

```
requests = 100_000
total time: 5.62 seconds
rate: 17777 requests/second
```

Problem starts where we have long streches of the same status, for example
when we have 10000 slices all with status 'DONE', basically for every new 
slice we need to go over all slices! so it's N^2, not ideal.

```
requests = 10_000
total time: 28.34 seconds
rate: 352 requests/second

last insert time: 6.45 ms
```

basically doubling the requests, halfs the rate

```
requets = 20_000
total time: 125.36 seconds 
rate: 159 requests/second
last insert time: 13.09 ms
```


## Possible solutions
* write a more sophisticated merging mechanism
* maybe have a max size per segment, for example segment can't be longer than a day
* do the merging lazily, not on every insert either on demand (with caching until a new slice is inserted) or after X inserts or T time passes

## More sophisticated merging mechanism

When a new slice arrives find the segments closes to it
* if it's same status and after/before them, merge them with it
* if it's a different status, add a new segment after/before it
* if it's same status in the middle of a segment - do nothing
* if it's a different status in the middle of a segment get the slices before and after 
  it, remove the segment and create 3 new segments [before][now][after] using the slices.
