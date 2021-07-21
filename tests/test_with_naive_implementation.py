from pydantic import BaseModel
from redis import Redis
import pytest
import random
import string

from itts import ITTS, SliceStatus, Segment

def create_random_itts():
    random_key = ''.join(random.choices(string.ascii_uppercase + string.digits, k=32))
    return ITTS(Redis(), f'tl::{random_key}')

class Slice(BaseModel):
    timestamp: int
    status: SliceStatus


def merge_to_segments(slices: list[Slice]):
    if not slices:
        return []

    # in redis slices are stored in a sorted set
    # so we're sorting slices to replicate that
    slices = sorted(slices, key=lambda s: s.timestamp)
    segments = []

    last_status = slices[0].status
    start = end = slices[0].timestamp

    def add_segment():
        segments.append(Segment(
            start = start, 
            end = end,
            status = last_status
        ))

    for s in slices[1:]:
        if last_status == s.status:
            end = s.timestamp
        else:
            add_segment()
            last_status = s.status
            start = end = s.timestamp

    # don't forget the last segment
    add_segment()

    return segments

def create_random_slices(n: int):
    timestamps = list(range(n))
    random.shuffle(timestamps)

    return [Slice(timestamp=t, status=random.choice(list(SliceStatus))) for t in timestamps]

def test_naive():
    """
    testing the real algorthim against a naive implementation
    """

    n = 1000 
    slices = create_random_slices(n)
    expected_segments = merge_to_segments(slices)

    t = create_random_itts()

    for s in slices:
        t.insert_slice(timestamp=s.timestamp, status=s.status)

    assert t.get_segments(0, n) == expected_segments


def test_naive_all_done_one_pending_in_the_middle():
    """
    Testing a sequence of slices where all are done and one is pending
    this should create 3 segments [pending][done][pending]
    """

    n = 1000 

    slices = [Slice(timestamp=t, status=SliceStatus.PENDING) for t in list(range(n))]
    slices[n//2].status = SliceStatus.DONE
    random.shuffle(slices)

    expected_segments = merge_to_segments(slices)

    t = create_random_itts()

    for s in slices:
        t.insert_slice(timestamp=s.timestamp, status=s.status)

    actual = t.get_segments(0, n)

    assert actual == expected_segments
    assert actual == [
        Segment(start=0, end=499, status=SliceStatus.PENDING),
        Segment(start=500, end=500, status=SliceStatus.DONE),
        Segment(start=501, end=999, status=SliceStatus.PENDING),
    ]
