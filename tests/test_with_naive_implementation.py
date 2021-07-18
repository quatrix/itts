from pydantic import BaseModel
from redis import Redis
import random
import string

from time_lord import TimeLord, SliceStatus, Segment

def create_random_timelord():
    random_key = ''.join(random.choices(string.ascii_uppercase + string.digits, k=32))
    return TimeLord(Redis(), f'tl::{random_key}')

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

    t = create_random_timelord()

    for s in slices:
        t.insert_slice(timestamp=s.timestamp, status=s.status)

    assert t.get_segments(0, n) == expected_segments
