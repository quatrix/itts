import random
import string
from redis import Redis

from time_lord import TimeLord, SliceStatus, Segment

def create_random_timelord():
    random_key = ''.join(random.choices(string.ascii_uppercase + string.digits, k=32))
    return TimeLord(Redis(), f'tl::{random_key}')


def test_empty():
    """
    no slices should return no segments
    """

    t = create_random_timelord()

    assert t.get_segments(0, 1) == []

def test_with_one_slice():
    """
    one slice should return one segment
    """

    t = create_random_timelord()
    t.insert_slice(timestamp=5, status=SliceStatus.PENDING)

    expected = [
        Segment(start=5, end=5, status=SliceStatus.PENDING),
    ]

    assert t.get_segments(0, 1) == []
    assert t.get_segments(0, 6) == expected

def test_with_one_slice_after_changing_status():
    """
    if we have a pending slice and then it was done (same id)
    we should have one segment which is done
    """

    t = create_random_timelord()
    t.insert_slice(timestamp=5, status=SliceStatus.PENDING)
    t.insert_slice(timestamp=5, status=SliceStatus.DONE)

    expected = [
        Segment(start=5, end=5, status=SliceStatus.DONE),
    ]

    assert t.get_segments(0, 1) == []
    assert t.get_segments(0, 6) == expected

def test_inserting_another_slice_after():
    """
    if we have one pending slice and we insert another pending slice after it
    then the segment will be the duration between both slices and it will be pending
    """

    t = create_random_timelord()
    t.insert_slice(timestamp=5, status=SliceStatus.PENDING)
    t.insert_slice(timestamp=10, status=SliceStatus.PENDING)

    expected = [
        Segment(start=5, end=10, status=SliceStatus.PENDING),
    ]

    print(t.get_segments(0, 20))
    assert t.get_segments(0, 1) == []
    assert t.get_segments(0, 20) == expected

def test_inserting_another_slice_before():
    """
    if we have one pending slice and we insert another pending slice bofore it
    then the segment will be the duration between both slices and it will be pending
    """

    t = create_random_timelord()
    t.insert_slice(timestamp=10, status=SliceStatus.PENDING)
    t.insert_slice(timestamp=5, status=SliceStatus.PENDING)

    expected = [
        Segment(start=5, end=10, status=SliceStatus.PENDING),
    ]

    assert t.get_segments(0, 1) == []
    assert t.get_segments(0, 20) == expected

def test_inserting_slice_between_two_slices():
    """
    when inserting multiple slices of same status
    they should be merged into one segment that starts with
    the minimum timestamp and ends with the maximum timestamp
    """

    t = create_random_timelord()
    t.insert_slice(timestamp=10, status=SliceStatus.PENDING)
    t.insert_slice(timestamp=5, status=SliceStatus.PENDING)
    t.insert_slice(timestamp=7, status=SliceStatus.PENDING)

    expected = [
        Segment(start=5, end=10, status=SliceStatus.PENDING),
    ]

    assert t.get_segments(0, 1) == []
    assert t.get_segments(0, 20) == expected

def test_inserting_two_slices_of_different_statuses():
    """
    inserting two slices with different statuses
    should create two segments

    they should be ordered by id
    """

    t = create_random_timelord()
    t.insert_slice(timestamp=5, status=SliceStatus.PENDING)
    t.insert_slice(timestamp=10, status=SliceStatus.DONE)

    expected = [
        Segment(start=5, end=5, status=SliceStatus.PENDING),
        Segment(start=10, end=10, status=SliceStatus.DONE),
    ]

    assert t.get_segments(0, 1) == []
    assert t.get_segments(0, 20) == expected

def test_inserting_another_done_slice_at_the_end():
    """
    if we have a bunch of slices and the last one is of some status
    inserting another slice of same status should extend last segment
    """

    t = create_random_timelord()
    t.insert_slice(timestamp=5, status=SliceStatus.PENDING)
    t.insert_slice(timestamp=6, status=SliceStatus.DONE)
    t.insert_slice(timestamp=7, status=SliceStatus.PENDING)
    t.insert_slice(timestamp=10, status=SliceStatus.DONE)
    t.insert_slice(timestamp=15, status=SliceStatus.DONE)

    expected = [
        Segment(start=5, end=5, status=SliceStatus.PENDING),
        Segment(start=6, end=6, status=SliceStatus.DONE),
        Segment(start=7, end=7, status=SliceStatus.PENDING),
        Segment(start=10, end=15, status=SliceStatus.DONE),
    ]


    assert t.get_segments(0, 1) == []
    assert t.get_segments(0, 20) == expected

def test_inserting_another_pending_slice_at_the_end():
    """
    if we have a bunch of slices and the last one is of some status
    inserting another slice of a different status should create another 
    segment at the end
    """

    t = create_random_timelord()
    t.insert_slice(timestamp=5, status=SliceStatus.PENDING)
    t.insert_slice(timestamp=6, status=SliceStatus.DONE)
    t.insert_slice(timestamp=7, status=SliceStatus.PENDING)
    t.insert_slice(timestamp=10, status=SliceStatus.DONE)
    t.insert_slice(timestamp=15, status=SliceStatus.PENDING)

    expected = [
        Segment(start=5, end=5, status=SliceStatus.PENDING),
        Segment(start=6, end=6, status=SliceStatus.DONE),
        Segment(start=7, end=7, status=SliceStatus.PENDING),
        Segment(start=10, end=10, status=SliceStatus.DONE),
        Segment(start=15, end=15, status=SliceStatus.PENDING),
    ]

    assert t.get_segments(0, 1) == []
    assert t.get_segments(0, 20) == expected

def test_inserting_another_pending_slice_at_the_begining():
    """
    if we have a bunch of slices and the first one is of some status
    inserting another slice of same status before it should extend first segment
    """

    t = create_random_timelord()
    t.insert_slice(timestamp=5, status=SliceStatus.PENDING)
    t.insert_slice(timestamp=10, status=SliceStatus.DONE)
    t.insert_slice(timestamp=15, status=SliceStatus.PENDING)
    t.insert_slice(timestamp=2, status=SliceStatus.PENDING)

    expected = [
        Segment(start=2, end=5, status=SliceStatus.PENDING),
        Segment(start=10, end=10, status=SliceStatus.DONE),
        Segment(start=15, end=15, status=SliceStatus.PENDING),
    ]

    assert t.get_segments(0, 1) == []
    assert t.get_segments(0, 20) == expected

def test_inserting_another_done_slice_at_the_begining():
    """
    if we have a bunch of slices and the first one is of some status
    inserting another slice of a different status before it should 
    create another segment of that status before the first segment
    """

    t = create_random_timelord()

    t.insert_slice(timestamp=5, status=SliceStatus.PENDING)
    t.insert_slice(timestamp=10, status=SliceStatus.DONE)
    t.insert_slice(timestamp=12, status=SliceStatus.PENDING)
    t.insert_slice(timestamp=15, status=SliceStatus.DONE)
    t.insert_slice(timestamp=2, status=SliceStatus.DONE)

    expected = [
        Segment(start=2, end=2, status=SliceStatus.DONE),
        Segment(start=5, end=5, status=SliceStatus.PENDING),
        Segment(start=10, end=10, status=SliceStatus.DONE),
        Segment(start=12, end=12, status=SliceStatus.PENDING),
        Segment(start=15, end=15, status=SliceStatus.DONE),
    ]

    assert t.get_segments(0, 1) == []
    assert t.get_segments(0, 20) == expected

def test_inserting_different_status_same_id():
    """
    inserting a slice of same id and different status
    should replace previous slice
    """

    t = create_random_timelord()

    t.insert_slice(timestamp=10, status=SliceStatus.PENDING)
    t.insert_slice(timestamp=10, status=SliceStatus.DONE)

    expected = [
        Segment(start=10, end=10, status=SliceStatus.DONE),
    ]

    assert t.get_segments(0, 1) == []
    assert t.get_segments(0, 20) == expected


def test_inserting_different_status_within_segment():
    """
    if we have a bunch of slices of one status that would 
    turn into one segment, if we insert a slie of a different
    status in the middle of the segment, it should split it
    to two segments of the prev status and in the middle a segment
    of the new status
    """

    t = create_random_timelord()

    t.insert_slice(timestamp=10, status=SliceStatus.PENDING)
    t.insert_slice(timestamp=15, status=SliceStatus.PENDING)
    t.insert_slice(timestamp=20, status=SliceStatus.PENDING)
    t.insert_slice(timestamp=25, status=SliceStatus.PENDING)
    t.insert_slice(timestamp=17, status=SliceStatus.DONE)

    expected = [
        Segment(start=10, end=15, status=SliceStatus.PENDING),
        Segment(start=17, end=17, status=SliceStatus.DONE),
        Segment(start=20, end=25, status=SliceStatus.PENDING),
    ]

    assert t.get_segments(0, 1) == []
    assert t.get_segments(0, 30) == expected

def test_inserting_within_existing_segment_different_status():
    """
    if we already have a segment with a status
    inserting a slice with a different status that falls inside
    that segment should split it to three parts
    """

    t = create_random_timelord()

    t.insert_slice(timestamp=5, status=SliceStatus.PENDING)
    t.insert_slice(timestamp=15, status=SliceStatus.PENDING)
    t.insert_slice(timestamp=10, status=SliceStatus.DONE)

    expected = [
        Segment(start=5, end=5, status=SliceStatus.PENDING),
        Segment(start=10, end=10, status=SliceStatus.DONE),
        Segment(start=15, end=15, status=SliceStatus.PENDING),

    ]

    assert t.get_segments(0, 1) == []
    assert t.get_segments(0, 25) == expected


def test_inserting_within_existing_segment_with_multiple_slices_different_status():
    """
    if we already have a segment that made of multiple slices with same status
    inserting a slice with a different status that falls inside
    that segment should split it to three parts
    """

    t = create_random_timelord()

    t.insert_slice(timestamp=5, status=SliceStatus.PENDING)
    t.insert_slice(timestamp=8, status=SliceStatus.PENDING)
    t.insert_slice(timestamp=12, status=SliceStatus.PENDING)
    t.insert_slice(timestamp=15, status=SliceStatus.PENDING)
    t.insert_slice(timestamp=10, status=SliceStatus.DONE)

    expected = [
        Segment(start=5, end=8, status=SliceStatus.PENDING),
        Segment(start=10, end=10, status=SliceStatus.DONE),
        Segment(start=12, end=15, status=SliceStatus.PENDING),
    ]

    assert t.get_segments(0, 1) == []
    assert t.get_segments(0, 25) == expected

def test_inserting_slice_at_the_edge_of_a_segment():
    """
    if we already have a segment that made of multiple slices with same status
    inserting a slice with a different status that falls inside
    that segment should split it to three parts
    """

    t = create_random_timelord()

    t.insert_slice(timestamp=5, status=SliceStatus.PENDING)
    t.insert_slice(timestamp=10, status=SliceStatus.PENDING)
    t.insert_slice(timestamp=10, status=SliceStatus.DONE)

    expected = [
        Segment(start=5, end=5, status=SliceStatus.PENDING),
        Segment(start=10, end=10, status=SliceStatus.DONE),
    ]

    assert t.get_segments(0, 1) == []
    assert t.get_segments(0, 25) == expected

def test_inserting_slice_at_the_end_of_a_segment_multi_sliced():
    """
    if we already have a segment that made of multiple slices with same status
    inserting a slice with a different status at the end of the segment
    should shorten the segment to the prev slice and add another segment
    """

    t = create_random_timelord()

    t.insert_slice(timestamp=5, status=SliceStatus.PENDING)
    t.insert_slice(timestamp=7, status=SliceStatus.PENDING)
    t.insert_slice(timestamp=10, status=SliceStatus.PENDING)
    t.insert_slice(timestamp=10, status=SliceStatus.DONE)

    expected = [
        Segment(start=5, end=7, status=SliceStatus.PENDING),
        Segment(start=10, end=10, status=SliceStatus.DONE),
    ]

    assert t.get_segments(0, 1) == []
    assert t.get_segments(0, 25) == expected

def test_inserting_slice_at_the_begining_of_a_segment_multi_sliced():
    """
    if we already have a segment that made of multiple slices with same status
    inserting a slice with a different status at the end of the segment
    should shorten the segment to the prev slice and add another segment
    """

    t = create_random_timelord()

    t.insert_slice(timestamp=5, status=SliceStatus.PENDING)
    t.insert_slice(timestamp=7, status=SliceStatus.PENDING)
    t.insert_slice(timestamp=10, status=SliceStatus.PENDING)
    t.insert_slice(timestamp=5, status=SliceStatus.DONE)

    expected = [
        Segment(start=5, end=5, status=SliceStatus.DONE),
        Segment(start=7, end=10, status=SliceStatus.PENDING),
    ]

    assert t.get_segments(0, 1) == []
    assert t.get_segments(0, 25) == expected


def test_mmm():
    t = create_random_timelord()

    t.insert_slice(timestamp=0, status=SliceStatus.PENDING)
    t.insert_slice(timestamp=3, status=SliceStatus.DONE)
    t.insert_slice(timestamp=4, status=SliceStatus.PENDING)
    t.insert_slice(timestamp=2, status=SliceStatus.DONE)
    t.insert_slice(timestamp=1, status=SliceStatus.DONE)

    expected = [
        Segment(start=0, end=0, status=SliceStatus.PENDING),
        Segment(start=1, end=3, status=SliceStatus.DONE),
        Segment(start=4, end=4, status=SliceStatus.PENDING),
    ]

    assert t.get_segments(0, 25) == expected

def test_mmm2():
    t = create_random_timelord()

    t.insert_slice(timestamp=1, status=SliceStatus.PENDING)
    t.insert_slice(timestamp=4, status=SliceStatus.PENDING)
    t.insert_slice(timestamp=0, status=SliceStatus.DONE)
    t.insert_slice(timestamp=2, status=SliceStatus.DONE)
    t.insert_slice(timestamp=3, status=SliceStatus.DONE)

    expected = [
        Segment(start=0, end=0, status=SliceStatus.DONE),
        Segment(start=1, end=1, status=SliceStatus.PENDING),
        Segment(start=2, end=3, status=SliceStatus.DONE),
        Segment(start=4, end=4, status=SliceStatus.PENDING),
    ]


    for s in t.get_segments(0, 25):
        print(s)

    assert t.get_segments(0, 25) == expected
