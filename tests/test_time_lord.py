import random
import string
from redis import Redis

from time_lord import TimeLord, SliceStatus, Segment

def create_random_timelord():
    random_key = ''.join(random.choices(string.ascii_uppercase + string.digits, k=32))
    return TimeLord(Redis(), f'tl::{random_key}')


def test_empty():
    t = create_random_timelord()

    assert t.get_segments(0, 1) == []

def test_with_one_slice():
    t = create_random_timelord()
    t.insert_slice(5, SliceStatus.PENDING)

    expected = [
        Segment(start=5, end=5, status=SliceStatus.PENDING),
    ]

    assert t.get_segments(0, 1) == []
    assert t.get_segments(0, 6) == expected

def test_inserting_another_slice_after():
    t = create_random_timelord()
    t.insert_slice(5, SliceStatus.PENDING)
    t.insert_slice(10, SliceStatus.PENDING)

    expected = [
        Segment(start=5, end=10, status=SliceStatus.PENDING),
    ]

    assert t.get_segments(0, 1) == []
    assert t.get_segments(0, 20) == expected

def test_inserting_another_slice_before():
    t = create_random_timelord()
    t.insert_slice(10, SliceStatus.PENDING)
    t.insert_slice(5, SliceStatus.PENDING)

    expected = [
        Segment(start=5, end=10, status=SliceStatus.PENDING),
    ]

    assert t.get_segments(0, 1) == []
    assert t.get_segments(0, 20) == expected

def test_inserting_slice_between_two_slices():
    t = create_random_timelord()
    t.insert_slice(10, SliceStatus.PENDING)
    t.insert_slice(5, SliceStatus.PENDING)
    t.insert_slice(7, SliceStatus.PENDING)

    expected = [
        Segment(start=5, end=10, status=SliceStatus.PENDING),
    ]

    assert t.get_segments(0, 1) == []
    assert t.get_segments(0, 20) == expected

def test_inserting_two_slices_of_different_statuses():
    t = create_random_timelord()
    t.insert_slice(5, SliceStatus.PENDING)
    t.insert_slice(10, SliceStatus.DONE)

    expected = [
        Segment(start=5, end=5, status=SliceStatus.PENDING),
        Segment(start=10, end=10, status=SliceStatus.DONE),
    ]

    assert t.get_segments(0, 1) == []
    assert t.get_segments(0, 20) == expected

def test_inserting_another_done_slice_at_the_end():
    t = create_random_timelord()
    t.insert_slice(5, SliceStatus.PENDING)
    t.insert_slice(6, SliceStatus.DONE)
    t.insert_slice(7, SliceStatus.PENDING)
    t.insert_slice(10, SliceStatus.DONE)
    t.insert_slice(15, SliceStatus.DONE)

    expected = [
        Segment(start=5, end=5, status=SliceStatus.PENDING),
        Segment(start=6, end=6, status=SliceStatus.DONE),
        Segment(start=7, end=7, status=SliceStatus.PENDING),
        Segment(start=10, end=15, status=SliceStatus.DONE),
    ]


    assert t.get_segments(0, 1) == []
    assert t.get_segments(0, 20) == expected

def test_inserting_another_pending_slice_at_the_end():
    t = create_random_timelord()
    t.insert_slice(5, SliceStatus.PENDING)
    t.insert_slice(6, SliceStatus.DONE)
    t.insert_slice(7, SliceStatus.PENDING)
    t.insert_slice(10, SliceStatus.DONE)
    t.insert_slice(15, SliceStatus.PENDING)

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
    t = create_random_timelord()
    t.insert_slice(5, SliceStatus.PENDING)
    t.insert_slice(10, SliceStatus.DONE)
    t.insert_slice(15, SliceStatus.PENDING)
    t.insert_slice(2, SliceStatus.PENDING)

    expected = [
        Segment(start=2, end=5, status=SliceStatus.PENDING),
        Segment(start=10, end=10, status=SliceStatus.DONE),
        Segment(start=15, end=15, status=SliceStatus.PENDING),
    ]

    assert t.get_segments(0, 1) == []
    assert t.get_segments(0, 20) == expected

def test_inserting_another_done_slice_at_the_begining():
    t = create_random_timelord()

    t.insert_slice(5, SliceStatus.PENDING)
    t.insert_slice(10, SliceStatus.DONE)
    t.insert_slice(12, SliceStatus.PENDING)
    t.insert_slice(15, SliceStatus.DONE)
    t.insert_slice(2, SliceStatus.DONE)

    expected = [
        Segment(start=2, end=2, status=SliceStatus.DONE),
        Segment(start=5, end=5, status=SliceStatus.PENDING),
        Segment(start=10, end=10, status=SliceStatus.DONE),
        Segment(start=12, end=12, status=SliceStatus.PENDING),
        Segment(start=15, end=15, status=SliceStatus.DONE),
    ]

    assert t.get_segments(0, 1) == []
    assert t.get_segments(0, 20) == expected

def test_inserting_same_status_same_timestamp_at_begining():
    t = create_random_timelord()

    t.insert_slice(10, SliceStatus.DONE)
    t.insert_slice(15, SliceStatus.DONE)
    t.insert_slice(17, SliceStatus.PENDING)
    t.insert_slice(10, SliceStatus.DONE)

    expected = [
        Segment(start=10, end=15, status=SliceStatus.DONE),
        Segment(start=17, end=17, status=SliceStatus.PENDING),
    ]

    assert t.get_segments(0, 1) == []
    assert t.get_segments(0, 20) == expected

def test_inserting_different_status_same_timestamp_at_begining():
    t = create_random_timelord()

    t.insert_slice(10, SliceStatus.DONE)
    t.insert_slice(15, SliceStatus.DONE)
    t.insert_slice(17, SliceStatus.PENDING)
    t.insert_slice(10, SliceStatus.PENDING)

    expected = [
        Segment(start=10, end=10, status=SliceStatus.PENDING),
        Segment(start=10, end=15, status=SliceStatus.DONE),
        Segment(start=17, end=17, status=SliceStatus.PENDING),
    ]

    assert t.get_segments(0, 1) == []
    assert t.get_segments(0, 20) == expected

def test_inserting_within_existing_slice_same_status():
    t = create_random_timelord()

    t.insert_slice(5, SliceStatus.PENDING)
    t.insert_slice(10, SliceStatus.PENDING)
    t.insert_slice(10, SliceStatus.DONE)
    t.insert_slice(15, SliceStatus.DONE)
    t.insert_slice(15, SliceStatus.PENDING)
    t.insert_slice(20, SliceStatus.PENDING)
    t.insert_slice(12, SliceStatus.DONE)

    expected = [
        Segment(start=5, end=10, status=SliceStatus.PENDING),
        Segment(start=10, end=15, status=SliceStatus.DONE),
        Segment(start=15, end=20, status=SliceStatus.PENDING),
    ]

    assert t.get_segments(0, 1) == []
    assert t.get_segments(0, 25) == expected

def test_inserting_within_existing_slice_different_status():
    t = create_random_timelord()

    t.insert_slice(5, SliceStatus.PENDING)
    t.insert_slice(10, SliceStatus.PENDING)
    t.insert_slice(10, SliceStatus.DONE)
    t.insert_slice(15, SliceStatus.DONE)
    t.insert_slice(15, SliceStatus.PENDING)
    t.insert_slice(20, SliceStatus.PENDING)
    t.insert_slice(12, SliceStatus.PENDING)

    expected = [
        Segment(start=5, end=10, status=SliceStatus.PENDING),
        Segment(start=10, end=12, status=SliceStatus.DONE),
        Segment(start=12, end=12, status=SliceStatus.PENDING),
        Segment(start=12, end=15, status=SliceStatus.DONE),
        Segment(start=15, end=20, status=SliceStatus.PENDING),
    ]

    assert t.get_segments(0, 1) == []
    assert t.get_segments(0, 25) == expected

def test_inserting_within_existing_slice_extending_it():
    t = create_random_timelord()

    t.insert_slice(5, SliceStatus.PENDING)
    t.insert_slice(10, SliceStatus.PENDING)
    t.insert_slice(10, SliceStatus.DONE)
    t.insert_slice(15, SliceStatus.DONE)
    t.insert_slice(15, SliceStatus.PENDING)
    t.insert_slice(20, SliceStatus.PENDING)
    t.insert_slice(12, SliceStatus.PENDING)
    t.insert_slice(13, SliceStatus.PENDING)

    expected = [
        Segment(start=5, end=10, status=SliceStatus.PENDING),
        Segment(start=10, end=12, status=SliceStatus.DONE),
        Segment(start=12, end=13, status=SliceStatus.PENDING),
        Segment(start=13, end=15, status=SliceStatus.DONE),
        Segment(start=15, end=20, status=SliceStatus.PENDING),
    ]

    assert t.get_segments(0, 1) == []
    assert t.get_segments(0, 25) == expected
