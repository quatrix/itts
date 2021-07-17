from redis import Redis
from enum import Enum
from pydantic import BaseModel, PrivateAttr
import cloudpickle
import datetime

class SliceStatus(Enum):
    PENDING = 1
    DONE = 2

class Segment(BaseModel):
    start: int
    end: int
    status: SliceStatus
    _pickled: bytes = PrivateAttr()

    @classmethod
    def from_pickled(cls, pickled_segment):
        unpickled = cloudpickle.loads(pickled_segment)

        segment = cls(**unpickled)
        segment._pickled = pickled_segment

        return segment

    def get_pickled(self):
        return self._pickled

def deserialize(pickled_segments):
    return [Segment.from_pickled(pickled) for pickled in pickled_segments]


class TimeLord:
    def __init__(self, redis: Redis, key: str):
        self.redis = redis
        self.key = key

    def get_surrounding_slices(self, timestamp):
        before = self.redis.zrangebyscore(self.key, '-inf', f'({timestamp}', start=0, num=-2)
        after = self.redis.zrangebyscore(self.key, timestamp, 'inf', start=0, num=2)

        return deserialize(before + after)

    def _store_slice(self, start: int, end: int, status: SliceStatus):
        new_slice = {
            'start': start,
            'end': end,
            'status': status,
        }

        item = {cloudpickle.dumps(new_slice): start}

        self.redis.zadd(self.key, item)

    def _remove_slice(self, single_slice : Segment):
        self.redis.zrem(self.key, single_slice.get_pickled())

    def _replace_slice(self, old_slice, start: int, end: int, status: SliceStatus):
        self._remove_slice(old_slice)
        self._store_slice(start=start, end=end, status=status)

    def insert_slice(self, timestamp: int, status: SliceStatus):
        """
        find the slice -inf -> timestamp
        find the slice timestamp -> inf

        if it's the same slice 

        """

        slices = self.get_surrounding_slices(timestamp)

        if not slices:
            self._store_slice(start=timestamp, end=timestamp, status=status)
            return

        if len(slices) == 1:
            single_slice = slices[0]

            if single_slice.status == status:
                self._replace_slice(
                    old_slice=single_slice,
                    start=min(single_slice.start, timestamp),
                    end=max(single_slice.end, timestamp),
                    status=status,
                )
            else:
                self._store_slice(start=timestamp, end=timestamp, status=status)

            return

        # if we have more than 1 slice, we need to find where to insert 
        # this new slice. 
        #
        # we iterate over slices and:
        # 1. first slice is already greater than timestamp, we add slice at begining
        # 2. we find a slice which timestamp is between its start and end
        # 3. we didn't find a matching slice, so we can end the slice at the end
        # 4. first or last slice is equal to timestamp 
        #
        # for 1 and 3 we just check:
        # - if the status is the same, extend it
        # - if the status is different, add a new slice
        #
        # for option 2
        # - if status is the same, don't change anything
        # - if status is different, then we need to split it!
        # 
        # for option 4
        # - if status is the same, do nothing
        # - if status is different, add a new slice
        #
        # splitting:
        # - o_{start,end_status} means the attributes from the found slice
        # - delete the slice
        # - create a [start=o_start, end=timestamp, status=o_status]
        #            [start=timestamp, end=timestamp, status=status]
        #            [start=timestamp, end=o_end, status=o_status]
        #

        first_slice, last_slice = slices[0], slices[-1]

        if first_slice.start == timestamp and first_slice.status != status:
            self._store_slice(start=timestamp, end=timestamp, status=status)
            return

        if last_slice.end == timestamp and last_slice.status != status:
            self._store_slice(start=timestamp, end=timestamp, status=status)
            return

        if first_slice.start > timestamp:
            if first_slice.status == status:
                self._replace_slice(
                    old_slice=first_slice,
                    start=timestamp,
                    end=first_slice.end,
                    status=status,
                )
            else:
                self._store_slice(start=timestamp, end=timestamp, status=status)

            return

        if timestamp > last_slice.end:
            if last_slice.status == status:
                self._replace_slice(
                    old_slice=last_slice,
                    start=last_slice.start,
                    end=timestamp,
                    status=status,
                )
            else:
                self._store_slice(start=timestamp, end=timestamp, status=status)

            return

        for single_slice in slices:
            if single_slice.start <= timestamp and single_slice.end >= timestamp:
                if single_slice.status != status:
                    self._remove_slice(single_slice)
                    self._store_slice(start=single_slice.start, end=timestamp, status=single_slice.status)
                    self._store_slice(start=timestamp, end=timestamp, status=status)
                    self._store_slice(start=timestamp, end=single_slice.end, status=single_slice.status)

                return


    def get_segments(self, start: int, end: int):
        """Returns segments between `start` and `end`

        Returns slices clustered by consecutive status
        For example if we have the following slices:

            [pending][pending][pending][done][done][pending]

        It's segmenetd as: [pending][done][pending]
        Where each segment stores the first and last timestamp of its slices
        """

        return deserialize(self.redis.zrangebyscore(self.key, start, end))
