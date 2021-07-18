from redis import Redis
from enum import IntEnum
from pydantic import BaseModel
from .RedisLua import RedisLua
import json
import cloudpickle
import datetime

class SliceStatus(IntEnum):
    PENDING = 1
    DONE = 2

class Segment(BaseModel):
    start: int
    end: int
    status: SliceStatus

    @classmethod
    def deserialize(cls, serialized):
        deserilized = json.loads(serialized)
        return cls(**deserilized)

class TimeLord:
    def __init__(self, redis: Redis, key: str):
        self.redis = redis
        self.lua = RedisLua(redis)
        self.key = key

    def insert_slice(self, timestamp: int, status: SliceStatus):
        self.lua.eval('insert_slice', self.key, timestamp, int(status))

    def get_segments(self, start: int, end: int):
        segments = self.lua.eval('get_segments', self.key, start, end)
        return [Segment.deserialize(s) for s in segments]

    def _merge_to_segments(self, start: int, end: int):
        """
        FIXME: this is here just to benchmark it
        """

        self.lua.eval('merge_to_segments', self.key, start, end)
