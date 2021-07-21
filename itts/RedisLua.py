#!/usr/bin/env python3

import hashlib
import pkg_resources


class RedisLua:
    def __init__(self, redisConnection):
        self.setRedisConnection(redisConnection)
        self.loadSource(self.getLuaPath())
        self.register()

    def getLuaPath(self):
        return pkg_resources.resource_filename('itts', 'islands_in_the_stream.lua')

    def file_get_contents(self, filename):
        with open(filename) as f:
            return f.read()

    def setRedisConnection(self, connection):
        self.connection = connection
        return True

    def loadSource(self, path):
        self.source = self.file_get_contents(path)
        return True

    def getSha1(self):
        d = hashlib.sha1(self.source.encode('utf-8'))
        d.digest()
        return d.hexdigest()

    def exists(self):
        # Check if the script exists
        t = self.connection.script_exists(self.sha1)

        if t and t[0]:
            return True

        return False

    def load(self):
        self.connection.script_load(self.source)

        return True

    def register(self):
        # Set LUA sha1
        self.sha1 = self.getSha1()

        # Load if needed
        if not self.exists():
            self.load()

        return True

    def eval(self, *args):
        return self.connection.evalsha(self.sha1, 0, *args)
