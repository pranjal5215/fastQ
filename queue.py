# -*- coding: utf-8 -*-

import redis
import uuid

class RedisQueue(object):

    """Simple Queue with Redis Backend"""
    def __init__(self, name, namespace='queue'):
        """
        The default connection parameters are: host='localhost', port=6379, db=0
        """
        self.redisDB = redis.Redis()
        self.key = '%s:%s' %(namespace, name)

    def qsize(self):
        """Return the approximate size of the queue."""
        return self.redisDB.llen(self.key)

    def empty(self):
        """Return True if the queue is empty, False otherwise."""
        return self.qsize() == 0

    def put(self, item):
        """Put item into the queue."""
        uid = str(uuid.uuid4())
        redis_data = {}
        redis_data['data'] = item
        redis_data['taskid'] = uid
        self.redisDB.rpush(self.key, redis_data)
        return uid

    def get(self, block=True, timeout=None):
        """Remove and return an item from the queue. 

        If optional args block is true and timeout is None (the default), block
        if necessary until an item is available."""
        if block:
            item = self.redisDB.brpop(self.key, timeout=timeout)
        else:
            item = self.redisDB.rpop(self.key)

        if item:
            item = item[1]
        return item

    def get_nowait(self):
        """Equivalent to get(False)."""
        return self.get(False)


class RedisDataStore():
    def __init__(self, name, namespace='hash'):
        """The default connection parameters are: host='localhost', port=6379, db=0"""
        self.redisDB= redis.Redis()
        self.key = '%s:%s' %(namespace, name)

    def get(self, map_key):
        value = self.redisDB.hget(self.key, map_key)
        return value
    
    def put(self, map_key, value):
        try:
            self.redisDB.hset(self.key, map_key, value)
            return True
        except Exception as e:
            return False

    def delete(self, map_key):
        self.redisDB.hdel(self.key, map_key)

