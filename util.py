from multiprocessing.pool import Pool

import numpy as np
import redis
from hdfs import InsecureClient


class Constants:
    TYPE_TRIAN = 0
    TYPE_PREDICT = 1

    TASK_RUNNING = 0
    TASK_FINISHED = 1
    TASK_ERROR = 2

    REDIS_HOST = '127.0.0.1'
    REDIS_PORT = 6379

    HDFS_HOST = '127.0.0.1'
    HDFS_PORT = 9870
    HDFS_USER = 'conch'


class RedisPool:
    def __init__(self):
        self.redis_pool = redis.ConnectionPool(host=Constants.REDIS_HOST,
                                               port=Constants.REDIS_PORT,
                                               decode_responses=True)

    def incr(self, name: str) -> int:
        return redis.StrictRedis(connection_pool=self.redis_pool).incr(name)

    def get(self, name: str) -> str:
        return redis.StrictRedis(connection_pool=self.redis_pool).get(name)

    def set(self, name: str, value: str):
        redis.StrictRedis(connection_pool=self.redis_pool).set(name, value)


class HdfsFile:
    '''HDFS File Object

    Keyword arguments:
        path -- HDFS file path
    '''

    def __init__(self, path: str):
        self.path = path
        self.name = path.split('\\')[-1]
        self.content = None
        self.fptr = 0
        self.client = InsecureClient(url=f'http://{Constants.HDFS_HOST}:{Constants.HDFS_PORT}',
                                     user=Constants.HDFS_USER)

    # iterable compatible
    def __iter__(self):
        return self

    # iterator compatible
    def __next__(self):
        buffer = self.readline()
        if buffer == '':
            raise StopIteration()
        return buffer

    # cache content in memeory before reading
    def __cache_content(self):
        if self.content is None:
            with self.client.read(self.path) as reader:
                self.content = reader.read()

    def close(self):
        self.flush()

    def exists(self) -> bool:
        if self.client.status(hdfs_path=self.path, strict=False) is None:
            return False
        return True

    def flush(self):
        if self.exists():
            self.client.write(hdfs_path=self.path,
                              data=self.content, overwrite=True)
        else:
            self.client.write(hdfs_path=self.path, data=self.content)

    def read(self, size: int=None):
        self.__cache_content()

        if size is None:
            return self.content
        else:
            offset = size
            buffer = self.content[self.fptr: self.fptr + offset]
            self.fptr += offset
            return buffer

    def readline(self, size: int=None):
        self.__cache_content()

        offset = 0
        while self.fptr + offset < len(self.content) and self.content[self.fptr + offset] not in [10, '\n']:
            offset += 1

        if offset == 0:
            return ''
        else:
            buffer = self.content[self.fptr: self.fptr + offset + 1]
            self.fptr += offset + 1
            return buffer

    def seek(self, cookie: int):
        self.fptr = cookie

    def write(self, text) -> int:
        if self.content is None:
            self.content = text
        else:
            self.content += text
        self.flush()
        print(self.content)
        return len(text)


def context(task_id: str, alg_func, args: set, data_path: str, model_path: str, result_path: str):
    redis_pool = RedisPool()
    try:
        redis_pool.set(task_id, str(Constants.TASK_RUNNING))
        alg_func(args, HdfsFile(data_path), HdfsFile(
            model_path), HdfsFile(result_path))
        redis_pool.set(task_id, str(Constants.TASK_FINISHED))
    except Exception:
        redis_pool.set(task_id, str(Constants.TASK_ERROR))
