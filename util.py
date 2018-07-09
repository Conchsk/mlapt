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


class DataAdapter:
    @staticmethod
    def csv2array(path: str) -> np.ndarray:
        return np.loadtxt(path, delimiter=',', skiprows=1)


class ProcessPool:
    def __init__(self):
        self.process_pool = Pool()

    def apply(self, alg_func, data_path: str, model_dir: str, result_dir: str) -> str:
        task_id = str(RedisPool().incr('task_id'))
        self.process_pool.apply_async(alg_func, args=(DataAdapter.csv2array(data_path), model_dir, result_dir,
                                                      task_id,
                                                      ProcessPool.running_handler,
                                                      ProcessPool.finished_handler,
                                                      ProcessPool.error_handler))
        return task_id

    def testfunc(self, alg_func, task_id):
        redis_pool = RedisPool()
        try:
            redis_pool.set(task_id, str(Constants.TASK_RUNNING))
            alg_func()
            redis_pool.set(task_id, str(Constants.TASK_FINISHED))
        except Exception:
            redis_pool.set(task_id, str(Constants.TASK_ERROR))

    @staticmethod
    def running_handler(task_id):
        RedisPool().set(task_id, str(Constants.TASK_RUNNING))

    @staticmethod
    def finished_handler(task_id):
        RedisPool().set(task_id, str(Constants.TASK_FINISHED))

    @staticmethod
    def error_handler(task_id):
        RedisPool().set(task_id, str(Constants.TASK_ERROR))


class RedisPool:
    def __init__(self):
        self.redis_pool = redis.ConnectionPool(
            host=Constants.REDIS_HOST, port=Constants.REDIS_PORT, decode_responses=True)

    def incr(self, name: str) -> int:
        return redis.StrictRedis(connection_pool=self.redis_pool).incr(name)

    def get(self, name: str) -> str:
        return redis.StrictRedis(connection_pool=self.redis_pool).get(name)

    def set(self, name: str, value: str):
        redis.StrictRedis(connection_pool=self.redis_pool).set(name, value)


class HdfsFile:
    def __init__(self, path):
        self.path = path
        self.name = path.split('\\')[-1]

        client = InsecureClient(
            url=f'http://{Constants.HDFS_HOST}:{Constants.HDFS_PORT}', user=Constants.HDFS_USER)
        with client.read(self.path) as reader:
            self.content = reader.read()
        self.fptr = 0

    def read(self, size=None):
        if size is None:
            return self.content
        else:
            offset = size
            buffer = self.content[self.fptr: self.fptr + offset]
            self.fptr += offset
            return buffer

    def readline(self, size=None):
        offset = 0
        while self.fptr + offset < len(self.content) and self.content[self.fptr + offset] != 10:
            offset += 1
        if offset == 0:
            return ''
        else:
            buffer = self.content[self.fptr: self.fptr + offset + 1]
            self.fptr += offset + 1
            return buffer

    def seek(self, cookie):
        self.fptr = cookie

    def write(self, content):
        client = InsecureClient(
            url=f'http://{Constants.HDFS_HOST}:{Constants.HDFS_PORT}', user=Constants.HDFS_USER)
        client.write(hdfs_path=self.path, data=content, overwrite=True)
