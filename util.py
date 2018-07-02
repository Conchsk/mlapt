from multiprocessing.pool import Pool

import numpy as np
import redis


class Constants:
    TYPE_TRIAN = 0
    TYPE_PREDICT = 1

    TASK_RUNNING = 0
    TASK_FINISHED = 1
    TASK_ERROR = 2


class DataAdapter:
    @staticmethod
    def csv2array(path: str) -> np.ndarray:
        return np.loadtxt(path, delimiter=',', skiprows=1)


class ProcessPool:
    def __init__(self):
        self.process_pool = Pool()
        self.redis_pool = RedisPool()

    def apply(self, alg_func, data_path: str, model_dir: str, result_dir: str) -> str:
        task_id = str(self.redis_pool.incr('task_id'))
        self.process_pool.apply_async(alg_func, args=(DataAdapter.csv2array(data_path), model_dir, result_dir,
                                                      task_id,
                                                      ProcessPool.running_handler,
                                                      ProcessPool.finished_handler,
                                                      ProcessPool.error_handler))
        return task_id

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
            host='192.168.1.67', port=6379, decode_responses=True)

    def incr(self, name: str) -> int:
        return redis.StrictRedis(connection_pool=self.redis_pool).incr(name)

    def get(self, name: str) -> str:
        return redis.StrictRedis(connection_pool=self.redis_pool).get(name)

    def set(self, name: str, value: str):
        redis.StrictRedis(connection_pool=self.redis_pool).set(name, value)
