import traceback
from multiprocessing.pool import Pool

import numpy as np
import redis


class RedisPool:
    def __init__(self):
        self.redis_pool = redis.ConnectionPool(host='127.0.0.1', port=6379, decode_responses=True)

    def incr(self, name: str) -> int:
        return redis.StrictRedis(connection_pool=self.redis_pool).incr(name)

    def get(self, name: str) -> str:
        return redis.StrictRedis(connection_pool=self.redis_pool).get(name)

    def set(self, name: str, value: str):
        redis.StrictRedis(connection_pool=self.redis_pool).set(name, value)


class ProcessPool:
    TASK_RUNNING = 'running'
    TASK_FINISHED = 'finished'
    TASK_ERROR = 'error'

    def __init__(self):
        self.pool = Pool()

    def apply(self, alg_func, args: set, data_path: str, model_path: str, result_path: str):
        task_id = str(RedisPool().incr('task_id'))
        self.pool.apply_async(ProcessPool.context, args=(task_id, alg_func, args,
                                                         data_path, model_path, result_path))
        return task_id

    @staticmethod
    def context(task_id: str, alg_func, args: set, data_path: str, model_path: str, result_path: str):
        redis_pool = RedisPool()
        try:
            redis_pool.set(task_id, ProcessPool.TASK_RUNNING)
            alg_func(args, data_path, model_path, result_path)
            redis_pool.set(task_id, ProcessPool.TASK_FINISHED)
        except Exception:
            redis_pool.set(task_id, ProcessPool.TASK_ERROR)
            print(traceback.format_exc())
