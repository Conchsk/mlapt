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
    process_pool = None

    @staticmethod
    def apply(alg_func, data_path: str, model_dir: str, result_dir: str) -> str:
        task_id = str(ProcessPool.get_next_task_id())
        ProcessPool.process_pool.apply_async(alg_func, args=(DataAdapter.csv2array(data_path), model_dir, result_dir,
                                                             task_id, ProcessPool.running_handler, ProcessPool.finished_handler, ProcessPool.error_handler))
        return task_id

    @staticmethod
    def get_next_task_id():
        return RedisPool.get_conn().incr('task_id')

    @staticmethod
    def running_handler(task_id):
        RedisPool.get_conn().set(task_id, Constants.TASK_RUNNING)

    @staticmethod
    def finished_handler(task_id):
        RedisPool.get_conn().set(task_id, Constants.TASK_FINISHED)

    @staticmethod
    def error_handler(task_id):
        RedisPool.get_conn().set(task_id, Constants.TASK_ERROR)


class RedisPool:
    redis_pool = None

    @staticmethod
    def get_conn() -> redis.Redis:
        return redis.Redis(connection_pool=RedisPool.redis_pool)
