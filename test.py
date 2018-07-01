import util
from multiprocessing.pool import Pool
import time


def test_func(a, b, c, d, e, f, g):
    print('1')


if __name__ == '__main__':
    util.ProcessPool.process_pool = Pool()
    util.ProcessPool.apply(test_func, '1', '2', '3')
    time.sleep(5)
