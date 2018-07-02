from multiprocessing.pool import Pool


class testclass:
    testvar = 0


def test_func():
    testclass.testvar = testclass.testvar + 1


if __name__ == '__main__':
    pool = Pool()
    for i in range(5):
        pool.apply_async(test_func)
    pool.close()
    pool.join()
    print(testclass.testvar)
