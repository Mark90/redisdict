import time
import multiprocessing

from redis import StrictRedis

from redis_dict import RedisDict

NAMESPACE_TEMPLATE = 'concurrent_test_{}'
MAX_POOL_SIZE = 8

redis_config = {
    'host': 'localhost',
    'port': 6379,
    'db': 0,
}

redisdb = StrictRedis(**redis_config)

# Prepare RedisDict instances
RD_INSTANCES = {process_id: RedisDict(namespace=NAMESPACE_TEMPLATE.format(process_id), **redis_config) for process_id in
                range(MAX_POOL_SIZE)}


def set_and_delete(rd, test_data, test_repetitions):
    inserted = deleted = 0
    error = ''
    try:
        for loop in range(test_repetitions):
            for key, value in test_data.iteritems():
                rd[key] = value
                inserted += 1

            for key, value in test_data.iteritems():
                del rd[key]
                deleted += 1
    except Exception as e:
        error = str(e)
    return inserted, deleted, error


def chain_set_and_delete(rd, test_data, test_repetitions):
    inserted = deleted = 0
    error = ''
    try:
        for loop in range(test_repetitions):
            for key, value in test_data.iteritems():
                rd.chain_set([key, value['chain_key']], value['chain_value'])
                inserted += 1

            for key, value in test_data.iteritems():
                rd.chain_del([key, value['chain_key']])
                deleted += 1
    except Exception as e:
        error = str(e)
    return inserted, deleted, error


FUNCTIONS = {
    'set_and_delete': set_and_delete,
    'chain_set_and_delete': chain_set_and_delete,
}

TEST_DATA = {
    'set_and_delete': lambda test_size: {'key_{}'.format(i): 'value_{}'.format(i) for i in range(test_size)},
    'chain_set_and_delete': lambda test_size: {
        'key_{}'.format(i): {
            'chain_key': 'subkey_{}'.format(i), 'chain_value': 'value_{}'.format(i)
        } for i in range(test_size)
    },
}


def namespace_has_keys():
    return any(redisdb.scan_iter(NAMESPACE_TEMPLATE.format('*')))


def get_redis_key_count():
    """Gets the number of keys in redis"""
    return sum(1 for _ in redisdb.keys())


def run_test(process_id, test, test_size, test_repetitions):
    """Function that runs the specified test in each process"""
    rd = RD_INSTANCES[process_id]
    test_data = TEST_DATA[test](test_size)
    func = FUNCTIONS[test]

    t1 = time.time()
    inserted, deleted, error = func(rd, test_data, test_repetitions)
    t2 = time.time()
    return {'process': process_id, 'inserted': inserted, 'deleted': deleted, 'error': error, 'time': (t2 - t1)}


def run_tests(test, processes, test_size=10000, test_repetitions=1):
    """Takes a testname and number of processes as arguments. Runs the test in parallel on said number of processes,
    where each process uses a unique Redis Namespace"""
    if namespace_has_keys():
        raise Exception(
            'Namespace {} contains keys before starting test, please check'.format(NAMESPACE_TEMPLATE.format('*')))

    print 'Run test {} with {} processes, test size {} and {} repetitions'.format(test, processes, test_size,
                                                                                  test_repetitions)

    number_keys_before = get_redis_key_count()
    pool = multiprocessing.Pool(processes)
    results = [pool.apply_async(run_test, args=(process_id, test, test_size, test_repetitions)) for process_id in
               range(processes)]
    pool.close()
    pool.join()

    # Validate everything is still fine
    if namespace_has_keys():
        raise Exception('Namespace {} contains keys after completing test {}, please check'.format(
            NAMESPACE_TEMPLATE.format('*'), test))

    number_keys_after = get_redis_key_count()
    if number_keys_before != number_keys_after:
        raise Exception(
            'Redis contained {} keys before the test and {} after..'.format(number_keys_before, number_keys_after))

    print 'Results for {}:'.format(test)
    for r in results:
        print '\t', r.get()


if __name__ == '__main__':
    run_tests('set_and_delete', 2)
    run_tests('chain_set_and_delete', 2)
    run_tests('set_and_delete', 3)
    run_tests('chain_set_and_delete', 3)
    run_tests('set_and_delete', 8, test_size=50, test_repetitions=500)
    run_tests('chain_set_and_delete', 8, test_size=50, test_repetitions=500)
    run_tests('set_and_delete', 4, test_size=10, test_repetitions=1000)
    run_tests('chain_set_and_delete', 4, test_size=10, test_repetitions=1000)
    run_tests('set_and_delete', 4, test_size=1024, test_repetitions=64)
    run_tests('set_and_delete', 4, test_size=1024, test_repetitions=64)
    # TODO: think of a test that concurrently writes to the same namespace
