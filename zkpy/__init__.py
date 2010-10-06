from functools import wraps
import logging
import time
import zookeeper


logger = logging.getLogger(__name__)

class RetryOperationError(Exception):
    pass

def zk_retry_operation(operation,  retry_count = 10, retry_delay = 0.5):
    '''Retries a zk operation for several times.
    Can be used as a decorator (with default arguments)

    @zk_retry_operation
    def foo(arg1):
        print arg1

    Or like
    def runner(arg1)
        print arg1
    zk_retry_operation(runner, retry_count=10, retry_delay=2)('thearg')

    '''
    @wraps(operation)
    def wrapper(*args, **kwargs):
        for attempt_count in range(1, retry_count):
            try:
                return operation(*args, **kwargs)
            except zookeeper.SessionExpiredException:
                logger.error('''Zookeeper session expired. Please clean up your state and start a new session and retry.''')
                raise
            except zookeeper.ConnectionLossException:
                if attempt_count >= retry_count:
                    logger.error('Retried operation for %d times. Giving up' % attempt_count)
                    raise
                time.sleep(retry_delay)
        raise RetryOperationError('Could not execute %s. Retried for %d times' % (operation, retry_count))
    return wrapper

