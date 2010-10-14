# -*- coding: utf-8 -*-
'''
Created on 19.08.2010

@author: luk
'''

from functools import wraps
from zkpy import zk_retry_operation
from zkpy.utils import enum
import logging
import threading
import zookeeper


# make zookeeper a bit less verbose
zookeeper.set_debug_level(zookeeper.LOG_LEVEL_WARN)
# set up our logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.WARN)

class Connection(object):
    '''Represents a zookeeper connection'''


    logger = logger #logging.getLogger('zookeeper.connection')
    __wrapped_functions = set([
        # 'add_auth',#overwritten
        'client_id',
        'close',
        'create',
        'delete',
        'set',
        'set2',
        'get_acl',
        'set_acl',
        'is_unrecoverable',
        'recv_timeout', # property
        'set_watcher',
        'state',
        #'deterministic_conn_order', # overwritten, does not need a handle
        #### with watchers
        'exists',
        'get',
        'get_children',
        #init #forbidden -> handled in Connection construction
            ])


    def __init__(self, servers, timeout):
        '''Creates a new Connection object.

        :param servers: either a python list or a comma (',')
                        sepparated list of  zookeper servers
        :param timeout: timeout in seconds after connection initialisation fails
        '''

        # set up members
        if isinstance(servers, basestring):
            self._servers = [server.strip() for server in servers.split(',')]
        else:
            self._servers = servers
        self._handle = None
        self._timeout = timeout

        # connect
        self.connect(self._timeout)

        # set up watch queu
        self._watchers = set()


    def __del__(self):
        '''Makes sure, that the connection is not left open'''
        logger.debug('ConnectionWatcher: __del__')
        if self._handle and zookeeper.state(self._handle) == zookeeper.CONNECTED_STATE:
            self.logger.warn('Closing open zookeeper connection')
            self.close()


    def __global_watch(self, handle, type, state, path):
        '''Private callback for connection changes.'''

        self.logger.debug(
            'handle=%d type=%s state=%s path=%s' % (handle, type, state, path))

        if self._handle != handle:
            raise RuntimeError('Inconsistend handles!')

        # copy list: watchers might remove themselve during this call...
        for watcher in list(self._watchers):
            watcher(type, state, path)

        #TODO: handle expiration

    def __getattr__(self, call):
        '''
        Wraps zookeeper calls and automagically provide the original zookeeper
        methods with the its handle.
        e.g. zookeeper.state(handle) -> conn.state()

        '''
        if call not in self.__wrapped_functions:
            logger.warn('unknown property/method %s' % call)
#            raise AttributeError

        # try to the the wrapped call from the zookeeper package
        wrapped = getattr(zookeeper, call)

        # if it is not a method/function
        if not hasattr(wrapped, '__call__'):
            raise AttributeError

        # create and return a wrapper function (http://gael-varoquaux.info/blog/?p=120)s
        @wraps(wrapped)
        def wrapper(*args, **kwargs):
            #logger.debug('calling %s(%s, %s)' % (call, ', '.join(str(arg) for arg in args), ', '.join('%s=%s' % (k,v) for k,v in kwargs.items())))
            return wrapped(self._handle, *args, **kwargs)

        return wrapper


    def set_watcher(self, watcher):
        '''Overwrite zookeeper.set_watcher method and forwards to
        add_global_watcher
        '''
        self.add_global_watcher(watcher)


    def add_global_watcher(self, watcher):
        '''Adds a watcher to  global events'''
        self._watchers.add(watcher)

    def remove_global_watcher(self, watcher):
        '''Removes a formerly added watcher.
        Does nothing, if the watcher is not in the pool
        '''
        try:
            self._watchers.remove(watcher)
        except KeyError:
            self.logger.warn('remove_global_watcher: %s is not in the oberserver pool' % watcher)

    def recv_timeout(self):
        '''Returns zookeeper's recv timout in seconds.'''
        return zookeeper.recv_timeout(self._handle) / 1000.

    def add_auth(self, scheme, credentials):
        '''Specifies the connection credentials
        Overwrites zookeeper.add_auth() which is asynchronous
        '''

        class AuthWatch(object):
            def __init__(self, condition):
                self.condition = condition
                self.has_fired = False

            def __call__(self,handle, auth_result):
                logger.debug('fired %d' % auth_result)
                self.has_fired = True
                if auth_result == zookeeper.OK:
                    self.condition.set()
                    logger.debug('Auth added')
                else:
                    logger.warn('Could not set authentication')

        # create condition
        condition = threading.Event()
        auth_watch = AuthWatch(condition)

        # call method
        logger.debug('Adding auth')
        zookeeper.add_auth(self._handle, scheme, credentials, auth_watch)

        # wait for completion
        wait_time = self.recv_timeout()
        logger.debug('waiting for %f seconds' % wait_time)
        condition.wait(wait_time)
        logger.debug('finished waiting')
        if not auth_watch.has_fired:
            logger.warn('Zookeeper server did not respond within %.2f seconds' % wait_time)

        # return result
        return condition.isSet()


    def is_connected(self):
        ''' Returns True, if the connection is in the Connection state.'''
        try:
            return self.state() == KeeperState.Connected
        except:
            return False

    def is_somehow_connected(self):
        '''Returns True, if the connection is in the Connection or
        Connecting state.
        '''
        state = self.state()
        return ((state == KeeperState.Connected)
                or (state == KeeperState.Connecting))

    def connect(self, timeout = None):
        '''Connects to the zookeeper server'''
        # if no timeout provided, thake the configured one
        if timeout is None:
            timeout = self._timeout

        # check, if we're already connected
        if self._handle is not None and not self.is_connected():
            raise RuntimeError('Already connected')
            return

        condition = threading.Condition()
        def connection_watch(handle, type, state, path):
            condition.acquire()
            condition.notify()
            condition.release()

        # try to connect
        condition.acquire()
        self._handle = zookeeper.init(
			','.join(self._servers),
            connection_watch,
            self._timeout * 1000)
        condition.wait(self._timeout)
        condition.release()

        if zookeeper.state(self._handle) != zookeeper.CONNECTED_STATE:
            raise RuntimeError(
                'unable to connect to %s (%d)' % (' or '.join(self._servers), zookeeper.state(self._handle)))
        zookeeper.set_watcher(self._handle, self.__global_watch)


    def close(self):
        '''Overwrites the zookeeper.close() method'''


        if self._handle is None:
            logger.error('Can not close an uninitialized connection.')
            return

        logger.debug('closing connection')

        try:
            _state = zookeeper.state(self._handle)
        except zookeeper.ZooKeeperException:
            logger.warn('Connection is already closed')
            return True

        for _ in range(3):
            try:
                return zookeeper.close(self._handle) == zookeeper.OK
            except: #zookeeper.ConnectionLossException:
                logger.info('Got exception while closing. Retrying...')
        logger.error('Failed closing the zookeeper connection')
        return False

    @zk_retry_operation
    def ensure_path_exists(self, path, data, acl, recursive = False):
        '''Checks, if a given path exists. If this is not the case, the path is
        created with the provided data and ACL, otherwise, the data and acl
        arguments are ignored.
        The operation will be retried in case of a ConnectionLossException
        Throws an exception, if this was not possible.
        :param conn: Connection object
        :param path: Path to check/create
        :param data: Eventual Znode data
        :param acl: Eventual Znode access control list
        :param recursive: If set to 'True' the whole path is created.
                          NOTE: Each node on the path is created with the provided
                          ACL, but with empty data.

        '''

        # check, if path exists
        stat = self.exists(path)
        if stat:
            return True
        else:
            # if recursie: ensure existance of parent node
            if recursive:
                parent_path = path[:path.rfind('/')]
                # if parent_path is not empty (the root): try to create it
                if parent_path:
                    self.ensure_path_exists(parent_path, '', acl, recursive)

            # create this node
            self.create(path, data, acl, NodeCreationMode.Persistent)
            return True




class zkopen(object):
    '''Provides functionalities for python's (>= 2.6) with statement.
    I.e.

    with open('localhost:2181", 10) as conn:
        print conn.state()
    '''
    def __init__(self, servers, timeout):
        '''Creates the open object.
        :param servers: either a coma separated list of zookeeper servers,
                        or a list of servers.
        :param timeout: timeout for connecting to the server (seconds)
        '''
        self.servers = servers
        self.timeout = timeout
        self.connection = None

    def __enter__(self):
        self.connection = Connection(self.servers, self.timeout)
        return self.connection

    def __exit__(self, type, value, traceback):
        if self.connection:
            self.connection.close()
            del self.connection

# Event types
EventType = enum(
            NotWatchingAnymore  = -2, # zookeeper.NOTWATCHING_EVENT
            NoneType            = -1, # zookeeper.SESSION_EVENT
            NodeCreated         =  1, # zookeeper.CREATED_EVENT
            NodeDeleted         =  2, # zookeeper.DELETED_EVENT
            NodeDataChanged     =  3, # zookeeper.CHANGED_EVENT
            NodeChildrenChanged =  4) # zookeeper.CHILD_EVENT

# Connection states/events
KeeperState = enum(
        Disconnected    = 0,     # not a watcher event, but is returned for state() if disconnected
        Connecting      = 1,     # zookeeper.CONNECTING_STATE
        Associating     = 2,     # zookeeper.ASSOCIATING_STATE
        Connected       = 3,     # zookeeper.CONNECTED_STATE
        AuthFailed      = -113,  # zookeeper.Auth_FAILED_STATE
        Expired         = -112)  # zookeeper.EXPIRED_SESSION_STATE

# Node creation modes
NodeCreationMode = enum(
    Persistent           = 0,     # not explicitly defined in zookeeper
    Ephemeral            = 1,     # zookeeper.EPHEMERAL

    Sequential           = 2,     # zookeeper.SEQUENCE
    PersistentSequential = 2,     # Persistent + Sequential
    EphemeralSequential  = 3      # Ephemeral + Sequential
)


def main():
    pass

if __name__ == '__main__':
    main()
