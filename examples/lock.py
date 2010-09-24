#!/usr/bin/env python2.6
'''Example to demonstrate how to use the write lock.'''

from __future__ import with_statement
from zkpy.acl import Acls
from zkpy.connection import Connection
from zkpy.lock import Lock

class LockObserver(object):
    '''Observer class to get lock event notifications'''

    def __init__(self, name):
        ''':param name: Name of the observer'''
        self.name = name

    def lock_acquired(self):
        '''Method, which gets called, when the lock is acquired.'''
        print self.name, 'acquired'

    def lock_released(self):
        '''Method, which gets called, when the lock is released'''
        print self.name, 'released'

ZOOKEEPER_SERVER='localhost:2181'
def main():
    # As a lock is per connection, we need to create a connection
    # per lock.
    conn1 = Connection(ZOOKEEPER_SERVER, 3)
    conn2 = Connection(ZOOKEEPER_SERVER, 3)

    # ensure parent path for the lock exists
    lock_node = '/locktest'
    conn1.ensure_path_exists(lock_node, '', [Acls.Unsafe])

    # create the lock observers
    lock_observer1 = LockObserver('lock1')
    lock_observer2 = LockObserver('lock2')

    # create the locks
    lock1 = Lock(conn1, lock_node, lock_observer1)
    lock2 = Lock(conn2, lock_node, lock_observer2)

    # try to acquire the lock
    lock1.acquire()
    lock2.acquire()

    # now release it
    lock1.release()
    lock2.release()

    # clean up
    conn1.delete(lock_node)
    conn1.close()
    conn2.close()


if __name__ == '__main__':
    main()

