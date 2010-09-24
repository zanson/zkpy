'''
Created on 07.09.2010

@author: luk
'''

from zkpy import zk_retry_operation
from zkpy.connection import NodeCreationMode
import logging
import threading
import zookeeper

class Queue(object):
    '''Distributed concurrent zookeeper queue.'''

    logger = logging.getLogger('Queue')

    def __init__(self, connection, path):
        '''Sets up the queue.
        :param connection: The zkpy connection to use
        :param path: The parent path of the Queue. Needs to exist!

        Note: Queue nodes will have the same acl as the provided queue node.

        '''
        self.zk_conn = connection
        self.path = path

        try:
            _stat, self.node_acl = self.zk_conn.get_acl(path)
        except zookeeper.NoNodeException:
            raise RuntimeError('Path %s does not exists.' % self.path)



    @zk_retry_operation
    def push(self, data):
        '''Push an item to the end of the queue.

        :return: True, if queue could be added.

        '''
        self.zk_conn.create('%s/item-' % self.path,
                            data,
                            self.node_acl,
                            NodeCreationMode.PersistentSequential)
        return True

    @zk_retry_operation
    def pop(self):
        '''Pops one item from the head of the queue.
        Raises an IndexError if there is no item in the queue
        '''
        # get queue items
        items = self.zk_conn.get_children(self.path)
        items.sort()

        # try all items
        for item in items:
            try:
                item_path = '%s/%s' % (self.path, item)
                # try to get this item and delete it
                return self._remove(item_path)
            except zookeeper.NoNodeException:
                # another consumer already popped this item. let's just move on
                pass

        # all items were consumed by another consumer...
        raise IndexError('pop from empty list')

    @zk_retry_operation
    def _remove(self, item_path):
        '''Removes an item and returns its data.

        May throw a NoNodeException.
        '''
        while True:
            try:
                data, stat = self.zk_conn.get(item_path)
                self.zk_conn.delete(item_path, stat['version'])
                return data
            except zookeeper.BadVersionException:
                self.logger.warn('Queue item "%s" was modified. This should not be done.' % item_path)
                continue


    def pop_blocking(self, timeout=None):
        '''Pops from the head of the queue. Blocks until there is at least
        one element.

        '''

        # set up a queue watcher.
        condition = threading.Event()
        def watcher(handle, event, type, path):
            condition.set()

        # set watcher
        self.zk_conn.exists(self.path, watcher)

        while True:
            try:
                return self.pop()
            except IndexError:
                # queue was empty. wait that something changes...
                condition.wait(timeout)
                # check for timeout
                if not condition.set():
                    raise RuntimeError('pop_blocking timed out')
                # reset condition. in case a node was added and removed..
                condition.clear()


    def __len__(self):
        '''Returns the number of items in the queue.'''
        return len(self.items())

    def is_empty(self):
        '''Returns True, if no items are in the queue'''
        return len(self) == 0





