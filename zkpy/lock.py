'''
Created on 20.08.2010

@author: luk
'''

from zkpy import zk_retry_operation
from zkpy.connection import KeeperState, NodeCreationMode, EventType
import logging
import operator
import zookeeper


logger = logging.getLogger(__name__)

class Lock(object):
    '''Distributed lock.
    Implements a distributed lock per connection (i.e. creating a lock object
    on the same connection object creates/retrieves the same Zookeeper lock
    node.

    '''

    def __init__(self, connection, path, watcher = None):
        '''Lock construction.
        :param connection:  The zkpy connection
        :param path: Parent node under which the lock nodes are created.
                     Needs to exist. Note: children will have the same ACL as
                     this node
        :param acls: Access control list for the Lock
        :param watcher: Lock watcher object. Needs to implement a lock_acquired()
                        and lock_released() method.
        '''
        # provided properties
        self._connection = connection
        self._connection.add_global_watcher(self._connection_watcher)
        self._path = path
        self.watcher = watcher

        self._id = None
        self._last_owner = None
        self._watched_neighbor = None

        try:
            _stat, self._acls = self._connection.get_acl(path)
        except zookeeper.NoNodeException:
            raise RuntimeError('Node %s needs to exist.' % self._path)


    def __del__(self):
        #release lock, if is aquired
        self.release()

    @property
    def path(self):
        return self._path

    @property
    def id(self):
        return self._id

    def _id_to_node_prefix(self, id):
        return 'lock-%s-' % str(id)

    def _connection_watcher(self, type, state, path):
        '''Receives global connection events.'''

        # check new connection
        if state == KeeperState.Expired:
            logger.warning('Connection expired! Lock \'%s\' is released.' % self._id)
            self._id = None
            if self.watcher:
                self.watcher.lock_released()
            #TODO: handle! raise?
        elif state == KeeperState.Connecting:
            logger.warning('Watcher: Connecting state!')
        else:
            logger.debug('Watcher: Lock \'%s\' catched connection event \'%s\'' %  (self._id, KeeperState[state]))
            self._lock()



    def _get_or_create_lock_node(self, prefix):
        '''Gets or creates the node with the given node name prefix.
        Returns the full node name (without the path)
        '''
        # get all children
        children = self._connection.get_children(self._path)

        # search it, in the children list
        for child in children:
            if child.startswith(prefix):
                logger.debug('Found already existing node %s' % child)
                return child

        # if not found: create the node
        node = self._connection.create('%s/%s' % (self._path, prefix),
                                '',
                                self._acls,
                                NodeCreationMode.EphemeralSequential)
        # strip the path
        node_id = node[len(self._path) + 1:]
        logger.debug('Created node %s' % node)
        return node_id

    def __smaller_neighbor_watcher(self, handle, type, state, path):
        if self._id and path[len(self._path)+1:] == self._watched_neighbor:
            logger.debug('Watcher fired on path: %s state: %s type: %s. Tryin to acquire the lock' % (path, EventType[type], KeeperState[state]))
            self._lock()


    def acquire(self):

        # check, if we need to lock ourself?
        if self.is_owner():
            return True

        return self._lock()


    @zk_retry_operation
    def _lock(self):
        '''Implementation of the node locking.'''
        max_retry_count = 10;

        former_lock_owner = self._last_owner

        # while the lock was not acquired or we could not set a watcher
        for _retry_count in range(max_retry_count):
            # create our node if needed or recover from old session
            if not self._id:
                session_id, _data = self._connection.client_id()
                node_name_prefix = self._id_to_node_prefix(session_id)
                self._id = self._get_or_create_lock_node(node_name_prefix)

            # get children and store them as a list of (seq_id, name)
            children = [ (int(child[child.rfind('-')+1:]), child)
                          for child in self._connection.get_children(self._path)]
            if not children:
                # this case should not happen, as we just added ourself
                logger.warn('No children in %s but there should be!' % self._path)
                self._id = None
                continue

            # sort them by the sequence id. nodeformat: <path>/lock-<session-id>-<sequence number>
            children.sort(key=operator.itemgetter(0), reverse=False)
            self._last_owner = children[0][1]

            # search next smaller neighbor
            me_not_found = True
            smaller_neighbor = None
            for _seq_id, name in children:
                # found our position -> stop
                if name == self._id:
                    me_not_found = False
                    break
                smaller_neighbor = name
            if me_not_found:
                logger.warn('Could not find own lock node \'%s\'. Recreating...' % self._id)
                self._id = None
                continue

            # if there is a smaller neighbor: we watch him
            if smaller_neighbor:
                # only set a watch, if the smaller id has changed
                if smaller_neighbor != self._watched_neighbor:
                    logger.debug('watching less than me node: %s' % smaller_neighbor)
                    stat = self._connection.exists(
                                        '%s/%s' % (self._path, smaller_neighbor),
                                         self.__smaller_neighbor_watcher)

                    # we could not get the stat: smaller neighbor does not exist
                    # anymore
                    if not stat:
                        logger.debug('can not watch lesser node %s. Retrying...' % smaller_neighbor)
                        continue

                    self._watched_neighbor = smaller_neighbor

                # return, that we did not acquire the lock
                return False

            # there is no smaller neighbor
            else:
                if self.is_owner():
                    if self.watcher and former_lock_owner != self._id:
                        self.watcher.lock_acquired()
                    return True
                logger.debug('we should be owner, but we arent!')
                continue

        raise RuntimeError('Could neither acquire the lock, nor set a watch')

    def is_owner(self):
        '''Returns true, if this instance holds the lock'''
        return (self._connection.is_somehow_connected()
                and self._id
                and self._last_owner
                and self._id == self._last_owner)

    def release(self):
        '''Releases the lock'''

        if not self._id:
            logger.warn('Can not release a not acquired lock')
            return
        if not self._connection.is_connected():
            logger.info('No connection. Lock is already released')
            return

        # set us to released
        node_id = self._id
        self._id = None

        # we don't need to retry this operation in the case of failure
        # as ZK will remove ephemeral files and we don't want to hang
        # this process when closing if we cannot reconnect to ZK
        try:
            self._connection.delete('%s/%s' % (self._path, node_id))

        # we do not bother, if there is no such node
        except zookeeper.NoNodeException:
            logger.warn('No such node')
            return
        finally:
            if self.watcher:
                self.watcher.lock_released()


def main():
    pass

if __name__ == '__main__':
    main()

