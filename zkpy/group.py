from socket import gethostname
from zkpy import zk_retry_operation
from zkpy.connection import EventType, NodeCreationMode
from zkpy.utils import enum
import logging
import zookeeper


GroupEvents = enum(
    ObserverError  = -1,
    MemberJoined   = 1,
    MemberLeft     = 2,
    MemberChanged  = 3
)

logging.warn('zkpy.group IS NOT MEANT TO BE USED RIGHT NOW!')

class Group(object):
    '''
    WIP. NOT useable at the moment.

    Multiple entries per host
    Host registers with hostname and port

    path: Path for the service group, needs to exists

    '''
    logger = logging.getLogger('zkpy.group.Group')

    def __init__(self, connection, path, observer = None):
        self.zk_conn = connection
        self.path = path
        self.observers = set()
        if observer:
            self.register_observer(observer)

        # register observer
        self.zk_conn.get_children(path, self._pool_watcher)

        _stat, self.node_acl = self.zk_conn.get_acl(path)


    def register_observer(self, watcher):
        '''Registers a watcher for this group.
        The watcher needs to implement two methods, namely
         - member_left(member_id)
         - member_joined(member_id)
         - member_changed(member_id)
        '''
        self.observers.add(watcher)

    def _pool_watcher(self, handle, event, state, path):

        self.logger.info('watcher cb called handle=%d type=%s state=%s path=%s' % (handle, EventType[event], state, path))

        # re add watcher
        self.zk_conn.get_children(self.path, self._pool_watcher)
        #self.logger.debug('watcher cb re-added: %s' % self.path)

        if event == EventType.NotWatchingAnymore:
            raise RuntimeError('lost watcher')
        elif event == EventType.NodeCreated:
            for watcher in self.observers:
                watcher.member_joined(self.path)
        elif event ==  EventType.NodeDeleted:
            for watcher in self.observers:
                watcher.member_left(self.path)
        elif event ==  EventType.NodeDataChanged:
            print path, 'has changed'
            for watcher in self.observers:
                watcher.member_changed(self.path)
        elif event ==  EventType.NodeChildrenChanged:
            self.logger.debug('children changed: %s' % self.path)

    def get_members(self):
        '''Returns all registered Group members.'''
        return self.zk_conn.get_children(self.path)


class GroupMember(Group):

    logger = logging.getLogger('zkpy.group.GroupMember')

    def __init__(self, connection, path, data):
        self.zk_conn = connection
        self.group = Group(connection, path)
        self.id = None

    def _group_watcher(self):
        pass

    @zk_retry_operation
    def join(self):
        '''Registers this member in the specified group.

        :returns id of the new group node.

        Note: Nodes are created with the same acl as the group node root
        '''
        node_path = '%s/member-' % self.group.path
        #self.logger.debug('create node')
        id = self.zk_conn.create(
                        node_path,
                        gethostname(),
                        self.group.node_acl,
                        NodeCreationMode.EphemeralSequential)
        #self.logger.debug('node created')
        self.id = id[len(self.group.path)+1:]

        return self.id

    @property
    def is_member(self):
        '''Returns True, if this object is associated to the group'''
        return self.zk_conn.is_connected() and self.id is not None

    def leave(self):
        '''Removes this member from the specified group'''

        try:
            self.zk_conn.delete('%s/%s' % (self.group.path, self.id))
        # we do not bother, if there is no such node
        except zookeeper.NoNodeException:
            self.logger.warn('No such node')
            return
        finally:
            self.id = None





def main():
    pass

if __name__ == '__main__':
    main()
