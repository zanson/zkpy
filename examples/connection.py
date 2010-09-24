#!/usr/bin/env python
'''Demonstrates how to establish a connection to the zookeeper
service.
'''
from __future__ import with_statement
from zkpy.acl import Acls, IdSchema
from zkpy.connection import zkopen, KeeperState, EventType
import zookeeper

ZOOKEEPER_HOST='localhost:2181'

def main():

    # define a watcher function
    def global_watcher(type, state, path):
        print 'GLOBAL: event=%s, state=%s, path=%s' % (EventType[type], KeeperState[state], path)

    # create a connection
    with zkopen(ZOOKEEPER_HOST, 5) as conn:
        # add a connection watcher for fun
        conn.add_global_watcher(global_watcher)
        # add an authentication
        print 'Setting authentication info: ', conn.add_auth(IdSchema.Digest, 'the_user:pass') # Acls.Creater needs an auth first
        # create a node with whose children are world readable, but only
        # the creator ('the_user') can create/write to/read from them.
        try:
            print 'Creating /foo:', conn.create('/foo','', [Acls.Creator, Acls.Readonly])
        except zookeeper.NodeExistsException:
            print '/foo already exists'
        try:
            print 'Creating /foo/nested:', conn.create('/foo/nested','', [Acls.Creator])
        except zookeeper.NodeExistsException:
            print '/foo/nested already exists'

        print 'ACL of foo:', conn.get_acl('/foo')[1]

        # create another connection
        with zkopen(ZOOKEEPER_HOST, 5) as conn2:
            # add some authentication
            conn2.add_auth(IdSchema.Digest, 'user2:pass')
            # try to enumerate the children
            try:
                print 'conn2: /foo\'s children', conn2.get_children('/foo')
            except:
                print 'conn2: could not get children'
            # try to delete one of the children
            try:
                print 'conn2: deleting /foo/nested ....', 0 == conn2.delete('/foo/nested')
            except:
                print 'conn2: could not delete /foo/nested'

        print 'conn1: deleted /foo/nested', 0 == conn.delete('/foo/nested')
        print 'conn1: deleted /foo', 0 == conn.delete('/foo')

if __name__ == '__main__':
    main()

