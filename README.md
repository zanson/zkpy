zkpy
====

zkpy is a wrapper around [Zookeeper's](http://hadoop.apache.org/zookeeper/) Python library, that should provide

- a more convenient access to zookeeper
- some of [zookeeper recipes](http://hadoop.apache.org/zookeeper/docs/current/recipes.html)


Short example:
--------------

    from __future__ import with_statement
    from zkpy.acl import Acls, IdSchema
    from zkpy.connection import zkopen, KeeperState, EventType, NodeCreationMode
    import zookeeper
    # define a watcher function
    def global_watcher(type, state, path):
        print 'GLOBAL: event=%s, state=%s, path=%s' % (EventType[type], KeeperState[state], path)
    with zkopen('localhost:2181', 5) as conn:
        # add a connection watcher for fun
        conn.add_global_watcher(global_watcher)
       
        # create a node
        conn.create('/bar','', [Acls.Unsafe], NodeCreationMode.Ephemeral)
        print conn.get_children('/')
        
       

Todo:
-----

* Better zookeeper wrapping (Exceptions, return codes)
* More recipes
* Tests
* Documentation

