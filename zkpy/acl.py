'''
Created on 20.08.2010

@author: luk
'''

from zkpy.utils import enum
import base64
import hashlib
import logging


# Possible ACL permission constants
AclPermission = enum(
    Read    = 1,       # zookeeper.PERM_READ
    Write   = 2,       # zookeeper.PERM_WRITE
    Create  = 4,       # zookeeper.PERM_CREATE
    Delete  = 8,       # zookeeper.PERM_DELETE
    Admin   = 16,      # zookeeper.PERM_ADMIN
    All     = 31       # zookeeper.PERM_ALL
)
# Possible? Id schemes
IdSchema = enum(
        Digest  = 'digest',#
        Ip      = 'ip',#
        Auth    = 'auth',#
        World   = 'world')#


class Id(object):
    '''Represents a Zookeeper id.
    Modeled after org.apache.zookeeper.data.Id

    '''
    __slots__ = ['scheme', 'id']

    logger = logging.getLogger('Id')

    def __init__(self, scheme, id = ''):
        '''Constructs the id object.

        :param scheme: One of the possible id schemes (see IdSchema)
        :param id: id string

        '''
        self.scheme = scheme

        # assign id for provided scheme
        if self.scheme == IdSchema.Digest:
            user, password = id.split(':')
            self.id = self.digest(user, password)
        elif self.scheme == IdSchema.World:
            if id and id != 'anyone':
                self.logger.info('Ignoring provided id "%s" for scheme "%s"' % (id, IdSchema.World))
            self.id = 'anyone'
        elif self.scheme == IdSchema.Auth:
            if id:
                self.logger.info('Ignoring provided id "%s" for scheme "%s"' % (id, IdSchema.Auth))
            self.id = ''
        else:
            self.id = id

    @staticmethod
    def digest(user, password):
        '''Encodes the zookeeper credentials into a zookeeper digest.'''
        return "%s:%s" % (
                user,
                base64.b64encode(
                    hashlib.sha1('%s:%s' % (user, password)).digest()))

class Acl(dict):
    '''Single item in Zookeeper's access control list (ACL)'''
    __slots__ = ['perms', 'id']

    def __init__(self, permissions, id):
        self.perms = permissions
        self.id = id

    def __getattr__(self, key):
        if key not in self.__slots__:
                raise AttributeError
        return self[key]

    def __setattr__(self, key, value):
        self[key] = value

    def __setitem__(self, key, val):
        if key == 'perms':
            dict.__setitem__(self, key, int(val))
        elif key == 'id' and isinstance(val, Id):
            dict.__setitem__(self, 'scheme', val.scheme)
            dict.__setitem__(self, 'id', val.id)
        else:
            raise AttributeError

    def __hash__(self):
        return hash('%s:%d:%s' % (self['scheme'], self['perms'], self['id']))


# Some predefined Ids
Ids = enum(
        Anyone        = Id('world', 'anyone'),
        Authenticated = Id('auth'))
# Some predefined ACL items
Acls = enum(
    Unsafe   = Acl(AclPermission.All, Ids.Anyone),          # completely unsafe
    Creator  = Acl(AclPermission.All, Ids.Authenticated),   # all permissions to the creator, NEED TO CALL add_auth() on the connection first
    Readonly = Acl(AclPermission.Read, Ids.Anyone)          # readable by anyone
)

