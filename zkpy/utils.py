'''
Created on 20.08.2010

@author: luk
'''


def enum(*sequential, **named):
    '''Creates a faked enum

    Animals = enum('Owl','Tiger','Snake')
    Plants = enum(Carrot = (1,2,3), Apple = 2)
    Mixed = enum('Foo', Bar=19)

    Usage:
        >>> Mixed = enum('Foo', Bar=19)
        >>> Mixed.Bar
        19
        >>> Mixed[19]
       'Bar'
       >>> Mixed[0]
       'Foo'

    Note: This function translates more or less into
        >>> class Enum(dict):
        >>>     __slots__ = ()
        >>>     Foo = 0
        >>>     Bar = 19
        >>> Mixed = Enum([0: 'Foo', 19 : 'Bar'])

    After recipe http://code.activestate.com/recipes/577024/
    '''
    enums = dict(zip(sequential, range(len(sequential))), __slots__ = (), **named)
    return type('Enum', (dict,), enums)( (v,k) for k,v in enums.iteritems())

