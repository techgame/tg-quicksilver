##~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~##
##~ Copyright (C) 2002-2010  TechGame Networks, LLC.              ##
##~                                                               ##
##~ This library is free software; you can redistribute it        ##
##~ and/or modify it under the terms of the MIT style License as  ##
##~ found in the LICENSE file included with this distribution.    ##
##~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~##

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~ Imports 
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

import sys
import contextlib

from ...mixins import NotStorableMixin

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~ Definitions 
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

class IBoundaryStrategy(object):
    @contextlib.contextmanager
    def inBoundaryCtx(self, oid=None, obj=False):
        raise NotImplementedError('Subclass Responsibility: %r' % (self,))
    def objForRef(self, oid):
        raise NotImplementedError('Subclass Responsibility: %r' % (self,))
    def refForObj(self, obj):
        raise NotImplementedError('Subclass Responsibility: %r' % (self,))

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

class BaseAmbitCodec(NotStorableMixin):
    def __init__(self, strategy):
        self.init(strategy)

    def init(self, strategy):
        self.inBoundaryCtx = strategy.inBoundaryCtx
        self._initEncoder(strategy.refForObj)
        self._initDecoder(strategy.objForRef)

    def _initEncoder(self, idForObj=None):
        raise NotImplementedError('Subclass Responsibility: %r' % (self,))
    def _initDecoder(self, objForId=None):
        raise NotImplementedError('Subclass Responsibility: %r' % (self,))

    def dump(self, obj):
        """Return serialized data from obj, and hash if hash is True"""
        raise NotImplementedError('Subclass Responsibility: %r' % (self,))
    def load(self, data):
        """Return object unserialized from data"""
        raise NotImplementedError('Subclass Responsibility: %r' % (self,))
    def hashDigest(self, data):
        raise NotImplementedError('Subclass Responsibility: %r' % (self,))


    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    #~ typeref to support better deferred proxies ~~~~~~~

    def encodeTyperef(self, klass):
        return u'%s:%s' % (klass.__module__, klass.__name__)
    def decodeTyperef(self, typeref):
        if typeref:
            module, name = typeref.split(':')
            return self.find_class(module, name)
    def find_class(self, module, name):
        # Subclasses may override this
        __import__(module)
        mod = sys.modules[module]
        klass = getattr(mod, name)
        return klass

