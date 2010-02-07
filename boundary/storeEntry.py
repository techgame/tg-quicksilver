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

import weakref

from ..mixins import NotStorableMixin
from .rootProxy import RootProxy, RootProxyRef

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~ Definitions 
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

class _AnExampleEntryObject_(object):
    """Use pickle protocol to handle persistence"""

    def _boundary_(self, bndV, bndCtx):
        return True # to allocated an oid
    def _awakenBoundary_(self, bndEntry, bndCtx):
        "to set state when fully loaded from boundary store"

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

class BoundaryEntry(NotStorableMixin):
    RootProxy = RootProxy

    def __init__(self, oid):
        self.oid = oid
        self.pxy = self.RootProxy()
        self.setup(None, None)

    def __repr__(self):
        k = self.__class__
        return '<%s.%s oid:%s>@%r' % (
                k.__module__, k.__name__, 
                self.oid, self.pxy)

    @classmethod
    def newFlyweight(klass, store, **kw):
        if store is not None:
            store=weakref.ref(store)
            name = '%s_%s' % (klass.__name__, id(store))
        else:
            name = '%s_copier' % (klass.__name__,)
        kw.update(store=store)
        return type(klass)(name, (klass,), kw)

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def setup(self, obj=False, hash=False):
        if obj is not False:
            self.setObject(obj)
        if hash is not False:
            self.setHash(hash)

    def setObject(self, obj):
        self.obj = obj
        if obj is not None:
            self.RootProxy.adaptProxy(self.pxy, obj)
            self.awakenObject(obj)

    def setDeferred(self, typeref):
        self._typeref = typeref
        self.RootProxy.adaptDeferred(self.pxy, self)

    def setHash(self, hash):
        self.hash = hash
        self.dirty = not hash

    def typeref(self):
        obj = self.obj
        if obj is None:
            return self._typeref
        else: return obj.__class__

    def proxyBoundary(self, bndV, bndCtx):
        return self.oid

    def awakenObject(self, obj):
        awaken = getattr(obj, '_awakenBoundary_', None)
        if awaken is not None:
            awaken(self, self.store().context)

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    store = None
    def fetchObject(self):
        obj = self.obj
        if obj is None:
            obj = self.store().get(self.oid)
        return obj

    def fetchProxyFn(self, name, args):
        obj = self.fetchObject()
        return getattr(self.obj, name, None)

    _fetchRemapAttr = {'__class__':'_typeref', '_boundary_':'proxyBoundary'}
    def fetchProxyAttr(self, name, _remap_=_fetchRemapAttr):
        rn = _remap_.get(name)
        if rn is not None:
            return getattr(self, rn)

        obj = self.fetchObject()
        return getattr(self.obj, name)

