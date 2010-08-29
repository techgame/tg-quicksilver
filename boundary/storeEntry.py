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
from contextlib import contextmanager

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
    def _awakenCopyBoundary_(self, bndEntry, bndCtx):
        "to set state on a copy operation.  Called before awakenBoundary"
    def _updateBoundary_(self, bndEntry, bndCtx):
        "to prepare object for hibernation"

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

class BoundaryEntry(NotStorableMixin):
    RootProxy = RootProxy
    store = None

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
        if store is None:
            raise ValueError("Store cannot be None for flyweight")

        kw.update(store=weakref.ref(store))
        name = '%s_%s' % (klass.__name__, id(store))
        return type(klass)(name, (klass,), kw)

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def getMeta(self):
        return self.ghostOid
    def setMeta(self, data):
        if data is None: return

        if isinstance(data, (int, long)):
            self.ghostOid = data

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def isActive(self):
        return self.obj is not None

    def setup(self, obj=False, hash=False):
        if obj is not False:
            self.setObject(obj)
        if hash is not False:
            self.setHash(hash)

    def decodeFailure(self, err, strTyperef):
        if not self.hasGhost(): return

        if isinstance(err, AttributeError):
            self.setDeferred(None)
            return True

    def setObject(self, obj):
        self.obj = obj
        if obj is not None:
            self.RootProxy.adaptProxy(self.pxy, obj)
            self.awakenObject(obj)

    def setDeferred(self, typeref):
        self._typeref = typeref
        self.RootProxy.adaptDeferred(self.pxy, self)

    def setupAsCopy(self, srcEntry):
        self.pxy = srcEntry.pxy
        self.awakenObject = self._awakenObjectCopy
        self.setObject(srcEntry.obj)

    def setHash(self, hash):
        self.hash = hash
        self.dirty = not hash

    _typeref = None
    def typeref(self):
        obj = self.obj
        if obj is None:
            return self._typeref
        else: return obj.__class__

    def proxyBoundary(self, bndV, bndCtx):
        return self.oid

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def getContext(self):
        return self.store().context
    context = property(getContext)

    def awakenObject(self, obj):
        return self._awakenObjectLoad(obj)

    def _awakenObjectLoad(self, obj):
        awaken = getattr(obj, '_awakenBoundary_', None)
        if awaken is not None:
            return awaken(self, self.store().context)

    def _awakenObjectCopy(self, obj):
        del self.awakenObject

        awakenCopy = getattr(obj, '_awakenCopyBoundary_', None)
        if awakenCopy is not None:
            awakenCopy(self, self.store().context)

        return self._awakenObjectLoad(obj)

    def _awakenObjectCopier(self, obj):
        self.bsCopyTarget.addEntryCopy(self)

    bsCopyTarget = None
    @classmethod
    @contextmanager
    def inContextForCopy(klass, bsSrc, bsTarget):
        if klass.bsCopyTarget is not None:
            raise RuntimeError("Nested copy context detected: bsCopyTarget is not None")

        klass.bsCopyTarget = bsTarget
        klass.awakenObject = klass._awakenObjectCopier
        try:
            yield
        finally:
            klass.awakenObject = klass._awakenObjectLoad
            klass.bsCopyTarget = None

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def hibernate(self):
        obj = self.obj
        updateBoundary = getattr(obj, '_updateBoundary_', None)
        if updateBoundary is not None:
            updateBoundary(self, self.store().context)
        return self.obj, self.getMeta()

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def hasGhost(self):
        return self.ghostOid is not None

    ghostOid = None
    def getGhostObj(self):
        g_oid = self.ghostOid
        if g_oid is not None:
            return self.store().get(g_oid)
    def setGhostObj(self, g_obj):
        if g_obj is not None:
            g_oid = self.store().add(g_obj)
        else: g_oid = None
        self.ghostOid = g_oid
    def delGhostObj(self):
        self.ghostOid = None
        del self.ghostOid
    ghostObj = property(getGhostObj, setGhostObj, delGhostObj)

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    _ghost_obj = None
    def fetchObject(self):
        obj = self.obj
        if obj is not None:
            return obj

        obj = self._ghost_obj
        if obj is not None:
            return obj

        # read the entry from store into self
        if not self.store()._readEntry(self):
            return None

        # return the obj set on self
        obj = self.obj

        if obj is None:
            # unless there was a readFailure
            obj = self.getGhostObj()
            self._ghost_obj = obj
            self._typeref = obj.__class__
        return obj

    def fetchProxyFn(self, name, args):
        obj = self.fetchObject()
        return getattr(obj, name, None)

    _fetchRemapAttr = {'__class__':'_typeref', '_boundary_':'proxyBoundary'}
    def fetchProxyAttr(self, name, _remap_=_fetchRemapAttr):
        rn = _remap_.get(name)
        if rn is not None:
            return getattr(self, rn)

        obj = self.fetchObject()
        return getattr(obj, name)

