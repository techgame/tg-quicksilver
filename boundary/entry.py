#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~ Imports 
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

import weakref

from ..mixins import NotStorableMixin
from .ambit import IBoundaryStrategy, PickleAmbitCodec

from .rootProxy import RootProxy, RootProxyRef

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~ Definitions 
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

BoundaryAmbitCodec = PickleAmbitCodec

class BoundaryStrategy(IBoundaryStrategy):
    def __init__(self, store):
        self._store = store
        self._objCache = store._objCache

    def setBoundary(self, oid):
        self._targetOid = oid

    def objForRef(self, oid):
        return self._store.ref(oid)

    def refForObj(self, obj):
        if isinstance(obj, type): 
            return

        fnBoundary = getattr(obj, '_boundary_', False)
        if not fnBoundary: 
            return

        oid = self._objCache.get(obj, None)
        oid = fnBoundary(self, oid)
        if oid is False: 
            return

        if oid is None:
            return self._store.set(None, obj, True)
        elif oid != self._targetOid:
            return oid

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

    def _boundary_(self, bctx, oid):
        return oid

    def setup(self, obj=False, hash=False):
        if obj is not False:
            self.setObject(obj)
        if hash is not False:
            self.setHash(hash)

    def setObject(self, obj):
        self.obj = obj
        if obj is not None:
            self.RootProxy.adaptProxy(self.pxy, obj)

    def setDeferred(self, typeref):
        self._typeref = typeref
        self.RootProxy.adaptDeferred(self.pxy, self)

    def typeref(self):
        obj = self.obj
        if obj is None:
            return self._typeref
        else: return obj.__class__

    def setHash(self, hash):
        self.hash = hash
        self.dirty = not hash

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    _wrStore = None
    def fetchObject(self):
        return self._wrStore().get(self.oid)

    def fetchProxyFn(self, name, args):
        obj = self.fetchObject()
        return getattr(self.obj, name)

    _fetchRemapAttr = {'__class__':'_typeref', '_boundary_':'_boundary_'}
    def fetchProxyAttr(self, name, _remap_=_fetchRemapAttr):
        rn = _remap_.get(name)
        if rn is not None:
            return getattr(self, rn)

        obj = self.fetchObject()
        return getattr(self.obj, name)

    @classmethod
    def newFlyweight(klass, store, **kw):
        kw.update(_wrStore=weakref.ref(store))
        name = '%s_%s' % (klass.__name__, id(store))
        return type(klass)(name, (klass,), kw)

