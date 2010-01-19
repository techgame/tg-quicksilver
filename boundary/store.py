#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~ Imports 
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

import os, sys
from contextlib import contextmanager
from .ambit import PickleAmbitCodec
from .rootProxy import RootProxy

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~ Definitions 
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

class Ambit(object):
    Codec = PickleAmbitCodec

    def __init__(self, host):
        self.host = host
        self._objCache = host._objCache
        self._codec = self.Codec(self._refForObj, self._objForRef)
        self._target = None

    def encode(self, obj):
        assert self._target is None, self._target
        self._target = obj
        data = self._codec.encode(obj)
        hash = self._codec.hashDigest(data)

        #data = data.encode('zlib')
        self._target = None
        return data, hash

    def decode(self, data, meta=None):
        assert self._target is None, self._target
        #data = data.decode('zlib')
        return self._codec.decode(data, meta)

    def _refForObj(self, obj):
        if isinstance(obj, type): 
            return
        if obj is self._target:
            return 
        if not getattr(obj, '__boundary__',False):
            return

        oid = self._objCache.get(obj, None)
        if oid is None:
            oid = self.host.set(None, obj)
        return oid

    def _objForRef(self, ref):
        obj = self.host.get(ref)
        return obj

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

class BoundaryEntry(object):
    __slots__ = ['oid', 'hash', 'obj', 'pxy']
    def __init__(self, oid):
        self.oid = oid
        self.pxy = RootProxy()

    def setup(self, obj, hash):
        self.obj = obj
        self.hash = hash
        object.__setattr__(self.pxy, '_obj_', obj)

class BoundaryStore(object):
    def __init__(self, workspace):
        self.init(workspace)

    def init(self, workspace):
        self.ws = workspace
        self._cache = {}
        self._objCache = {}

    @contextmanager
    def _ambit(self):
        yield Ambit(self)

    def get(self, oid, default=NotImplemented):
        cache = self._cache
        r = cache.get(oid, cache)
        if r is not cache:
            return r.pxy

        entry = BoundaryEntry(oid)
        cache[oid] = entry

        with self._ambit() as ambit:
            objData = self.ws.read(oid)
            if objData is None:
                if default is NotImplemented:
                    raise LookupError("No object found at oid: %r" % (oid,), oid)
                else: return default

            obj, rest = ambit.decode(bytes(objData.payload), objData)
            entry.setup(obj, bytes(objData.hash))

        self._objCache[entry.pxy] = oid
        self._objCache[entry.obj] = oid
        return obj

    def set(self, oid, obj, **meta):
        if oid is None:
            oid = self.ws.newOid()

        entry = BoundaryEntry(oid)
        entry.setup(obj, None)
        self._cache[oid] = entry
        self._objCache[entry.pxy] = oid
        self._objCache[entry.obj] = oid

        with self._ambit() as ambit:
            data, hash = ambit.encode(obj)
            entry.setup(obj, hash)
            self.ws.write(oid, payload=buffer(data), hash=buffer(hash), **meta)
        return oid

    def delete(self, oid):
        cache = self._cache
        entry = cache.pop(oid, None)
        if entry is not None:
            self._objCache.pop(entry.obj, None)
            self._objCache.pop(entry.pxy, None)
        self.ws.remove(oid)

    def saveAll(self):
        ws = self.ws
        cache = self._cache
        with self._ambit() as ambit:
            for oid, entry in cache.items():
                data, hash = ambit.encode(entry.obj)
                if entry.hash != hash:
                    print 'changed:', (entry.hash.encode('hex'), hash.encode('hex'))
                    entry.hash = hash
                    ws.write(oid, payload=buffer(data), hash=buffer(hash))
                else:
                    print 'unchanged:', hash.encode('hex')
        return ws
                    
    def commit(self, **kw):
        return self.ws.commit(**kw)

