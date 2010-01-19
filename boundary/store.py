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
        self._target = None
        return data, hash

    def decode(self, data, meta=None):
        assert self._target is None, self._target
        return self._codec.decode(data, meta)

    def _refForObj(self, obj):
        if isinstance(obj, type): 
            return
        if obj is self._target:
            return 
        if not getattr(obj, '__bounded__', False):
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
        r = self._cache.get(oid, self)
        if r is not self:
            return r.pxy

        entry = BoundaryEntry(oid)
        if self._readEntry(entry):
            return entry.pxy
        elif default is NotImplemented:
            raise LookupError("No object found for oid: %r" % (oid,), oid)
        return default

    def set(self, oid, obj):
        if oid is None:
            oid = self.ws.newOid()

        entry = BoundaryEntry(oid)
        entry.setup(obj, None)
        self._cache[oid] = entry
        self._objCache[entry.pxy] = oid
        self._objCache[entry.obj] = oid

        self._writeEntry(entry)
        return oid

    def delete(self, oid):
        entry = self._cache.pop(oid, None)
        if entry is not None:
            self._objCache.pop(entry.obj, None)
            self._objCache.pop(entry.pxy, None)
        self.ws.remove(oid)

    def saveAll(self):
        for e in self.iterSaveAll():
            pass

    def iterSaveAll(self, entries=None):
        if entries is None:
            entries = self._cache.values()

        writeEntry = self._writeEntry
        with self._ambit() as ambit:
            for entry in entries:
                changed = writeEntry(entry)
                yield entry, changed
                    
    def commit(self, **kw):
        return self.ws.commit(**kw)

    def raw(self, oidOrObj, decode=True):
        oidOrObj = self._objCache.get(oidOrObj, oidOrObj)
        record = self.ws.read(oidOrObj)
        if decode and record:
            data = bytes(record.payload)
            data = data.decode('zlib')
            self._onRead(record, data)
            record.payload = data
        else:
            self._onRead(record, None)
        return record

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def _readEntry(self, entry):
        oid = entry.oid
        with self._ambit() as ambit:
            record = self.ws.read(oid)
            if record is None:
                return False

            self._cache[oid] = entry
            self._objCache[entry.pxy] = oid

            data = bytes(record.payload)
            data = data.decode('zlib')
            self._onRead(record, data)
            obj, record = ambit.decode(data, record)
            entry.setup(obj, bytes(record.hash))

        self._objCache[entry.obj] = oid
        return True

    def _writeEntry(self, entry):
        with self._ambit() as ambit:
            data, hash = ambit.encode(entry.obj)
            changed = (entry.hash != hash)
            if changed:
                entry.setup(entry.obj, hash)
                payload = data.encode('zlib')
                self._onWrite(payload, data)
                self.ws.write(entry.oid, payload=buffer(payload), hash=buffer(hash))
        return changed

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def _onReadNull(self, record, data=None): pass
    _onRead =_onReadNull
    def _onReadStats(self, record, data=None):
        if data is None:
            return
        print '@read  %8.1f%% = %6i/%6i' % (
                ((100.0 * len(data))/len(record.payload)),
                len(data), len(record.payload),)

    _onRead = _onReadStats

    def _onWriteNull(self, payload, data): pass
    _onWrite = _onWriteNull
    def _onWriteStats(self, payload, data):
        print '@write %8.1f%% = %6i/%6i' % (
                ((100.0 * len(data))/len(payload)),
                len(data), len(payload),)
    _onWrite = _onWriteStats

