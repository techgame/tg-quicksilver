#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~ Imports 
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

import os, sys
from contextlib import contextmanager

from ..mixins import NotStorableMixin
from .ambit import AmbitStrategy
from .rootProxy import RootProxy

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~ Definitions 
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

class BoundaryEntry(NotStorableMixin):
    __slots__ = ['oid', 'hash', 'obj', 'pxy', 'dirty']
    RootProxy = RootProxy
    def __init__(self, oid):
        self.oid = oid
        self.dirty = True
        self.pxy = self.RootProxy()

    def setup(self, obj, hash):
        self.obj = obj
        self.hash = hash
        self.dirty = not hash
        self.RootProxy.adaptProxy(self.pxy, obj)

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

class BoundaryStore(NotStorableMixin):
    AmbitStrategy = AmbitStrategy
    saveDirtyOnly = True

    def __init__(self, workspace):
        self.init(workspace)

    def init(self, workspace):
        self.ws = workspace
        self._cache = {}
        self._objCache = {}
        self._newEntries = []
        self._ambitList = []

    @contextmanager
    def ambit(self):
        ambitList = self._ambitList
        if ambitList:
            a = ambitList.pop()
        else: a = self.AmbitStrategy(self)

        yield a
        ambitList.append(a)

    def get(self, oid, default=NotImplemented):
        r = self._cache.get(oid, self)
        if r is not self:
            return r.pxy

        entry = BoundaryEntry(oid)
        if self._readEntry(entry):
            return entry.pxy
        elif default is NotImplemented:
            for k in self._cache.keys():
                print k
            raise LookupError("No object found for oid: %r" % (oid,), oid)
        return default

    def mark(self, oidOrObj, dirty=True):
        oid = self._objCache.get(oidOrObj, oidOrObj)
        entry = self._cache[oid]
        if dirty is not None:
            entry.dirty = dirty
        return entry

    def set(self, oid, obj, deferred=False):
        if oid is None:
            oid = self.ws.newOid()

        entry = BoundaryEntry(oid)
        entry.setup(obj, None)
        self._cache[oid] = entry
        self._objCache[entry.pxy] = oid
        self._objCache[entry.obj] = oid

        if deferred:
            self._newEntries.append(entry)
        else:
            self._writeEntry(entry)
            self._writeEntryCollection(self._iterNewEntries())
        return oid

    def delete(self, oid):
        entry = self._cache.pop(oid, None)
        if entry is not None:
            self._objCache.pop(entry.obj, None)
            self._objCache.pop(entry.pxy, None)
        self.ws.remove(oid)

    def saveAll(self):
        entryColl = self._iterNewEntries(self._cache.values())
        return self._writeEntryCollection(entryColl)

    def iterSaveAll(self):
        entryColl = self._iterNewEntries(self._cache.values())
        return self._iterWriteEntryCollection(entryColl)

    def commit(self, **kw):
        return self.ws.commit(**kw)

    def raw(self, oidOrObj, decode=True):
        oid = self._objCache.get(oidOrObj, oidOrObj)
        record = self.ws.read(oid)
        if decode and record:
            data = bytes(record.payload)
            if data.startswith('\x78\x9c'):
                # zlib encoded
                data = data.decode('zlib')
            self._onRead(record, data, None)
            record.payload = data
        else:
            self._onRead(record, None, None)
        return record

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    #~ Read and write entry workhorses
    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def _readEntry(self, entry):
        oid = entry.oid
        with self.ambit() as ambit:
            record = self.ws.read(oid)
            if record is None:
                return False
            if record.payload is None:
                return False

            self._cache[oid] = entry
            self._objCache[entry.pxy] = oid

            data = bytes(record.payload)
            if data.startswith('\x78\x9c'):
                # zlib encoded
                data = data.decode('zlib')

            obj = ambit.load(oid, data, record)
            entry.setup(obj, bytes(record.hash))
            self._onRead(record, data, entry)

        self._objCache[entry.obj] = oid
        return True

    def _checkWriteEntry(self):
        if self.saveDirtyOnly:
            return entry.dirty
        else: return True 

    def _writeEntry(self, entry):
        if not self._checkWriteEntry(entry):
            return False, None

        with self.ambit() as ambit:
            data, hash = ambit.dump(entry.oid, entry.obj)
            changed = (entry.hash != hash)
            if changed:
                entry.setup(entry.obj, hash)
                payload = data.encode('zlib')
                self._onWrite(payload, data, entry)
                self.ws.write(entry.oid, payload=buffer(payload), hash=buffer(hash))
        return changed, len(data)

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    #~ Collection writing
    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def _writeEntryCollection(self, entryCollection, onError=None):
        if onError is None:
            onError = self._onWriteError
        writeEntry = self._writeEntry
        for entries in entryCollection:
            for entry in entries:
                try: 
                    writeEntry(entry)
                except Exception, exc:
                    if onError(entry, exc):
                        raise

    def _iterWriteEntryCollection(self, entryCollection, onError=None):
        if onError is None:
            onError = self._onWriteError
        writeEntry = self._writeEntry
        for entries in entryCollection:
            for entry in entries:
                try: 
                    r = writeEntry(entry)
                except Exception, exc:
                    if onError(entry, exc):
                        raise
                else:
                    yield entry, r

    def _iterNewEntries(self, entries=None):
        if entries is not None:
            yield entries

        entries = self._popNewEntries()
        while entries:
            yield entries
            entries = self._popNewEntries()

    def _popNewEntries(self):
        entries = self._newEntries
        if entries:
            self._newEntries = []
            return entries

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def _onWrite(self, payload, data, entry): 
        pass
    def _onRead(self, record, data, entry): 
        pass

    def _onWriteError(self, entry, exc):
        sys.excepthook(*sys.exc_info())

