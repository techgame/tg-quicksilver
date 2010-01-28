#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~ Imports 
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

import sys
from contextlib import contextmanager

from ..mixins import NotStorableMixin
from . import entry

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~ Definitions 
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

class BoundaryStore(NotStorableMixin):
    BoundaryEntry = entry.BoundaryEntry
    BoundaryStrategy = entry.BoundaryStrategy
    AmbitCodec = entry.BoundaryAmbitCodec
    saveDirtyOnly = True

    def __init__(self, workspace):
        self.init(workspace)

    def init(self, workspace):
        self.ws = workspace
        self.BoundaryEntry = self.BoundaryEntry.newFlyweight(self)

        self._cache = {}
        self._objCache = {}
        self._newEntries = []
        self._ambitList = []

    @contextmanager
    def ambit(self):
        ambitList = self._ambitList
        if ambitList:
            ambit = ambitList.pop()
        else: 
            boundary = self.BoundaryStrategy(self)
            ambit = self.AmbitCodec(boundary)

        yield ambit
        ambitList.append(ambit)

    def ref(self, oid):
        entry = self._cache.get(oid, self)
        if entry is not self:
            return entry.pxy
        else: entry = self.BoundaryEntry(oid)

        if self._hasEntry(entry):
            return entry.pxy
        else: return None

    def get(self, oid, default=NotImplemented):
        entry = self._cache.get(oid, self)
        if entry is not self:
            if entry.obj is not None:
                return entry.pxy
        else: entry = self.BoundaryEntry(oid)

        if self._readEntry(entry):
            return entry.pxy
        elif default is NotImplemented:
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

        entry = self.BoundaryEntry(oid)
        entry.setup(obj, None)
        self._cache[oid] = entry
        self._objCache[entry.pxy] = oid
        self._objCache[entry.obj] = oid

        if deferred:
            self._newEntries.append(entry)
            return oid

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
        rec = self.ws.read(oid)
        if decode and rec:
            data = self._decodeData(rec['payload'])
            self._onRead(rec, data, None)
            rec = dict(rec)
            rec['payload'] = buffer(data)
        else:
            self._onRead(rec, None, None)
        return rec

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    #~ Read and write entry workhorses
    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def nextGroupId(self):
        return self.ws.nextGroupId()

    def _hasEntry(self, entry):
        ref = self.ws.contains(entry.oid)
        if not ref:
            return False

        with self.ambit() as ambit:
            rec = self.ws.read(ref, 'typeref')
            typeref = ambit.decodeTyperef(rec['typeref'])
            entry.setDeferred(typeref)

        oid = entry.oid
        self._cache[oid] = entry
        self._objCache[entry.pxy] = oid
        return True

    def _readEntry(self, entry):
        oid = entry.oid
        with self.ambit() as ambit:
            rec = self.ws.read(oid)
            if rec is None:
                return False
            data = rec['payload']
            if data is None:
                return False

            self._cache[oid] = entry
            self._objCache[entry.pxy] = oid

            data = self._decodeData(data)
            obj = ambit.load(oid, data)
            hash = rec['hash']
            if hash: hash = bytes(hash)
            entry.setup(obj, hash)
            self._onRead(rec, data, entry)

        self._objCache[entry.obj] = oid
        return True

    def _checkWriteEntry(self, entry):
        if self.saveDirtyOnly:
            return entry.dirty
        else: return True 

    def _writeEntry(self, entry):
        if not self._checkWriteEntry(entry):
            return False, None

        oid = entry.oid
        obj = entry.obj
        if obj is None:
            return False, None

        with self.ambit() as ambit:
            data = ambit.dump(oid, obj)
            # TODO: delegate expensive hashing
            hash = ambit.hashDigest(oid, data)
            changed = (entry.hash != hash)

            if changed:
                payload = self._encodeData(data)
                hash = buffer(hash)
                self._onWrite(payload, data, entry)

                typeref = ambit.encodeTyperef(entry.typeref())
                seqId = self.ws.write(oid, 
                    hash=hash, typeref=typeref, payload=payload)
                entry.setHash(hash)

                #if entry.hash != hash:
                #    self.ws.postUpdate(seqId, hash=buffer(hash))
                #    entry.setHash(hash)
                #else: self.ws.postBackout(seqId)

        return changed, len(data)

    def _encodeData(self, data):
        return buffer(data.encode('zlib'))
    def _decodeData(self, payload):
        data = bytes(payload)
        if data.startswith('\x78\x9c'):
            # zlib encoded
            data = data.decode('zlib')
        return data

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
    def _onRead(self, rec, data, entry): 
        pass

    def _onWriteError(self, entry, exc):
        sys.excepthook(*sys.exc_info())
        print >> sys.stderr, 'entry %r' % (entry,)

