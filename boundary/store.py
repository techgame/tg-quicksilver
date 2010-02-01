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
from contextlib import contextmanager

from ..mixins import NotStorableMixin
from . import storeEntry, storeRegistry, storeStrategy

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~ Definitions 
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

class BoundaryStore(NotStorableMixin):
    BoundaryStoreRegistry   = storeRegistry.BoundaryStoreRegistry
    BoundaryEntry           = storeEntry.BoundaryEntry
    BoundaryStrategy        = storeStrategy.BoundaryStrategy
    BoundaryAmbitCodec      = storeStrategy.BoundaryAmbitCodec
    BoundaryRefWalker       = storeStrategy.BoundaryRefWalker

    saveDirtyOnly = True
    context = None

    def __init__(self, workspace):
        self.init(workspace)

    def init(self, workspace):
        self.ws = workspace
        self.BoundaryEntry = self.BoundaryEntry.newFlyweight(self)

        self.reg = self.BoundaryStoreRegistry()
        self._deferredEntries = []
        self._ambitList = []

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    #~ Read Access: ref, get, raw
    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def ref(self, oid):
        """Returns a deferred proxied to the object stored at oid"""
        entry = self.reg.lookup(oid)
        if entry is not None:
            return entry.pxy

        entry = self.BoundaryEntry(oid)
        if self._hasEntry(entry):
            return entry.pxy
        else: return None

    def get(self, oid, default=NotImplemented):
        """Returns a transparent proxied to object stored at oid"""
        entry = self.reg.lookup(oid, None)
        if entry is None:
            entry = self.BoundaryEntry(oid)
        elif entry.obj is not None:
            # otherwise we need to load the proxy
            return entry.pxy

        if self._readEntry(entry):
            return entry.pxy
        elif default is NotImplemented:
            raise LookupError("No object found for oid: %r" % (oid,), oid)
        return default

    def raw(self, oidOrObj, decode=True):
        """Returns the raw database record for the oidOrObj"""
        entry = self.reg.lookup(oidOrObj)
        if entry is None: 
            oid = long(oidOrObj)
        else: oid = entry.oid

        rec = self.ws.read(oid)
        if decode and rec:
            data = self._decodeData(rec['payload'])
            self._onReadRaw(rec, data)
            rec = dict(rec)
            rec['payload'] = buffer(data)
        else:
            self._onReadRaw(rec, None)
        return rec

    def findRefsFrom(self, oidOrObj):
        """Returns a set of oids referenced by oidOrObj"""
        data = self.raw(oidOrObj, True)
        refs = self.BoundaryRefWalker().findRefs(data)
        return refs

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    #~ Write Access: mark, add, set, delete
    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def mark(self, oidOrObj, dirty=True):
        entry = self.reg.lookup(oidOrObj)
        if dirty is not None:
            entry.dirty = dirty
        return entry

    def add(self, obj, deferred=False):
        return self.set(None, obj, deferred)
    def set(self, oid, obj, deferred=False, onError=None):
        if oid is None:
            oid = self.ws.newOid()

        entry = self.BoundaryEntry(oid)
        entry.setup(obj, None)
        self.reg.add(entry)

        if deferred:
            self._deferredEntries.append(entry)
            return oid

        self._writeEntry(entry)
        self._writeEntryCollection(self._iterNewEntries(), onError)
        return oid

    def write(self, oid, deferred=False, onError=None):
        entry = self.reg.lookup(oid)
        if entry is None:
            return None

        if deferred:
            self._deferredEntries.append(entry)
            return entry

        self._writeEntry(entry)
        self._writeEntryCollection(self._iterNewEntries(), onError)
        return entry

    def delete(self, oid):
        self.reg.remove(oid)
        self.ws.remove(oid)

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    #~ Dictionary-like key access by oid
    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def __getitem__(self, oid):
        return self.get(oid)
    def __setitem__(self, oid, obj):
        return self.set(oid, obj)
    def __detitem__(self, oid):
        return self.delete(oid)

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    #~ Workspace methods: commit, saveAll
    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def allOids(self):
        """Returns all stored oids"""
        return self.ws.allOids()

    def allLoadedOids(self):
        """Returns all currently loaded oids"""
        return self.reg.allLoadedOids()

    def nextGroupId(self):
        """Increments groupId to mark sets of changes to support in-changeset rollback"""
        return self.ws.nextGroupId()

    def saveAll(self, onError=None):
        """Saves all entries loaded.  
        See also: saveDirtyOnly to controls behavior"""
        entryColl = self._iterNewEntries(self.reg.allLoadedOids())
        return self._writeEntryCollection(entryColl, onError)

    def iterSaveAll(self, onError=None):
        """Saves all entries loaded.  
        See also: saveDirtyOnly to controls behavior"""
        entryColl = self._iterNewEntries(self.reg.allLoadedOids())
        return self._iterWriteEntryCollection(entryColl, onError)

    def commit(self, **kw):
        """Commits changeset to quicksilver backend store"""
        return self.ws.commit(**kw)

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    #~ Utility: Entry read and write workhorses
    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def _hasEntry(self, entry):
        ref = self.ws.contains(entry.oid)
        if not ref:
            return False

        with self.ambit() as ambit:
            rec = self.ws.read(ref, 'typeref')
            typeref = ambit.decodeTyperef(rec['typeref'])
            entry.setDeferred(typeref)

        self.reg.add(entry)
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

            self.reg.add(entry)

            data = self._decodeData(data)
            obj = ambit.load(oid, data)
            hash = rec['hash']
            if hash: hash = bytes(hash)
            entry.setup(obj, hash)
            self._onRead(rec, data, entry)

        self.reg.add(entry)
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
                self._onWrite(payload, data, entry)

                typeref = ambit.encodeTyperef(entry.typeref())
                seqId = self.ws.write(oid, 
                    hash=buffer(hash), typeref=typeref, payload=payload)
                entry.setHash(hash)

                # TODO: process delegated hashing
                ##if entry.hash != hash:
                ##    self.ws.postUpdate(seqId, hash=buffer(hash))
                ##    entry.setHash(hash)
                ##else: self.ws.postBackout(seqId)

        return changed, len(data)

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    #~ Payload encoding/decoding
    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

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
        entries = self._deferredEntries
        if entries:
            self._deferredEntries = []
            return entries

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    #~ Utility: ambit access, events
    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    @contextmanager
    def ambit(self):
        """Returns an ambit for the boundary store instance"""
        ambitList = self._ambitList
        if ambitList:
            ambit = ambitList.pop()
        else: 
            boundary = self.BoundaryStrategy(self, self.context)
            ambit = self.BoundaryAmbitCodec(boundary)

        yield ambit
        ambitList.append(ambit)

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def _onWrite(self, payload, data, entry): 
        pass
    def _onRead(self, rec, data, entry): 
        pass
    def _onReadRaw(self, rec, data):
        pass

    def _onWriteError(self, entry, exc):
        sys.excepthook(*sys.exc_info())
        print >> sys.stderr, 'entry %r' % (entry,)

