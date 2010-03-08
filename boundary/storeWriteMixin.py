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

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~ Definitions 
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

class BoundaryStoreWriteMixin(object):
    def _initWriteStore(self):
        self._deferredWriteEntries = []

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    #~ Write Access: mark, add, set, delete
    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def mark(self, oidOrObj, dirty=True):
        entry = self.reg.lookup(oidOrObj)
        if dirty is not None:
            entry.dirty = dirty
        return entry

    def add(self, obj, deferred=False, onError=None):
        entry = self.setEntry(None, obj, deferred, onError)
        return entry.oid
    append = add

    def set(self, oid, obj, deferred=False, onError=None):
        entry = self.setEntry(oid, obj, deferred, onError)
        return entry.oid

    def __setitem__(self, oid, obj):
        self.setEntry(oid, obj)

    def setEntry(self, oid, obj, deferred=False, onError=None):
        entry = self.reg.lookup(obj)
        if entry is not None:
            oid = entry.oid

        if oid is None:
            oid = self.ws.newOid()

        if entry is None:
            entry = self.BoundaryEntry(oid)
            entry.setup(obj, None)
            self.reg.add(entry)

        if deferred:
            self._deferredWriteEntries.append(entry)
            return entry

        entryColl = self._iterNewWriteEntries([entry])
        self._writeEntryCollection(entryColl, False, onError)
        return entry

    def addEntryCopy(self, fgnEntry):
        if fgnEntry.store() is self:
            raise RuntimeError("Foreign Entry is not foreign")

        oid = self.ws.newOid()
        entry = self.BoundaryEntry(oid)
        entry.setupAsCopy(fgnEntry)
        self.reg.add(entry)

        self._deferredWriteEntries.append(entry)
        return entry

    def write(self, oid, deferred=False, onError=None):
        entry = self.writeEntry(oid, deferred, onError)
        if entry is not None:
            return entry.oid

    def writeEntry(self, oid, deferred=False, onError=None):
        entry = self.reg.lookup(oid)
        if entry is None:
            return None

        if deferred:
            self._deferredWriteEntries.append(entry)
            return entry

        entryColl = self._iterNewWriteEntries([entry])
        self._writeEntryCollection(entryColl, False, onError)
        return entry

    def __delitem__(self, oidOrObj):
        self.delete(oidOrObj)

    def delete(self, oidOrObj):
        entry = self.reg.lookup(oidOrObj)
        self.reg.remove(entry.oid)
        self.ws.remove(entry.oid)

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    #~ Workspace methods: commit, saveAll
    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def nextGroupId(self):
        """Increments groupId to mark sets of changes to support in-changeset rollback"""
        return self.ws.nextGroupId()

    def saveAll(self, context=False, onError=None):
        """Saves all entries loaded.  
        See also: saveDirtyOnly to controls behavior"""
        entryColl = self._iterNewWriteEntries(self.reg.allLoadedEntries())
        return self._writeEntryCollection(entryColl, context, onError)

    def iterSaveAll(self, context=False, onError=None):
        """Saves all entries loaded.  
        See also: saveDirtyOnly to controls behavior"""
        entryColl = self._iterNewWriteEntries(self.reg.allLoadedEntries())
        return self._iterWriteEntryCollection(entryColl, context, onError)

    def commit(self, **kw):
        """Commits changeset to quicksilver backend store"""
        return self.ws.commit(**kw)

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    #~ Utility: Entry write workhorses
    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def _checkWriteEntry(self, entry):
        if self.saveDirtyOnly:
            return entry.dirty
        else: return True 

    def _writeEntry(self, entry, context=False, onError=None):
        if entry.obj is None:
            return False, None
        elif not self._checkWriteEntry(entry):
            return False, None

        try:
            with self.ambit(entry, context) as ambit:
                self.reg.add(entry)
                data = ambit.dump(entry.obj)
                # TODO: delegate expensive hashing
                hash = ambit.hashDigest(data)
                changed = (entry.hash != hash)

                if changed:
                    payload = self._encodeData(data)
                    self._onWrite(payload, data, entry)

                    typeref = ambit.encodeTyperef(entry.typeref())
                    seqId = self.ws.write(entry.oid, 
                        hash=buffer(hash), typeref=typeref, payload=payload)
                    entry.setHash(hash)

                    # TODO: process delegated hashing
                    ##if entry.hash != hash:
                    ##    self.ws.postUpdate(seqId, hash=buffer(hash))
                    ##    entry.setHash(hash)
                    ##else: self.ws.postBackout(seqId)
                pass
        except Exception, exc:
            if onError is None:
                onError = self._onWriteError
            if onError(entry, exc):
                raise
            return None

        else:
            return changed, len(data)

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    #~ Collection writing
    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def _writeEntryCollection(self, entryCollection, context=False, onError=None):
        if onError is None:
            onError = self._onWriteError
        writeEntry = self._writeEntry
        with self.ws.conn:
            for entries in entryCollection:
                for entry in entries:
                    writeEntry(entry, context, onError)

    def _iterWriteEntryCollection(self, entryCollection, context=False, onError=None):
        if onError is None:
            onError = self._onWriteError
        writeEntry = self._writeEntry
        with self.ws.conn:
            for entries in entryCollection:
                for entry in entries:
                    r = writeEntry(entry, context, onError)
                    if r is not None:
                        yield entry, r

    def _iterNewWriteEntries(self, entries=None):
        if entries is not None:
            yield entries

        entries = self._popNewWriteEntries()
        while entries:
            yield entries
            entries = self._popNewWriteEntries()

    def _popNewWriteEntries(self):
        entries = self._deferredWriteEntries
        if entries:
            self._deferredWriteEntries = []
            return entries

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def _onWrite(self, payload, data, entry): 
        pass
    def _onWriteError(self, entry, exc):
        sys.excepthook(*sys.exc_info())
        print >> sys.stderr, 'entry %r' % (entry,)

