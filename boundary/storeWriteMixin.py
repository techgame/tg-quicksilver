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
            if entry is not None:
                entry.dirty = dirty
        return entry

    def add(self, obj, deferred=False):
        entry = self.setEntry(None, obj, deferred)
        return entry.oid
    append = add

    def set(self, oid, obj, deferred=False):
        entry = self.setEntry(oid, obj, deferred)
        return entry.oid

    def __setitem__(self, oid, obj):
        self.setEntry(oid, obj)

    def setEntry(self, oid, obj, deferred=False):
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
        self._writeEntryCollection(entryColl)
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

    def write(self, oid, deferred=False):
        entry = self.writeEntry(oid, deferred)
        if entry is not None:
            return entry.oid

    def writeEntry(self, oid, deferred=False):
        entry = self.reg.lookup(oid)
        if entry is None:
            return None

        if deferred:
            self._deferredWriteEntries.append(entry)
            return entry

        entryColl = self._iterNewWriteEntries([entry])
        self._writeEntryCollection(entryColl)
        return entry

    def __delitem__(self, oidOrObj):
        self.delete(oidOrObj)

    def delete(self, oidOrObj):
        entry = self.reg.lookup(oidOrObj)
        if entry is None: 
            return False
        return self._deleteByOid(entry.oid)
    def _deleteByOid(self, oid):
        res = self.ws.remove(oid)
        self.reg.remove(oid)
        return res

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    #~ Workspace methods: commit, saveAll
    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def nextRevId(self, oid):
        return self.ws.nextRevId(oid)

    def nextGroupId(self):
        """Increments groupId to mark sets of changes to support in-changeset rollback"""
        return self.ws.nextGroupId()

    def saveAll(self):
        """Saves all entries loaded.  
        See also: saveDirtyOnly to controls behavior"""
        entryColl = self._iterNewWriteEntries(self.reg.allLoadedEntries())
        return self._writeEntryCollection(entryColl)

    def iterSaveAll(self):
        """Saves all entries loaded.  
        See also: saveDirtyOnly to controls behavior"""
        entryColl = self._iterNewWriteEntries(self.reg.allLoadedEntries())
        return self._iterWriteEntryCollection(entryColl)

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

    def _writeEntry(self, entry):
        if not entry.isActive():
            return False, None
        elif not self._checkWriteEntry(entry):
            return False, None

        try:
            obj, entryMeta = entry.hibernate()

            with self.ambit(entry) as ambit:
                self.reg.add(entry)
                data = ambit.dump(obj)
                # TODO: delegate expensive hashing
                hash = ambit.hashDigest(data)
                changed = (entry.hash != hash)

                if changed:
                    entry.dataChanged()
                    payload = self._encodeData(data)
                    self._onWrite(payload, data, entry)

                    typeref = ambit.encodeTyperef(entry.typeref())
                    seqId = self.ws.write(
                        entry.oid, 
                        hash=buffer(hash),
                        typeref=typeref,
                        payload=payload, 
                        entryMeta=entryMeta)
                    entry.setHash(hash)

                    # TODO: process delegated hashing
                    ##if entry.hash != hash:
                    ##    self.ws.postUpdate(seqId, hash=buffer(hash))
                    ##    entry.setHash(hash)
                    ##else: self.ws.postBackout(seqId)

                pass # note: exceptions while finishing ambit

        except Exception, exc:
            if entry.onWriteEntryError(exc):
                raise
            return False, None

        else:
            return changed, len(data)

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    #~ Collection writing
    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def _writeEntryCollection(self, entryCollection):
        writeEntry = self._writeEntry
        with self.ws.inCommit():
            for entries in entryCollection:
                for entry in entries:
                    writeEntry(entry)

    def _iterWriteEntryCollection(self, entryCollection):
        writeEntry = self._writeEntry
        with self.ws.inCommit():
            for entries in entryCollection:
                for entry in entries:
                    r = writeEntry(entry)
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

