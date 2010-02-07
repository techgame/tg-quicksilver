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

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~ Definitions 
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

class BoundaryStoreWriteMixin(object):
    def _initWriteStore(self):
        self._deferredEntries = []

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

    def __setitem__(self, oid, obj):
        return self.set(oid, obj)

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

    def __detitem__(self, oid):
        return self.delete(oid)

    def delete(self, oid):
        self.reg.remove(oid)
        self.ws.remove(oid)

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    #~ Workspace methods: commit, saveAll
    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def nextGroupId(self):
        """Increments groupId to mark sets of changes to support in-changeset rollback"""
        return self.ws.nextGroupId()

    def saveAll(self, onError=None):
        """Saves all entries loaded.  
        See also: saveDirtyOnly to controls behavior"""
        entryColl = self._iterNewEntries(self.reg.allLoadedEntries())
        return self._writeEntryCollection(entryColl, onError)

    def iterSaveAll(self, onError=None):
        """Saves all entries loaded.  
        See also: saveDirtyOnly to controls behavior"""
        entryColl = self._iterNewEntries(self.reg.allLoadedEntries())
        return self._iterWriteEntryCollection(entryColl, onError)

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

    def _writeEntry(self, entry, context=False):
        if entry.obj is None:
            return False, None
        elif not self._checkWriteEntry(entry):
            return False, None

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
        entries = self._deferredEntries
        if entries:
            self._deferredEntries = []
            return entries

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def _onWrite(self, payload, data, entry): 
        pass
    def _onWriteError(self, entry, exc):
        sys.excepthook(*sys.exc_info())
        print >> sys.stderr, 'entry %r' % (entry,)

