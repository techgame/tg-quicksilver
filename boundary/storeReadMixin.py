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
from .errors import OidLookupError
from .strategy import _oidTypes_

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~ Definitions 
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

class BoundaryStoreReadMixin(object):
    def _initReadStore(self): 
        if self.isCopier():
            self._clearDeferredProxies()

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    #~ Read Access: ref, get, raw
    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def oidForObj(self, obj):
        entry = self.reg.lookup(obj)
        if entry is not None:
            return entry.oid

    def refEntry(self, oid):
        """Returns an entry for the object stored at oid"""
        entry = self.reg.lookup(oid)
        if entry is not None:
            if entry.obj is None:
                self._addDeferredRef(entry)
            return entry

        entry = self.BoundaryEntry(oid)
        if self._hasEntry(entry):
            return entry
        else: return None

    def ref(self, oid):
        """Returns a deferred proxied to the object stored at oid"""
        entry = self.refEntry(oid)
        if entry is None:
            return None
        return entry.pxy

    def __getitem__(self, oid):
        return self.get(oid)

    def iterAllEntries(self):
        for oid in self.allOids():
            entry = self.refEntry(oid)
            if entry is not None:
                yield entry

    def iterAllObjects(self):
        sentinal = object()
        for oid in self.allOids():
            obj = self.get(oid, sentinal)
            if obj is not sentinal:
                yield obj

    def get(self, oid, default=NotImplemented):
        """Returns a transparent proxied to object stored at oid"""
        entry = self.reg.lookup(oid)

        if entry is None:
            entry = self.BoundaryEntry(oid)
        elif entry.obj is not None:
            # otherwise we need to load the proxy
            return entry.pxy

        if self._readEntry(entry):
            return entry.pxy
        elif default is NotImplemented:
            raise OidLookupError("No object found for oid: %r" % (oid,), oid)
        return default

    def syncUpdate(self, syncOp, oid, rec, rec_prev):
        """Re-read the entry from the workspace, if loaded"""
        entry = self.reg.lookup(oid)
        if entry is not None:
            return self._readEntryEx(entry, rec, rec_prev, syncOp)
    def syncUpdateMap(self, syncOp, oidEntries):
        """Re-read each entry from the workspace, if loaded"""
        for oid, (rec, rec_prev) in oidEntries.iteritems():
            self.syncUpdate(syncOp, oid, rec, rec_prev)
    def bindSyncUpdateMap(self, syncOp):
        def doSyncUpdateMap(oidEntries):
            self.syncUpdateMap(syncOp, oidEntries)
        return doSyncUpdateMap

    def raw(self, oidOrObj, decode=True):
        """Returns the raw database record for the oidOrObj"""
        entry = self.reg.lookup(oidOrObj)

        if entry is None: 
            oid = oidOrObj
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

    def findRevId(self, oid):
        return self.ws.revId(oid)

    def findRefsFrom(self, oidOrObj):
        """Returns a set of oids referenced by oidOrObj"""
        data = self.raw(oidOrObj, True)
        if data is not None:
            refs = self.BoundaryRefWalker().findRefs(data['payload'], _oidTypes_)
        else: refs = []
        return refs

    def copy(self, oidOrObj, bsTarget=None):
        entry = self.reg.lookup(oidOrObj)

        self = self.newCopier(bsTarget)

        # clear any refernces from initialization
        self._clearDeferredProxies()

        with self.inContextForCopy(bsTarget):
            cpyObj = self.ref(entry.oid)

            # load all referenced objects, so that they are in
            # memory when the boundary store goes out of memory
            self._loadDeferredProxies()

        return cpyObj

    def inContextForCopy(self, bsTarget):
        return self.BoundaryEntry.inContextForCopy(self, bsTarget)
        
    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    #~ Workspace methods: commit, saveAll
    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def allLoadedOids(self):
        """Returns all currently loaded oids"""
        return self.reg.allLoadedOids()

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    #~ Utility: Entry read workhorses
    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def _hasEntry(self, entry):
        ref = self.ws.contains(entry.oid)
        if not ref:
            return False

        rec = self.ws.read(ref, 'typeref, entryMeta')
        entry.setMeta(rec['entryMeta'])

        try:
            with self.ambit(entry) as ambit:
                self.reg.add(entry)
                try:
                    typeref = ambit.decodeTyperef(rec['typeref'])
                except Exception, err:
                    r = entry.decodeFailure(err, rec['typeref'])
                    if r is None: raise
                    return r
                else:
                    entry.setDeferred(typeref)

        except Exception, exc:
            if entry.onHasEntryError(exc, rec['typeref']):
                raise
            return False

        else:
            self._addDeferredRef(entry)
            return True

    def _readEntry(self, entry):
        rec = self.ws.read(entry.oid)
        return self._readEntryEx(entry, rec)

    def _readEntryEx(self, entry, rec, rec_prev=None, syncOp=None):
        if rec is None:
            return False

        entry.setMeta(rec['entryMeta'])
        payload = rec['payload']
        if payload is None:
            return False

        try:
            with self.ambit(entry) as ambit:
                self.reg.add(entry)
                payload = self._decodeData(payload)
                try: obj = ambit.load(payload)
                except Exception, err:
                    r = entry.decodeFailure(err, rec['typeref'])
                    if r is None: raise
                    return r

                if syncOp is None:
                    hash = rec['hash']
                    if hash: hash = bytes(hash)
                    entry.setup(obj, hash)
                else:
                    if rec_prev is None: obj_prev = None
                    elif rec_prev['hash'] != rec['hash']:
                        payload_prev = self._decodeData(rec_prev['payload'])
                        try: obj_prev = ambit.load(payload_prev)
                        except Exception, err: obj_prev = None
                        self._onRead(rec_prev, payload_prev, entry)
                    else: obj_prev = obj
                    entry.syncUpdate(syncOp, obj, obj_prev)

                self._onRead(rec, payload, entry)
                self.reg.add(entry)
        except Exception, exc:
            if entry.onReadEntryError(exc, rec['typeref']):
                raise
            return False

        return True

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    #~ deferred reference handling ~~~~~~~~~~~~~~~~~~~~~~

    def _addDeferredRef(self, entry):
        dl = self._deferredReadEntries
        if dl is not None:
            dl.add(entry)

    _deferredReadEntries = None
    def _clearDeferredProxies(self):
        self._deferredReadEntries = set()

    def _loadDeferredProxies(self):
        dl = self._deferredReadEntries
        while dl:
            self._deferredReadEntries = dlNext = set()
            for e in dl:
                e.fetchObject()
            dl = dlNext

    def _iterDeferredProxies(self):
        dl = self._deferredReadEntries
        while dl:
            self._deferredReadEntries = dlNext = set()
            yield dl
            dl = dlNext

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def _onRead(self, rec, data, entry): 
        pass
    def _onReadRaw(self, rec, data):
        pass

    def _onReadError(self, entry, exc, sz_typeref=None):
        sys.excepthook(*sys.exc_info())
        print >> sys.stderr, 'entry %r, typeref %r' % (entry, sz_typeref)

