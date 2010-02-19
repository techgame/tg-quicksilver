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

    def ref(self, oid):
        """Returns a deferred proxied to the object stored at oid"""
        entry = self.reg.lookup(oid)
        if entry is not None:
            if entry.obj is None:
                self._addDeferredRef(entry)
            return entry.pxy

        entry = self.BoundaryEntry(oid)
        if self._hasEntry(entry):
            return entry.pxy
        else: return None

    def __getitem__(self, oid):
        return self.get(oid)

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

    def copy(self, oidOrObj, bsTarget=None):
        entry = self.reg.lookup(oidOrObj)

        self = self.newCopier(bsTarget)

        # clear any refernces from initialization
        self._clearDeferredProxies()

        cpyObj = self.get(entry.oid)

        # load all referenced objects, so that they are in
        # memory when the boundary store goes out of memory
        self._loadDeferredProxies()

        return cpyObj

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    #~ Workspace methods: commit, saveAll
    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def allLoadedOids(self):
        """Returns all currently loaded oids"""
        return self.reg.allLoadedOids()

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    #~ Utility: Entry read workhorses
    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def _hasEntry(self, entry, context=False):
        ref = self.ws.contains(entry.oid)
        if not ref:
            return False

        rec = self.ws.read(ref, 'typeref')
        with self.ambit(entry, context) as ambit:
            self.reg.add(entry)
            typeref = ambit.decodeTyperef(rec['typeref'])
            entry.setDeferred(typeref)

        self._addDeferredRef(entry)
        return True

    def _readEntry(self, entry, context=False):
        rec = self.ws.read(entry.oid)
        if rec is None:
            return False
        data = rec['payload']
        if data is None:
            return False

        with self.ambit(entry, context) as ambit:
            self.reg.add(entry)
            data = self._decodeData(data)
            obj = ambit.load(data)
            hash = rec['hash']
            if hash: hash = bytes(hash)
            entry.setup(obj, hash)
            self._onRead(rec, data, entry)
            self.reg.add(entry)
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

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def _onRead(self, rec, data, entry): 
        pass
    def _onReadRaw(self, rec, data):
        pass

