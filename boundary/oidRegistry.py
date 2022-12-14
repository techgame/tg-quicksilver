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

from .strategy import _oidTypes_

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~ Definitions 
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

class BoundaryOidRegistry(object):
    db_oid = None
    db_ident = None

    def __init__(self, BoundaryEntry):
        self.BoundaryEntry = BoundaryEntry
        self.clear()

    def __nonzero__(self):
        return True
    def __len__(self):
        return len(self.db_oid)

    def allLoadedOids(self):
        return self.db_oid.keys()
    def allLoadedEntries(self):
        return self.db_oid.values()

    def clear(self):
        self.db_oid = {}
        self.db_ident = db_ident = {}
        self.db_refs = db_refs = []
        def _addOidsFor(oid, a, b):
            if a is not None: db_ident[id(a)]=oid; db_refs.append(a)
            if b is not None: db_ident[id(b)]=oid; db_refs.append(b)
        self._addOidsFor = _addOidsFor

    def alias(self, anObj, oidOrObj):
        if not isinstance(oidOrObj, _oidTypes_):
            if not isinstance(oidOrObj, self.BoundaryEntry):
                oidOrObj = self.db_ident[id(oidOrObj)]
            else: oidOrObj = oidOrObj.oid

        self.db_ident[id(anObj)] = oidOrObj
        return oidOrObj

    def add(self, entry):
        oid = entry.oid
        self.db_oid[oid] = entry
        self._addOidsFor(oid, entry.pxy, entry.obj)

    def remove(self, oid):
        entry = self.lookup(oid)
        if entry is None:
            return None

        oid = entry.oid
        entry = self.db_oid.pop(oid, None)
        self.db_ident.pop(id(entry.obj), None)
        self.db_ident.pop(id(entry.pxy), None)
        return entry

    def lookup(self, oidOrObj, default=None):
        if not isinstance(oidOrObj, _oidTypes_):
            if not isinstance(oidOrObj, self.BoundaryEntry):
                oidOrObj = self.db_ident.get(id(oidOrObj))
                if oidOrObj is None:
                    return default
            else: oidOrObj = oidOrObj.oid
        return self.db_oid.get(oidOrObj, default)

    def oidForObj(self, obj, default=None):
        return self.db_ident.get(id(obj), default)
    def get(self, obj, default=None):
        return self.db_ident.get(id(obj), default)
    def __getitem__(self, obj):
        assert not isinstance(obj, _oidTypes_), "Registry's getitem is for objects only"
        return self.db_ident[id(obj)]

