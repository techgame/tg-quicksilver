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

import os, contextlib, itertools
from ..base.errors import WorkspaceError
from ..base.workspace import WorkspaceBasic
from . import metadata

from .monotonicIds import MonotonicUniqueId

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~ Workspace concept
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

class MonotonicOids(MonotonicUniqueId):
    digits = 2

class FlatWorkspace(WorkspaceBasic):
    metadataView = metadata.metadataView

    def __init__(self, dbFilename, db=None, db_w=None):
        self.newOid = MonotonicOids().next
        self._dbname = os.path.abspath(dbFilename)
        self.initDB(db, db_w)

    def initDB(self, db=None, db_w=None):
        if db is None: db = {}
        self._db = db
        self._entries = db.setdefault('entries', {})

        if db_w is None: db_w = {}
        self._db_w = db_w
        self._entries_w = db_w.setdefault('entries', {})

    def __repr__(self):
        K = self.__class__
        return '<%s.%s>' % (K.__module__, K.__name__)

    def getDBName(self): return self._dbname
    dbname = property(getDBName)

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    #~ OID data operations
    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def revId(self, oid): return None
    def nextRevId(self, oid): return None
    def nextGroupId(self, grpId=False): return None

    def allOids(self):
        return itertools.chain(self._entries_w.iterkeys(),
                self._entries.iterkeys())
    def contains(self, oid):
        return (oid in self._entries or oid in self._entries_w)
    def read(self, oid, cols=False):
        ans = self._entries.get(oid)
        return self._entries_w.get(oid, ans)

    def commit(self, **kw):
        self.metadataView().commit()
        self._entries.update(self._entries_w)
        self._entries_w.clear()

    def write(self, oid, **data):
        entry = self._entries.pop(oid, {})
        entry.update(data)
        self._entries_w[oid] = entry
    def postUpdate(self, seqId, **data):
        raise NotImplementedError('Not supported')
    def postBackout(self, seqId):
        raise NotImplementedError('Not supported')

    def remove(self, oid):
        ans = self._entries.pop(oid, None)
        self._entries_w.pop(oid, ans)
        return ans

    def createExternTable(self, *args, **kw):
        raise NotImplementedError('Not supported')
    def createExternWSView(self, *args, **kw):
        raise NotImplementedError('Not supported')

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def workspaceView(host, wsid=None):
    return FlatWorkspace(host, wsid)
Workspace = FlatWorkspace

