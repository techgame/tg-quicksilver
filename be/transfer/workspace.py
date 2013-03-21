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

from ..base.errors import WorkspaceError
from ..base.workspace import WorkspaceBasic
from . import metadata

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~ Workspace concept
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

class TransferWorkspace(WorkspaceBasic):
    metadataView = metadata.metadataView

    def __init__(self, ws_src, ws_dst):
        self._ws_src = ws_src
        self._ws_dst = ws_dst

    def __repr__(self):
        K = self.__class__
        return '<%s.%s>' % (K.__module__, K.__name__)

    def migrate(self):
        src = self._ws_src; dst = self._ws_dst
        self.metadataView().migrate()
        for oid in src.allOids():
            self._migrateEntry(oid, src, dst)
    def _migrateEntry(self, oid, src, dst):
        data = dict(src.read(oid))
        data.pop('oid', None)
        dst.write(oid, **data)

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def getDBName(self):
        return self._ws_src.getDBName()
    dbname = property(getDBName)
    def getDBPath(self):
        return self._ws_src.getDBPath()
    dbpath = property(getDBPath)

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    #~ OID data operations
    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def newOid(self):
        return self._ws_dst.newOid()
    def revId(self, oid):
        return self._ws_dst.revId(oid)
    def nextRevId(self, oid):
        return self._ws_dst.nextRevId(oid)
    def nextGroupId(self, grpId=False):
        return self._ws_dst.nextGroupId(grpId)

    def allOids(self):
        return self._ws_src.allOids()
    def contains(self, oid):
        return self._ws_src.contains(oid)
    def read(self, oid, cols=False):
        return self._ws_src.read(oid, cols)

    def inCommit(self):
        return self._ws_dst.inCommit()
    def commit(self, **kw):
        return self._ws_dst.commit(**kw)

    def write(self, oid, **data):
        return self._ws_dst.write(oid, **data)
    def postUpdate(self, seqId, **data):
        return self._ws_dst.postUpdate(seqId, **data)
    def postBackout(self, seqId):
        return self._ws_dst.postBackout(seqId)
    def remove(self, oid):
        return self._ws_dst.remove(oid)

    def createExternTable(self, *args, **kw):
        return self._ws_dst.createExternTable(*args, **kw)
    def createExternWSView(self, *args, **kw):
        return self._ws_dst.createExternWSView(*args, **kw)

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def workspaceView(host, wsid=None):
    return TransferWorkspace(host, wsid)
Workspace = TransferWorkspace

