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

from TG.quicksilver.be.sqlite import Versions

from .store import BoundaryStore
from .extensions import BoundaryStats

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~ Definitions 
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

class BoundaryVersions(Versions):
    ns = Versions.ns.copy()
    ns.payload = [
        ('hash','BLOB'),
        ('typeref','TEXT'),
        ('payload','BLOB'),
        ]
    ns.changeset = [
        ('note', 'text default null'),
        ]

    def boundaryWorkspace(self, wsid=None, **kw):
        ws = self.workspace(wsid)
        return self.boundaryStoreForWS(ws, **kw)
    boundaryWS = boundaryWorkspace

    BoundaryStore = BoundaryStore
    def boundaryStoreForWS(self, ws, stats=False):
        bs = self.BoundaryStore(ws)
        if stats:
            BoundaryStats(bs)
        return bs

