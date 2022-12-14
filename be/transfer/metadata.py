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

from ..base.metadata import MetadataViewBase

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~ Definitions 
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

class TransferMetadataView(MetadataViewBase):
    def __init__(self, host):
        self._meta_src = host._ws_src.metadataView()
        self._meta_dst = host._ws_dst.metadataView()

    def migrate(self):
        src = self._meta_src; dst = self._meta_dst
        for (n,i),v in src.iter(None):
            self._migrateEntry(n,i,v, dst)

    def _migrateEntry(self,n,i,v,dst):
        dst.setAt(n,i,v)

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def getAt(self, name, idx=None, default=None):
        return self._meta_src.getAt(name, idx, default)
    def setAt(self, name, idx, value):
        return self._meta_dst.getAt(name, idx, value)
    def deleteAt(self, name, idx):
        return self._meta_dst.deleteAt(name, idx)
    def iter(self, name=None):
        return self._meta_src.iter(name)

MetadataView = TransferMetadataView
def metadataView(host, key=None, **kw):
    return TransferMetadataView(host, **kw).keyView(key)

