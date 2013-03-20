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

class MetadataView(MetadataViewBase):
    def __init__(self, host):
        self._meta_src = host._ws_src.metadataView()
        self._meta_dst = host._ws_dst.metadataView()

    def migrate(self):
        src = self._meta_src; dst = self._meta_dst
        for (n,i),v in src.iter(None):
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

def metadataView(host, key=None, **kw):
    view = MetadataView(host, **kw)
    if key is not None:
        view = view.keyView(key)
    return view

