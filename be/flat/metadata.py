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

import itertools
from ..base.metadata import MetadataViewBase

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~ Definitions 
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

class FlatMetadataView(MetadataViewBase):
    def __init__(self, host):
        self._meta = host._db.setdefault('meta', {})
        self._meta_w = host._db_w.setdefault('meta', {})

    def commit(self, **kw):
        self._meta.update(self._meta_w)
        if kw.get('clear', True):
            self._meta_w.clear()

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def getAt(self, name, idx=None, default=None):
        ans = self._meta.get((name,idx), default)
        return self._meta_w.get((name,idx), ans)

    def setAt(self, name, idx, value):
        self._meta.pop((name,idx), None)
        self._meta_w[name,idx] = value

    def deleteAt(self, name, idx):
        ans = self._meta.pop((name,idx), None)
        ans = self._meta_w.pop((name,idx), ans)
        return ans

    def iter(self, name=None):
        if name is None:
            return itertools.chain(self._meta.iteritems(),
                    self._meta_w.iteritems())

        byName = lambda (meta): ((i,e) for (n,i),e in meta.iteritems() if n == name)
        return itertools.chain(byName(self._meta), byName(self._meta_w))

MetadataView = FlatMetadataView
def metadataView(host, key=None, **kw):
    return FlatMetadataView(host, **kw).keyView(key)

