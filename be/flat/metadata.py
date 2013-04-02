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

class FlatMetadataView(MetadataViewBase):
    def __init__(self, host):
        self._meta = host._db.setdefault('meta', {})
        self._meta_w = host._db_w.setdefault('meta', {})

    def commit(self, **kw):
        for k,v in self._meta_w.iteritems():
            self._meta.setdefault(k,{}).update(v)
        if kw.get('clear', True):
            self._meta_w.clear()

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def getAt(self, name, idx=None, default=None):
        idx = idx or None
        ans = self._meta.get(name,{}).get(idx, default)
        return self._meta_w.get(name, {}).get(idx, ans)

    def setAt(self, name, idx, value):
        idx = idx or None
        self._meta.get(name,{}).pop(idx, None)
        self._meta_w.setdefault(name,{})[idx] = value

    def deleteAt(self, name, idx):
        idx = idx or None
        ans = self._meta.get(name,{}).pop(idx, None)
        ans = self._meta_w.get(name,{}).pop(idx, ans)
        return ans

    def iter(self, name=None):
        def iterAt(name):
            ns = dict()
            ns.update(self._meta.get(name, ()))
            ns.update(self._meta_w.get(name, ()))
            return ns.iteritems()

        if name is None:
            allNames = set(self._meta.iterkeys())
            allNames.update(self._meta_w.iterkeys())
            for name in allNames:
                for k,v in iterAt(name):
                    yield k,v
        else:
            for k,v in iterAt(name):
                yield k,v

MetadataView = FlatMetadataView
def metadataView(host, key=None, **kw):
    return FlatMetadataView(host, **kw).keyView(key)

