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

from cPickle import Pickler, Unpickler
from cStringIO import StringIO

from .pickleHash import PickleHash
from .baseCodec import BaseAmbitCodec

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~ Definitions 
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

class PickleAmbitCodec(BaseAmbitCodec):
    Pickler = Pickler
    PickleHash = PickleHash
    Unpickler = Unpickler
    StringIO = StringIO
    protocol = 2

    def _initEncoder(self, idForObj=None):
        io = self.StringIO()
        p = self.Pickler(io, self.protocol)
        if idForObj is not None:
            p.persistent_id = idForObj
        self._encoding = io, p, self._clearMemo
        self._hasher = self.PickleHash()

    def _initDecoder(self, objForId=None):
        io = self.StringIO()
        up = self.Unpickler(io)
        if objForId is not None:
            up.persistent_load = objForId
        self._decoding = io, up

    _clearMemo = True
    def getClearMemo(self):
        return self._encoding[-1]
    def setClearMemo(self, clearMemo):
        io, p, _ = self._encoding
        self._encoding = io, p, clearMemo
    clearMemo = property(getClearMemo, setClearMemo)

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def dump(self, obj):
        io, p, clear = self._encoding
        p.clear_memo()
        try:
            p.dump(obj)
            data = io.getvalue()
        finally:
            io.seek(0)
            io.truncate()
            if clear:
                p.clear_memo()
        return data

    def load(self, data):
        io, up = self._decoding
        io.write(data)
        io.seek(0)
        try:
            obj = up.load()
        finally:
            io.seek(0)
            io.truncate()
        return obj

    def hashDigest(self, data):
        h = self._hasher.hashs(data)
        return h.digest()

