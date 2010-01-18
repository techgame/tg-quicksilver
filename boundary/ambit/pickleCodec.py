#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~ Imports 
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

import pickle
from pickle import Pickler, Unpickler

try:
    import cPickle
    from cPickle import Pickler, Unpickler
except ImportError: 
    cPickle = None

from StringIO import StringIO
try:
    import cStringIO
    from cStringIO import StringIO
except ImportError: 
    cStringIO = None

from .baseCodec import BaseAmbitCodec

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~ Definitions 
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

class PickleAmbitCodec(BaseAmbitCodec):
    Pickler = Pickler
    Unpickler = Unpickler
    StringIO = StringIO
    protocol = 2

    def _initEncoder(self, idForObj=None):
        io = self.StringIO()
        p = self.Pickler(io, self.protocol)
        if idForObj is not None:
            p.persistent_id = idForObj
        self._encoding = io, p

    def _initDecoder(self, objForId=None):
        io = self.StringIO()
        up = self.Unpickler(io)
        if objForId is not None:
            up.persistent_load = objForId
        self._decoding = io, up

    def encode(self, obj, incMemo=False):
        io, p = self._encoding
        p.clear_memo()
        try:
            p.dump(obj)
            data = io.getvalue()
        finally:
            io.seek(0)
            io.truncate()

        if incMemo:
            return data, p.memo
        else: return data

    def decode(self, data, meta=None):
        io, up = self._decoding
        io.write(data)
        io.seek(0)
        try:
            obj = up.load()
        finally:
            io.seek(0)
            io.truncate()

        if meta is not None:
            return obj, meta
        else: return obj

