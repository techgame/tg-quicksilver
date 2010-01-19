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

import hashlib
import pickletools
from .pickleHash import HashUnpickler
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

        #self.computeHash(data)
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

    def computeHash(self, data):
        hup = HashUnpickler(StringIO(data))
        raw = hup.load()
        h = hashlib.md5(raw)
        return h, raw

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

code2op = pickletools.code2op
readArgOp = {
    -2: lambda p, fn=pickletools.read_uint1: p.read(fn(p)),
    -3: lambda p, fn=pickletools.read_int4: p.read(fn(p)),
    }

def genops(pickle):
    if isinstance(pickle, str):
        pickle = StringIO(pickle)

    if hasattr(pickle, "tell"):
        getpos = pickle.tell
    else:
        getpos = lambda: None

    while True:
        code = pickle.read(1)
        opcode = code2op.get(code)
        if opcode is None:
            if code == "":
                raise ValueError("pickle exhausted before seeing STOP")
            else:
                pos = getpos()
                raise ValueError("at position %s, opcode %r unknown" % (
                                 pos is None and "<unknown>" or pos-1,
                                 code))

        arg = opcode.arg
        if arg is not None:
            n = arg.n
            if n > 0:
                raw = pickle.read(n)
            elif n < -1:
                raw = readArgOp[n](pickle)
            else:
                raw = arg.reader(pickle)
        else: raw = None

        yield opcode, raw
        if code == '.':
            break

PickleAmbitCodec._genops = staticmethod(genops)

