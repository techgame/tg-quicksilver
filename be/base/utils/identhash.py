#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~ Imports 
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

import hashlib
import struct
from datetime import datetime

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~ Definitions 
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def _fmtEntry(f):
    return f, struct.calcsize(f)

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

class IdentHasher(object):
    def __init__(self, factory=hashlib.sha1):
        self._h = factory()

    def update(self, v):
        return self._h.update(v)

    def digest(self):
        return self._h.digest()
    def hexdigest(self):
        return self._h.hexdigest()

    now = staticmethod(datetime.now)
    def addTimestamp(self, ts=None):
        if ts is None:
            ts = self.now()
        self.ts = ts
        return self.update(ts.isoformat('T'))

    def _fmtUpdate(self, f, *vals):
        if isinstance(f, tuple):
            f = f[0]
        v = struct.pack(f, *vals)
        return self.update(v)
    def _fmtUnpack(self, f, i=0):
        if not isinstance(f, tuple):
            n = struct.calcsize(f)
        else: f, n = f

        d = self.digest()
        r = struct.unpack(f, d[:n])
        if i is not None:
            r = r[i]
        return r

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

class IntIdentHasher(IdentHasher):
    _signed = _fmtEntry('i')
    _unsigned = _fmtEntry('i')

    def __int__(self): 
        return self.asInt()
    def __long__(self): 
        return self.asInt()

    def addInt(self, v): 
        return self._fmtUpdate(self._signed, v)
    def asInt(self): 
        return self._fmtUnpack(self._signed)
    def addUInt(self, v): 
        return self._fmtUpdate(self._unsigned, v)
    def asUInt(self): 
        return self._fmtUnpack(self._unsigned)

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

class Int32IdentHasher(IntIdentHasher):
    _signed = _fmtEntry('i')
    _unsigned = _fmtEntry('i')

class Int64IdentHasher(IntIdentHasher):
    _signed = _fmtEntry('q')
    _unsigned = _fmtEntry('Q')

