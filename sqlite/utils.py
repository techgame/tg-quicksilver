#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~ Imports 
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

import uuid
import hashlib
import struct
from datetime import datetime

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~ Definitions 
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

getnode = uuid.getnode


class HostDataView(object):
    def __init__(self, host):
        self.setHost(host)

    def setHost(self, host):
        self.cur = host.cur
        self.ns = host.ns

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

class Namespace(object):
    def __init__(self, ns=None):
        if ns is not None:
            self.__dict__ = ns

    def __repr__(self):
        return '<ns %r>' % (self.__dict__,)

    @classmethod
    def new(klass, ns=None):
        return klass(ns)
    def copy(self):
        return self.new(self.__dict__.copy())
    def update(self, *args, **kw):
        return self.__dict__.update(*args, **kw)
    def branch(self, *args, **kw):
        d = self.__dict__.copy()
        d.update(*args, **kw)
        return self.new(d)

    def __len__(self): return len(self.__dict__)
    def __iter__(self): return self.__dict__.iteritems()
    def __contains__(self, key): return key in self.__dict__
    def __getitem__(self, key): return self.__dict__[key]
    def __setitem__(self, key, value): self.__dict__[key] = value
    def __delitem__(self, key): del self.__dict__[key]

    def debug(self):
        print
        for i, e in enumerate(self):
            print (i,e)
        print

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def splitColumnData(kwData, columns):
    cols = []; values = []
    for c in columns:
        v = kwData.pop(c, values)
        if v is not values:
            cols.append(c)
            values.append(v)
    return cols, values

class OpBase(object):
    splitColumnData = staticmethod(splitColumnData)
    getnode = staticmethod(getnode)

    def setHost(self, host):
        self.cur = host.cur
        self.ns = host.ns

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

class QuicksilverError(Exception):
    pass

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

class IndexHasher(object):
    def __init__(self, factory=hashlib.sha1):
        self._h = factory()

    def update(self, v):
        return self._h.update(v)

    def digest(self):
        return self._h.digest()
    def hexdigest(self):
        return self._h.hexdigest()

    def addInt(self, v):
        v = self.pack('q', v)
        return self.update(v)
    def addUInt(self, v):
        v = self.pack('Q', v)
        return self.update(v)
    def addTimestamp(self, ts=None):
        if ts is None:
            ts = self.now()
        self.ts = ts
        return self.update(ts.isoformat('T'))

    def asInt(self):
        v = self._h.digest()[:8]
        return self.unpack('q', v)[0]
    def asUInt(self):
        v = self._h.digest()[:8]
        return self.unpack('Q', v)[0]

    def __int__(self): return self.asInt()
    def __long__(self): return self.asInt()

    pack = staticmethod(struct.pack)
    unpack = staticmethod(struct.unpack)
    now = staticmethod(datetime.now)

