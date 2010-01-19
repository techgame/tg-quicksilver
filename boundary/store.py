#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~ Imports 
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

import os, sys
from contextlib import contextmanager
from .ambit import PickleAmbitCodec

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~ Definitions 
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

class Ambit(object):
    Codec = PickleAmbitCodec

    def __init__(self, host):
        self.host = host
        self._objCache = host._objCache
        self._codec = self.Codec(self._refForObj, self._objForRef)
        self._target = None

    def encode(self, obj):
        assert self._target is None, self._target
        self._target = obj
        data = self._codec.encode(obj)
        #data = data.encode('zlib')
        self._target = None
        return data

    def decode(self, data, meta=None):
        assert self._target is None, self._target
        #data = data.decode('zlib')
        return self._codec.decode(data, meta)

    def _refForObj(self, obj):
        if isinstance(obj, type): 
            return
        if obj is self._target:
            return 
        r = getattr(obj, '__bounded__', False)
        if not r: 
            return 

        oid = self._objCache.get(obj, None)
        if oid is None:
            oid = self.host.set(None, obj)
        return oid

    def _objForRef(self, ref):
        obj = self.host.get(ref)
        return obj

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

class Proxy(object):
    __bounded__ = True

    def __getattribute__(self, name):
        obj = object.__getattribute__(self, '_obj_')
        return getattr(obj, name)
    def __setattr__(self, name, value):
        obj = object.__getattribute__(self, '_obj_')
        setattr(obj, name, value)
    def __delattr__(self, name, value):
        obj = object.__getattribute__(self, '_obj_')
        delattr(obj, name)

class BoundaryStore(object):
    def __init__(self, workspace):
        self.init(workspace)

    def init(self, workspace):
        self.ws = workspace
        self._cache = {}
        self._objCache = {}

    @contextmanager
    def _ambit(self):
        yield Ambit(self)

    def get(self, oid, default=NotImplemented):
        cache = self._cache
        r = cache.get(oid, cache)
        if r is not cache:
            return r

        pxy = Proxy()
        cache[oid] = pxy
        with self._ambit() as ambit:
            objData = self.ws.read(oid)
            if objData is None:
                if default is NotImplemented:
                    raise LookupError("No object found at oid: %r" % (oid,), oid)
                else: return default

            obj, entry = ambit.decode(bytes(objData.payload), objData)

        object.__setattr__(pxy, '_obj_', obj)
        #cache[oid] = obj
        self._objCache[pxy] = oid
        self._objCache[obj] = oid
        return obj

    def set(self, oid, obj, **meta):
        ws = self.ws
        cache = self._cache
        if oid is None:
            oid = ws.newOid()

        pxy = Proxy()
        object.__setattr__(pxy, '_obj_', obj)

        cache[oid] = pxy
        self._objCache[pxy] = oid
        self._objCache[obj] = oid

        with self._ambit() as ambit:
            data = buffer(ambit.encode(obj))
            ws.write(oid, payload=data, **meta)

        return oid

    def delete(self, oid):
        cache = self._cache
        obj = cache.pop(oid, None)
        if obj is not None:
            self._objCache.pop(obj, None)
            obj = object.__getattribute__(obj, '_obj_')
            self._objCache.pop(obj, None)
        self.ws.remove(oid)

    def saveAll(self):
        ws = self.ws
        cache = self._cache
        with self._ambit() as ambit:
            for oid, obj in cache.items():
                obj = object.__getattribute__(obj, '_obj_')
                data = buffer(ambit.encode(obj))

                exdata = ws.read(oid).payload
                if exdata is not None:
                    self.compare(oid, data, exdata, ambit)

                ws.write(oid, payload=data)
        return ws
                    
    def commit(self, **kw):
        return self.ws.commit(**kw)

    def compare(self, oid, data, exdata, ambit):
        import pickletools

        computeHash = ambit._codec.computeHash
        print "~"*20, "compare", "~"*20
        print "NEW:"
        hd = computeHash(data)
        print 
        print "OLD:"
        hx = computeHash(exdata)
        print 

        print "comp:"
        eq = hd.digest() == hx.digest()
        print (oid, eq, data==exdata, len(data)==len(exdata))
        print '    %s/%s' % (len(data), hd.hexdigest())
        print '    %s/%s' % (len(exdata), hx.hexdigest())
        if not eq:
            with file('hash_N.hash', 'w') as fh:
                for l in hdhash.split('\n'):
                    print >>fh, l.encode('hex')

            with file('hash_X.hash', 'w') as fh:
                for l in hxhash.split('\n'):
                    print >>fh, l.encode('hex')

            os.system('diff hash_N.hash hash_X.hash > hash.diff')

            with file('N.pickle', 'w') as fh:
                fh.write(bytes(data))

            with file('X.pickle', 'w') as fh:
                fh.write(bytes(exdata))

            with file('dis_N.dis', 'w') as fh:
                pickletools.dis(bytes(data), fh)

            with file('dis_X.dis', 'w') as fh:
                pickletools.dis(bytes(exdata), fh)

            os.system('diff dis_N.dis dis_X.dis > dis.diff')

        print "~"*60
        print
