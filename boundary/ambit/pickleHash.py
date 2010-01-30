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

import hashlib
from cPickle import Pickler, Unpickler
from cStringIO import StringIO

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~ Definitions 
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def register(aKey, regMap, wrap=staticmethod):
    def binder(fn):
        regMap[aKey] = fn
        return wrap(fn)
    return binder

class CanonicalForm(object):
    """Transform all containers into a stable ordered lists, creating a
    canonical form for pickled objects"""

    def __call__(self, item):
        fn = self.sanMap.get(type(item))
        if fn is not None:
            q = []; p = []

            item = fn(item, q, p)
            self.process(q, p)
        return item

    def process(self, q, p):
        sanMap = self.sanMap

        # keep references to visited objects
        visited = dict() 
        while q:
            v = q.pop()
            if id(v) in visited:
                continue

            visited[id(v)] = v
            for i in xrange(len(v)):
                e = v[i]
                fn = sanMap.get(type(e))
                if fn is not None:
                    v[i] = fn(e, q, p)
            e = None
        del visited

        # now perform post-ops, like sorting the dictionaries-turned-lists
        for fn in p: 
            fn()
        del p[:]

    sanMap = {}

    @register(tuple, sanMap)
    def _sanitizeTuple(v, q, p):
        v = list(v)
        q.append(v)
        return v
    @register(dict, sanMap)
    def _sanitizeDict(v, q, p):
        v = map(list, v.items())
        q.extend(v)
        p.append(v.sort)
        return v
    @register(list, sanMap)
    def _sanitizeList(v, q, p):
        q.append(v)
        return v

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

class CanonicalObject(object):
    __slots__ = ['n', 'cv']
    counter = 100
    canonical = CanonicalForm()

    def __new__(klass, *args):
        self = object.__new__(CanonicalObject)
        self.cv = cv = {0: klass.kind}
        if args:
            cv[1] = args

        n = CanonicalObject.counter + 1
        CanonicalObject.counter = n
        self.n = n
        return self

    def __cmp__(self, other):
        if isinstance(other, CanonicalObject):
            return cmp(self.n, other.n)
        return cmp(self.n, other)
    def __getstate__(self):
        return self.cv
    def __setstate__(self, state):
        cv = self.cv
        cv[2] = state
        cv = cv.items()
        cv.sort()
        cv = [e[1] for e in cv]

        self.cv = self.canonical(cv)

    def append(self, v):
        l = self.cv.get(3)
        if l is None:
            self.cv[3] = l = []
        l.append(v)
    def extend(self, vl):
        l = self.cv.get(3)
        if l is None:
            self.cv[3] = l = []
        l.extend(vl)
    def __setitem__(self, k, v):
        d = self.cv.get(4)
        if d is None:
            self.cv[4] = d = {}
        d[k] = v

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

class FullCanonicalForm(CanonicalForm):
    sanMap = CanonicalForm.sanMap.copy()

    @register(CanonicalObject, sanMap)
    def _sanitizeObject(v, q, p):
        s = v.__getstate__()
        q.append(s)
        return s

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

class PickleHash(object):
    hasher = hashlib.md5
    def __init__(self):
        self.gcache = {}

    def hashs(self, aPickleString):
        return self.hash(StringIO(aPickleString))

    def debug(self, aPickleString):
        obj = self._load(StringIO(aPickleString))
        raw = self._dump(obj)
        z = self.hasher(raw)
        r = FullCanonicalForm()(obj)
        return r, raw, z

    def hash(self, fh):
        z = self._dump(self._load(fh))
        z = self.hasher(z)
        return z

    Pickler = Pickler
    def _dump(self, obj):
        out = StringIO()
        p = Pickler(out, -1)
        p.persistent_id = self._persistent_id
        p.dump(obj)
        return out.getvalue()

    Unpickler = Unpickler
    def _load(self, fh):
        CanonicalObject.counter = 100
        up = self.Unpickler(fh)
        up.find_global = self._find_global
        #up.find_class = self._find_global
        up.persistent_load = self._persistent_load
        return up.load()

    def _find_global(self, *kind):
        gcache = self.gcache
        klass = gcache.get(kind, None)
        if klass is None:
            klass = type('T/'+'/'.join(kind), (CanonicalObject,), dict(kind=kind))
            gcache[kind] = klass
        return klass

    def _persistent_load(self, ref):
        return ('ref', ref)

    def _persistent_id(self, obj):
        if isinstance(obj, type):
            return '%s:%s' % (obj.__module__, obj.__name__)

