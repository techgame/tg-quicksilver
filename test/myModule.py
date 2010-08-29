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

import random
from TG.quicksilver.boundary.ambit import PickleAmbitCodec

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~ Definitions 
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

class A(object): 
    def __cmp__(self, other):
        c = cmp(self.idx, other.idx)
        if c: return c
        c = cmp(type(self), type(other))
        if c: return c
        c = cmp(type(self.prev), type(other.prev))
        if c: return c
        c = cmp(type(self.post), type(other.post))
        if c: return c
        c = cmp(len(self.before), len(other.before))
        if c: return c
        c = cmp(len(self.after), len(other.after))
        if c: return c
        return 0

    def answer(self):
        return 42

class B(A): 
    def _boundary_(self, bndS, bndCtx):
        return False

class C(B): 
    def _boundary_(self, bndS, bndCtx):
        return True

class Root(B):
    __bounded__ = True

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def connect(l):
    for i, e in enumerate(l):
        e.before = l[0:i]
        e.after = l[i:]
        e.idx = i
        e.prev = l[(i-1)%len(l)]
        e.post = l[(i+1)%len(l)]
        e.data = []

def genList(n, kl=[A,B,C]):
    l = []
    for x in xrange(n):
        klass = random.choice(kl)
        l.append(klass())
    connect(l)
    return l

#myData = [C(), B(), A(), A(), B(), C()]
myData = [C(), B(), A(), C(), A(), B(), C()]
connect(myData)

class DemoCodec(PickleAmbitCodec):
    class DemoStrategy(object):
        def __init__(self):
            self._map = {}

        def inBoundaryCtx(self, entry, bndCtx):
            pass

        def refForObj(self, obj):
            if getattr(obj, '__bounded__', None):
                self._map[id(obj)] = obj
                return id(obj)

        def objForRef(self, ref):
            return self._map[anId]

    def __init__(self):
        strategy = self.DemoStrategy()
        super(DemoCodec, self).__init__(strategy)

