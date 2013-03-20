# -*- coding: utf-8 -*- vim: set ts=4 sw=4 expandtab
##~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~##
##~ Copyright (C) 2002-2013  TechGame Networks, LLC.              ##
##~                                                               ##
##~ This library is free software; you can redistribute it        ##
##~ and/or modify it under the terms of the MIT style License as  ##
##~ found in the LICENSE file included with this distribution.    ##
##~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~##

import os, time

def bindTableEncode(table):
    mod = len(table)
    def lut_encode(v, z=0, s=''):
        if v is not None:
          while 1:
            v,k = divmod(v,mod)
            s = table[k]+s
            if not v: return s.zfill(z)
    return lut_encode

class MonotonicUniqueId(object):
    table = '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ_abcdefghijklmnopqrstuvwxyz~'
    epoch = min(time.mktime((2013,1,1,0,0,0,0,0,0)), time.time())

    prefix = ''
    digits = 2
    randDigits = 2
    urandom = staticmethod(os.urandom)

    def __init__(self, digits=None, ts_scale=None, prefix=None):
        if digits is not None:
            self.digits = int(digits)
        if ts_scale is not None:
            self.ts_scale = ts_scale/1
        if prefix is not None:
            self.prefix = str(prefix)
        self.encode = bindTableEncode(self.table)

    n = trn = None
    def init(self):
        while 1:
            trn = self.prefix+self.timestamp()
            if trn<=self.trn:
                time.sleep(0)
            else: break
        self.trn = trn+self.rand()
        self.n = 0
        return 0

    ts_scale = 1000
    def timestamp(self, encode=True):
        ts = int((time.time()-self.epoch)*self.ts_scale)
        return encode and self.encode(ts,0) or ts
    def rand(self, encode=True):
        w = self.randDigits
        r = reduce(lambda l,r:(l<<8)|ord(r), self.urandom(w), 0)
        return encode and self.encode(r,w)[:w] or r

    def __iter__(self): return self
    def iter(self): return iter(self)
    def next(self):
        n = self.n; w = self.digits
        s = self.encode(n, w)
        if s is None or w<len(s):
            n = self.init()
            s = self.encode(n, w)
        self.n = n+1
        return self.trn+s

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

if __name__=='__main__':
    from collections import defaultdict
    mid = MonotonicUniqueId(2)
    w = -mid.digits
    m = defaultdict(list)
    v = None
    for i in xrange(100000):
        v0=v; v=mid.next()
        assert v>v0, (i,v,v0)
        assert v0 is None or len(v)==len(v0), (i,len(v),len(v0))
        m[v[:w]].append(v[w:])

    for k,v in sorted(m.items()):
        print k, len(v)

