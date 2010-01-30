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

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~ Definitions 
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

