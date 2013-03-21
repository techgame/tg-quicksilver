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

import sys
from cPickle import Pickler, Unpickler
from cStringIO import StringIO

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~ Definitions 
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

class TargetStandin(object):
    __slots__ = []
    def __new__(klass, *args):
        return object.__new__(klass)

    def __getstate__(self): pass
    def __setstate__(self, state): pass
    def append(self, v): pass
    def extend(self, vl): pass
    def __setitem__(self, k, v): pass

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

class PickleRefWalker(object):
    Unpickler = Unpickler
    def findRefs(self, aPickleString, oidTypes):
        refs = self._loadRefs(aPickleString)
        refs = set(e for e in refs if isinstance(e, oidTypes))
        return refs

    def _loadRefs(self, aPickleString):
        fh = StringIO(aPickleString)
        refs = self._noload(fh)
        if refs is None:
            fh.seek(0)
            refs = self._load(fh)
        return refs

    def _noload(self, fh):
        refList = []
        up = self.Unpickler(fh)
        up.persistent_load = refList
        try: up.noload()
        except Exception:
            # in Python 2.6.4, we sometimes get an error
            # about NoneType not having an append method
            # --> fallback to _load
            return None
        else:
            return refList

    def _load(self, fh):
        refList = []
        up = self.Unpickler(fh)
        up.persistent_load = refList
        up.find_global = self._find_global
        #up.find_class = self._find_global

        up.load()
        return refList

    def _find_global(self, *kind):
        return TargetStandin

