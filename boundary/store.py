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
from contextlib import contextmanager

from ..mixins import NotStorableMixin
from .storeReadMixin import BoundaryStoreReadMixin
from .storeWriteMixin import BoundaryStoreWriteMixin
from . import storeEntry, oidRegistry, strategy

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~ Definitions 
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

class BoundaryStoreBase(NotStorableMixin):
    BoundaryOidRegistry = None
    BoundaryEntry = None
    BoundaryStrategy = None
    BoundaryAmbitCodec = None
    BoundaryRefWalker = None

    context = None

    def __init__(self, workspace):
        self.stateTags = set()
        if workspace is not None:
            self.initWorkspace(workspace)

    def __repr__(self):
        K = self.__class__
        return '<%s.%s open:%s>@%r' % (K.__module__, K.__name__, 
                len(self.reg), self.cs)

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    #~ Initialization
    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def init(self):
        pass

    def initStore(self):
        self.BoundaryEntry = self.BoundaryEntry.newFlyweight(self)
        self.reg = self.BoundaryOidRegistry()
        self._ambitList = []

        self._initReadStore()
        self._initWriteStore()

        self.init()
        return self

    def _initReadStore(self): 
        pass
    def _initWriteStore(self): 
        pass

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def initWorkspace(self, workspace):
        self.stateTags.add('workspace')
        self.ws = workspace
        return self.initStore()
    @classmethod
    def fromWorkspace(klass, workspace):
        self = klass(None)
        self.initWorkspace(workspace)
        return self

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def initCopier(self, bs, bsTarget=None):
        self.stateTags.add('copier')

        self.ws = bs.ws
        self.initStore()
        if bsTarget is not None:
            self.context = bsTarget.context
        else: self.context = None
        return self

    def newCopier(self, bsTarget=None):
        # bypass the standard workspace initialization and init the
        # copier from this instance, using the context of target
        self._flushForCopier()
        cbs = self.__class__(None)
        return cbs.initCopier(self, bsTarget)

    def _flushForCopier(self):
        pass

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    #~ Workspace methods
    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def getChangeset(self):
        return self.ws.cs
    cs = changeset = property(getChangeset)

    def getDBName(self):
        return self.ws.getDBName()
    dbname = property(getDBName)
    def getDBPath(self):
        return self.ws.getDBPath()
    dbpath = property(getDBPath)

    def allOids(self):
        """Returns all stored oids"""
        return self.ws.allOids()

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    #~ Utility: ambit access, events
    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    @contextmanager
    def ambit(self, entry, context=False):
        """Returns an ambit for the boundary store instance"""
        ambit, releaseAmbit = self._acquireAmbit()

        if context is False:
            context = self.context

        ambit.inBoundaryCtx(entry, context)
        yield ambit
        ambit.inBoundaryCtx(None, context)
        releaseAmbit(ambit)

    def _acquireAmbit(self):
        ambitList = self._ambitList
        if ambitList:
            ambit = ambitList.pop()
        else: 
            strategy = self.BoundaryStrategy(self)
            ambit = self.BoundaryAmbitCodec(strategy)
        return ambit, ambitList.append

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

class BoundaryStoreEncodingMixin(object):
    """Payload encoding/decoding"""

    def _encodeData(self, data):
        if len(data) >= 256:
            data = data.encode('zlib')
        return buffer(data)
    def _decodeData(self, payload):
        data = bytes(payload)
        if data.startswith('\x78\x9c'):
            # zlib encoded
            data = data.decode('zlib')
        return data

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~ The default boundary store, mixed together
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

class BoundaryStore(
        BoundaryStoreReadMixin, 
        BoundaryStoreWriteMixin,
        BoundaryStoreEncodingMixin, 
        BoundaryStoreBase, ):

    """A good default boundary store"""

    BoundaryOidRegistry     = oidRegistry.BoundaryOidRegistry
    BoundaryEntry           = storeEntry.BoundaryEntry
    BoundaryStrategy        = strategy.BoundaryStrategy
    BoundaryAmbitCodec      = strategy.BoundaryAmbitCodec
    BoundaryRefWalker       = strategy.BoundaryRefWalker

    context = None
    saveDirtyOnly = True

