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

import os
import contextlib
from .errors import WorkspaceError
from ...mixins import NotStorableMixin

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~ Definitions 
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

class WorkspaceBase(NotStorableMixin):
    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    #~ Checkout and Working Changesets
    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def _asChangeset(self, cs=False):
        if cs is False:
            return self.cs
        raise NotImplementedError('Subclass Responsibility: %r' % (self,))

    csCheckout = None

    def isMutable(self):
        return True

    _csWorking = None
    def getCSWorking(self, create=True):
        cs = self._csWorking
        if cs is None and create:
            cs = self._createWorkingChangeset()
            self._csWorking = cs
        return cs
    def setCSWorking(self, cs):
        self._csWorking = cs
    csWorking = property(getCSWorking, setCSWorking)

    def getCS(self):
        r = self._csWorking 
        if r is None:
            r = self.csCheckout
        return r
    def setCS(self, cs):
        self.checkCommited()
        if self.csCheckout is not None:
            self.clearWorkspace()
        cs = self._asChangeset(cs)
        self.csCheckout = cs
    cs = property(getCS, setCS)

    def _createWorkingChangeset(self):
        csParent = self.csCheckout
        if csParent is None:
            csParent = self._asChangeset(None)

        cs = csParent.newChild()
        self._csWorking = cs
        self._updateCurrentChangeset(cs)
        return cs

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    #~ Workspace Operations 
    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def checkCommited(self, fail=True):
        csw = self.getCSWorking(False)
        if csw is None: 
            if self.lastChangeLogId() is None:
                return True
            elif fail:
                raise WorkspaceError("Uncommited workspace log items")
            else: return False

        if fail: 
            raise WorkspaceError("Uncommited changeset")
        return False

    def checkout(self, cs=False):
        self.checkCommited()

        if cs is False:
            cs = self.csCheckout
        else: cs = self._asChangeset(cs)

        self.clearWorkspace()
        self.fetchVersions(cs)

        self.csCheckout = cs
        self._updateCurrentChangeset(cs)
        return cs

    def commit(self, **kw):
        cs = self.getCSWorking(False)
        if cs is None:
            return False
        ns = self.ns

        if kw: cs.update(kw)

        with self.inCommit():
            self.publishChanges()
            self.clearChangeLog()
            cs.closeChangeset()
            self.csWorking = None
            self.csCheckout = cs
            self._updateCurrentChangeset(cs)

        return True

    def revertAll(self):
        cs = self.getCSWorking(False)
        self._revertChanges()
        if cs is not None:
            cs.revert()
        self.csWorking = None
        self._updateCurrentChangeset(self.cs)

    def _revertChanges(self):
        self.clearChangeLog()
        self.clearWorkspace()

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    #~ template methods to effect workspace ~~~~~~~~~~~~~

    def fetchVersions(self, cs):
        raise NotImplementedError('Subclass Responsibility: %r' % (self,))
    def markCheckout(self):
        raise NotImplementedError('Subclass Responsibility: %r' % (self,))

    def clearWorkspace(self):
        raise NotImplementedError('Subclass Responsibility: %r' % (self,))
    def publishChanges(self):
        raise NotImplementedError('Subclass Responsibility: %r' % (self,))

    def clearChangeLog(self):
        raise NotImplementedError('Subclass Responsibility: %r' % (self,))
    def lastChangeLogId(self):
        raise NotImplementedError('Subclass Responsibility: %r' % (self,))

    def _updateCurrentChangeset(self, cs):
        raise NotImplementedError('Subclass Responsibility: %r' % (self,))

    @contextlib.contextmanager
    def inCommit(self):
        raise NotImplementedError('Subclass Responsibility: %r' % (self,))

    def getDBName(self):
        raise NotImplementedError('Subclass Responsibility: %r' % (self,))
    dbname = property(lambda self: self.getDBName())
    def getDBPath(self):
        return os.path.dirname(self.getDBName())
    dbpath = property(getDBPath)

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    #~ OID data operations
    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def nextGroupId(self, grpId=False):
        raise NotImplementedError('Subclass Responsibility: %r' % (self,))
    def newOid(self):
        raise NotImplementedError('Subclass Responsibility: %r' % (self,))

    def allOids(self):
        raise NotImplementedError('Subclass Responsibility: %r' % (self,))
    def contains(self, oid):
        raise NotImplementedError('Subclass Responsibility: %r' % (self,))
    def read(self, oid, asNS=True):
        raise NotImplementedError('Subclass Responsibility: %r' % (self,))
    def write(self, oid, **data):
        raise NotImplementedError('Subclass Responsibility: %r' % (self,))
    def remove(self, oid):
        raise NotImplementedError('Subclass Responsibility: %r' % (self,))

    def postUpdate(self, seqId, **data):
        raise NotImplementedError('Subclass Responsibility: %r' % (self,))
    def postBackout(self, seqId):
        raise NotImplementedError('Subclass Responsibility: %r' % (self,))

