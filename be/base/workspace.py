#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~ Imports 
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

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
        self.publishChanges()
        self.clearChangeLog()
        cs.updateState('closed', remove='open')
        self.csWorking = None
        self.csCheckout = cs
        self._updateCurrentChangeset(cs)

        self._dbCommit()
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
    def _dbCommit(self):
        raise NotImplementedError('Subclass Responsibility: %r' % (self,))

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    #~ OID data operations
    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def nextGroupId(self, grpId=False):
        raise NotImplementedError('Subclass Responsibility: %r' % (self,))
    def newOid(self):
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

