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
        self._updateCurrentChangeset()
        return cs

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    #~ Title 
    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def checkCommited(self, fail=True):
        csw = self.getCSWorking(False)
        if csw is None: return True
        if not fail: return False
        raise WorkspaceError("Uncommited changeset")

    def checkout(self, cs=False):
        self.checkCommited()

        if cs is False:
            cs = self.csCheckout
        else: cs = self._asChangeset(cs)

        self.clearWorkspace()
        self.fetchVersions(cs)

        self.csCheckout = cs
        self._updateCurrentChangeset()
        return cs

    def commit(self, **kw):
        cs = self.getCSWorking(False)
        if cs is None:
            return False
        ns = self.ns

        if kw: cs.update(kw)
        cs.updateState('closed')
        self.clearLog()
        self.csWorking = None
        self.csCheckout = cs
        self._updateCurrentChangeset()

        self._dbCommit()
        return True

    def revertAll(self):
        cs = self.getCSWorking(False)
        if cs is not None:
            cs.revert()
        self.csWorking = None
        self.clearLog()
        self.clearWorkspace()
        self._updateCurrentChangeset()

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    #~ template methods to effect workspace ~~~~~~~~~~~~~

    def _updateCurrentChangeset(self):
        raise NotImplementedError('Subclass Responsibility: %r' % (self,))
    def fetchVersions(self, cs):
        raise NotImplementedError('Subclass Responsibility: %r' % (self,))
    def clearWorkspace(self):
        raise NotImplementedError('Subclass Responsibility: %r' % (self,))
    def clearLog(self):
        raise NotImplementedError('Subclass Responsibility: %r' % (self,))
    def _dbCommit(self):
        raise NotImplementedError('Subclass Responsibility: %r' % (self,))

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    #~ OID data operations
    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def read(self, oid, asNS=True):
        raise NotImplementedError('Subclass Responsibility: %r' % (self,))
    def readAll(self, asNS=True):
        raise NotImplementedError('Subclass Responsibility: %r' % (self,))
    def newOid(self):
        raise NotImplementedError('Subclass Responsibility: %r' % (self,))
    def write(self, oid, **data):
        raise NotImplementedError('Subclass Responsibility: %r' % (self,))
    def remove(self, oid):
        raise NotImplementedError('Subclass Responsibility: %r' % (self,))

