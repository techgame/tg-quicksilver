#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~ Imports 
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

from datetime import datetime
from ...mixins import NotStorableMixin
from . import utils
from .utils import identhash
from .errors import ChangesetError

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~ Definitions 
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

class ChangesetAbstract(NotStorableMixin):
    def __init__(self, host, versionId=None):
        self.setHost(host)
        if versionId:
            self.versionId = versionId

    def isChangeset(self): return True
    def asChangeset(self): return self 

    @classmethod
    def fromArg(klass, host, arg):
        if arg is None: 
            return klass(host)

        asCS = getattr(arg, 'asChangeset', None)
        if asCS is not None:
            return asCS()

        return klass(host, arg)

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def __repr__(self):
        name = self.__class__.__name__
        if not self._initialized:
            return "<%s ref|%x>" % (name, self.versionId)
        else:
            return "<%s %s|%x at %s>" % (name, self.state, self.versionId, self.ts)

    def __cmp__(self, other): 
        return cmp(self.versionId, other.versionId)

    def __hash__(self): return hash(self.versionId)
    def __index__(self): return self.versionId
    def __nonzero__(self): return bool(self.versionId)

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    #~ Initialization and status
    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    _initialized = False
    def init(self, versionId=None, force=False):
        if self._initialized and not force:
            return self

        items = self._selectEntryItems()
        return self._setState(items)

    @classmethod
    def new(klass, host, versionId=None):
        return klass(host, versionId)

    def newChild(self, branch=None):
        if branch is None:
            branch = self.branch
        child = self.new(self)
        return child.createVersion(self.versionId, branch)

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    #~ Attributes
    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    state = None
    branch = None
    ts = None
    mergeId = None
    mergeFlags = None

    _versionId = None
    def getVersionId(self):
        return self._versionId
    def setVersionId(self, versionId):
        if versionId is not None:
            versionId = self.asVersionId(versionId)
        self._versionId = versionId
    versionId = property(getVersionId, setVersionId)

    _parentId = None
    def getParentId(self):
        return self._parentId
    def setParentId(self, parentId):
        if parentId is not None:
            parentId = self.asVersionId(parentId)
        self._parentId = parentId
    parentId = property(getParentId, setParentId)

    def _setState(self, items):
        for n,v in items:
            setattr(self, n, v)

        self._initialized = True
        return self

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    #~ changeset wrappers around Ids ~~~~~~~~~~~~~~~~~~~~
    _csParent = False
    def getCSParent(self):
        r = self._csParent
        if r is False:
            r = self.parentId
            if r is not None:
                r = self.new(self, r)
            self._csParent = r
        return r
    csParent = property(getCSParent)

    _csMerge = None
    def getCSMerge(self):
        r = self._csMerge
        if r is False:
            r = self.mergeId
            if r is not None:
                r = self.new(self, r)
            self._csMerge = r
        return r
    csMerge = property(getCSMerge)

    #~ iterate over parents ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def iterLineage(self):
        return self.iterParentIds(True)
    def iterParentIds(self, incSelf=False):
        self.init()
        if incSelf:
            yield self.versionId, self.state
        verId = self.parentId

        selParent = self._bindSelectParent()
        while verId is not None:
            for parId, state in selParent(verId):
                yield verId, state
                verId = parId
                break
            else: verId = None

    #~ passive readers ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def isChangesetOpen(self):
        return not self.isChangesetClosed()
    def isChangesetClosed(self):
        self.init()
        return 'closed' in self.state

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    #~ Modification 
    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def update(self, items=None, **kw):
        items = utils.joinKWDict(items, kw)
        return self._updateEntryItems(items)

    def updateState(self, state):
        r = self._updateEntryItems(state=state)
        self.state = state
        return r

    def merge(self, mergeId=None, mergeFlags=None):
        self.inti()
        if mergeId is None:
            mergeId = self.mergeId
        mergeId = self.asVersionId(mergeId)
        if mergeFlags is None:
            mergeFlags = self.mergeFlags

        return self._updateEntryItems(mergeId=mergeId, mergeFlags=mergeFlags)

    def createVersion(self, parentId, branch=None, state='new'):
        if self.versionId is not None:
            raise ChangesetError("Already have a valid versionId")
        self._initialized = False

        ts = datetime.now()
        parentId = self.asVersionId(parentId)
        versionId, fullVersionId = self.newVersionId(parentId, ts)

        self.versionId = versionId
        items = self._insertEntryItems(
                    parentId=parentId,
                    fullVersionId=fullVersionId,
                    state=state, branch=branch, ts=ts)

        if items is None:
            return self.init(force=True)

        return self._setState(items)

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    #~ Override obligations
    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def setHost(self, host):
        raise NotImplementedError('Subclass Responsibility: %r' % (self,))
    def asVersionId(self, versionId=False):
        """Returns a backend compatible versionId"""
        raise NotImplementedError('Subclass Responsibility: %r' % (self,))
    def newVersionId(self, parentId, ts):
        """Returns a new versionId for parentId and a timestamp"""
        raise NotImplementedError('Subclass Responsibility: %r' % (self,))

    def _selectEntryItems(self, columns=None):
        """Returns a list of the form [(name, value)]"""
        raise NotImplementedError('Subclass Responsibility: %r' % (self,))
    def _insertEntryItems(self, _items_=None, **kw):
        raise NotImplementedError('Subclass Responsibility: %r' % (self,))
    def _updateEntryItems(self, _items_=None, **kw):
        raise NotImplementedError('Subclass Responsibility: %r' % (self,))

    def _bindSelectParent(self):
        """Bind fn(versionId) => [(parent versionId, state)] for the matching parents"""
        raise NotImplementedError('Subclass Responsibility: %r' % (self,))
    def _bindSelectMergeParent(self):
        """Bind fn(versionId) => [(merge parent versionId, state)] for the matching merge parents"""
        raise NotImplementedError('Subclass Responsibility: %r' % (self,))
    def _bindSelectChildren(self):
        """Bind fn(versionId) => [(child versionId, state)] for the matching children"""
        raise NotImplementedError('Subclass Responsibility: %r' % (self,))

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~ Generally usble base class
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

class ChangesetBase(ChangesetAbstract):
    VersionIdHasher = identhash.Int64IdentHasher

    def __int__(self): return self.versionId
    def __long__(self): return self.versionId

    def asVersionId(self, versionId=False):
        if versionId is False:
            versionId = self.versionId
        if versionId is not None:
            versionId = long(versionId)
        return versionId

    getnode = staticmethod(utils.getnode)
    def newVersionId(self, parentId, ts):
        """Returns a new versionId for parentId and a timestamp"""
        if parentId is None:
            parentId = self.getnode()
        h = self.VersionIdHasher()
        h.addInt(parentId)
        h.addTimestamp(ts)
        return abs(h.asInt()), h.hexdigest()

    def update(self, items=None, **kw):
        c,v = utils.splitColumnData(items, kw, self.ns.changeset)
        if kw: 
            raise ValueError("Unknown keys: %s" % (kw.keys(),))
        return self._updateEntryColumnsValues(c,v)

    def _insertEntryItems(self, _items_=None, **kw):
        columns, vals = utils.splitKeyValsEx(_items_, kw)
        self._insertEntryColumnsValues(columns, vals)
        return zip(columns, vals)
    def _updateEntryItems(self, _items_=None, **kw):
        columns, vals = utils.splitKeyValsEx(_items_, kw)
        return self._updateEntryColumnsValues(columns, vals)

    def _insertEntryColumnsValues(self, columns, values):
        """Update changeset entry from parallel arrays: (columns, values)"""
        raise NotImplementedError('Subclass Responsibility: %r' % (self,))
    def _updateEntryColumnsValues(self, columns, values):
        """Update changeset entry from parallel arrays: (columns, values)"""
        raise NotImplementedError('Subclass Responsibility: %r' % (self,))

