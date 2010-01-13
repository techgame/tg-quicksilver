#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~ Imports 
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

from .utils import OpBase, IndexHasher
 
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~ Definitions 
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

class Changeset(OpBase):
    _initialized = False
    versionId = None
    parentId = None
    state = None
    branch = None
    ts = None

    mergeId = None
    mergeFlags = None

    def __init__(self, host, versionId=None):
        self.setHost(host)
        if versionId:
            self.versionId = int(versionId)

    def __repr__(self):
        name = self.__class__.__name__
        if not self._initialized:
            return "<%s ref|%x>" % (name, self.versionId)
        else:
            return "<%s %s|%x at %s>" % (name, self.state, self.versionId, self.ts)

    def __cmp__(self, other): 
        return cmp(self.versionId, other.versionId)
        if isinstance(other, Changeset):
            return cmp(self.versionId, other.versionId)
        else:
            return cmp(self.versionId, other)

    def __hash__(self): return hash(self.versionId)
    def __int__(self): return self.versionId
    def __long__(self): return self.versionId
    def __index__(self): return self.versionId

    def init(self, versionId=None):
        if self._initialized:
            return self
        if versionId is None:
            versionId = self.versionId
            if versionId is None:
                raise ValueError("Not possible to init changeset from a null versionId")

        stmt  = "select * from %(qs_changesets)s \n" % self.ns
        stmt += "  where versionId=?"
        res = self.cur.execute(stmt, (versionId,))

        cols = [e[0] for e in res.description]
        ciRange = range(len(cols))
        for vals in res:
            for n,v in zip(cols, vals):
                setattr(self, n, v)
        self._initialized = True
        return self

    def isChangeset(self): return True
    def asChangeset(self): return self 

    @classmethod
    def fromArg(klass, host, arg):
        if arg is None: 
            return klass(host)

        if not isinstance(arg, (int, long)):
            asCS = getattr(arg, 'asChangeset', None)
            if asCS is not None:
                return asCS()
            else: arg = int(arg)

        return klass(host, arg)


    def isChangesetOpen(self):
        return not self.isChangesetClosed()
    def isChangesetClosed(self):
        self.init()
        return 'closed' in self.state

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def updateState(self, state):
        stmt  = "update %(qs_changesets)s \n" % self.ns
        stmt += "  set state=? where versionId=? "
        self.cur.execute(stmt, (state, self.versionId))
        self.state = state

    def merge(self, mergeId=None, mergeFlags=None):
        if mergeId is None:
            mergeId = self.mergeId
        mergeId = int(mergeId)
        if mergeFlags is None:
            mergeFlags = self.mergeFlags
        stmt  = "update %(qs_changesets)s \n" % self.ns
        stmt += "  set mergeId=?, mergeFlags=? \n"
        stmt += "  where versionId=? "
        self.cur.execute(stmt, (mergeId, mergeFlags, self.versionId))
        self.mergeId = mergeId
        self.mergeFlags = mergeFlags

    def update(self, data=None, **kw):
        if data is not None: 
            if kw:
                data = dict(data)
                data.update(kw)
        else: data = kw

        cols, data = self.splitColumnData(data, ns.changeset)
        if data: 
            raise ValueError("Unknown keys: %s" % (data.keys(),))

        stmt  = "update into %(qs_changesets)s \n" % self.ns
        stmt += "  set %s \n" % (', '.join('%s=?' for c in cols),)
        stmt += "  where versionId=? "
        self.cur.execute(stmt, data + [versionId])

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    _csParent = False
    def getCSParent(self, init=False):
        r = self._csParent
        if r is False:
            r = self.parentId
            if r is not None:
                r = self.new(self, r)
                if init: r.init()
            self._csParent = r
        return r
    csParent = property(getCSParent)

    _csMerge = None
    def getCSMerge(self, init=False):
        r = self._csMerge
        if r is False:
            r = self.mergeId
            if r is not None:
                r = self.new(self, r)
                if init: r.init()
            self._csMerge = r
        return r
    csMerge = property(getCSMerge)

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def iterLineage(self):
        return self.iterParentIds(True)
    def iterParentIds(self, incSelf=False):
        self.init()
        if incSelf:
            yield self.versionId, self.state
        vid = self.parentId

        ex = self.cur.execute
        stmt  = 'select parentId,state from %(qs_changesets)s where versionId=?'
        stmt %= self.ns
        while vid is not None:
            for r in ex(stmt, (vid,)):
                yield vid, r[1]
                vid = r[0]
                break
            else: vid = None

    def iterChildIds(self, incSelf=False):
        stmt = 'select versionId, state from %(qs_changesets)s where parentId=?'
        return self.cur.execute(stmt%self.ns, (self.versionId,))

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    #~ Changeset creation
    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    @classmethod
    def new(klass, host, versionId=None):
        return klass(host, versionId)

    def newChild(self, branch=None):
        if branch is None:
            branch = self.branch
        child = self.new(self)
        child.createVersion(self.versionId, branch)
        return child

    def createVersion(self, parentId, branch=None, state='new'):
        if parentId is not None:
            parentId = int(parentId)
        versionId, fullVersionId, ts = self.newVersionId(parentId)

        stmt  = "insert into %(qs_changesets)s \n" % self.ns
        stmt += "  (versionId, fullVersionId, parentId, state, ts) values (?,?,?,?,?)"
        self.cur.execute(stmt, (versionId, fullVersionId, parentId, state, ts))

        self.versionId = versionId
        self.fullVersionId = fullVersionId
        self.parentId = parentId
        self.state = state
        self.ts = ts
        self._initialized = True

    def newVersionId(self, parentId):
        if parentId is None:
            parentId = self.getnode()
        else: parentId = int(parentId)

        h = IndexHasher()
        h.addInt(parentId)
        h.addTimestamp()

        versionId = abs(h.asInt())
        fullVersionId = h.hexdigest()
        return versionId, fullVersionId, h.ts

