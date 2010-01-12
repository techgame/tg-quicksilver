#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~ Imports 
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

import uuid
import hashlib
from datetime import datetime
from struct import pack, unpack
from .utils import OpBase

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~ Definitions 
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

class Changeset(OpBase):
    _initialized = False
    versionId = None
    parentId = None
    state = None

    mergeId = None
    mergeFlags = None

    def __init__(self, host, versionId=None):
        self.setHost(host)
        if versionId:
            self.versionId = versionId

    def __conform__(self, protocol):
        print 'conform:', protocol
        if protocol is sqlite3.PrepareProtocol:
            return int(self)

    def __int__(self):
        return self.versionId

    def init(self, versionId=None):
        if versionId is None:
            versionId = self.versionId
            if versionId is None:
                raise ValueError("Not possible to init changeset from a null versionId")

        stmt  = "select * from %(qs_changesets)s \n" % self.ns
        stmt += "  where versionId=?"
        res = self.cur.execute(stmt, (versionId,))
        print 'init:', versionId

        cols = [e[0] for e in res.description]
        ciRange = range(len(cols))
        for vals in res:
            for n,v in zip(cols, vals):
                setattr(self, n, v)
        self._initialized = True
        return self

    def _updateState(self, state):
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

    def update(self, **kw):
        cols, data = self.splitColumnData(kw, ns.changeset)
        if kw: raise ValueError("Unknown keys: %s" % (kw.keys(),))

        stmt  = "update into %(qs_changesets)s \n" % self.ns
        stmt += "  set %s \n" % (', '.join('%s=?' for c in cols),)
        stmt += "  where versionId=? "
        self.cur.execute(stmt, data + [versionId])

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def iterParentIds(self, incSelf=False):
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
    def new(klass, host):
        return klass(host)

    def newChild(self):
        child = self.new(self)
        child.createVersion(self.versionId)
        return child

    def createVersion(self, parentId, state='new'):
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
        h = hashlib.sha1()
        if parentId is None:
            parentId = uuid.getnode()

        h.update(pack('q', parentId))
        ts = datetime.now()
        h.update(ts.isoformat('T'))

        versionId, = unpack('q', h.digest()[:8])
        fullVersionId = h.hexdigest()
        return versionId, fullVersionId, ts

