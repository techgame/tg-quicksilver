#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~ Imports 
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

from .utils import OpBase

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~ Definitions 
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

class Changeset(OpBase):
    versionId = None
    parentId = None
    state = None

    mergeId = None
    mergeState = 0

    def initFrom(self, versionId):
        stmt  = "select * from %(qs_changesets)s \n" % self.ns
        stmt += "  where versionId=?"
        res = self.cur.execute(stmt, (versionId,))
        for e in res:
            print e

    def _createVersion(self, state='new'):
        stmt  = "insert into %(qs_changesets)s \n" % self.ns
        stmt += "  (versionId, parentId, state) values (?,?,?)"
        self.cur.execute(stmt, (self.versionId, self.parentId, state))
        self.state = state

    def _updateState(self, state):
        stmt  = "update into %(qs_changesets)s \n" % self.ns
        stmt += "  set state=? where versionId=? "
        self.cur.execute(stmt, (state, self.versionId))
        self.state = state

    def _updateMerge(self):
        stmt  = "update into %(qs_changesets)s \n" % self.ns
        stmt += "  set mergeId=?, mergeState=? \n"
        stmt += "  where versionId=? "
        self.cur.execute(stmt, (self.mergeId, self.mergeState, self.versionId))

    def update(self, **kw):
        cols, data = self.splitColumnData(kw, ns.changeset)
        if kw: raise ValueError("Unknown keys: %s" % (kw.keys(),))

        stmt  = "update into %(qs_changesets)s \n" % self.ns
        stmt += "  set %s \n" % (', '.join('%s=?' for c in cols),)
        stmt += "  where versionId=? "
        self.cur.execute(stmt, data + [versionId])

