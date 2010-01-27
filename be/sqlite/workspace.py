#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~ Imports 
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

import sqlite3

from ..base.errors import WorkspaceError
from ..base.workspace import WorkspaceBase
from . import metadata
from . import workspaceSchema
from .changeset import Changeset

from . import workspaceChangeOps as Ops

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~ Workspace concept
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

class Workspace(WorkspaceBase):
    Schema = workspaceSchema.WorkspaceSchema
    metadataView = metadata.metadataView
    temporary = False

    def __init__(self, host, wsid='main'):
        self.setHost(host)
        self.wsid = wsid
        self._initSchema()
        self._initStoredCS()

    def setHost(self, host):
        self.ns = host.ns.copy()
        self.conn = host.conn
        self.cur = host.cur

    def _asChangeset(self, cs=False):
        if cs is False:
            return self.cs
        return Changeset.fromArg(self, cs)

    def _initSchema(self):
        schema = self.Schema(self.ns, self)
        schema.initStore(self.conn)

    def _initStoredCS(self):
        cs = self.readCurrentChangeset()
        if cs is None:
            return

        cs.init()
        if cs.isChangesetClosed():
            self.csCheckout = cs
            self.csWorking = None
        else:
            self.csCheckout = cs.csParent
            self.csWorking = cs

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def readCurrentChangeset(self):
        wsMeta = self.metadataView("workspaces")
        cs = wsMeta.getAt(self.wsid, None)
        if cs is not None:
            return self._asChangeset(cs)
    def saveCurrentChangeset(self, cs=False, force=False):
        if cs is False:
            cs = self.cs
        if self.temporary and not force:
            return False

        wsMeta = self.metadataView("workspaces")
        if cs is not None:
            wsMeta[self.wsid] = long(cs)
        else: del wsMeta[self.wsid]
        return True

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    #~ overridden template methods ~~~~~~~~~~~~~~~~~~~~~~

    def _updateCurrentChangeset(self, cs):
        r = self.saveCurrentChangeset(cs)
        self.tagRevIds()
        return r

    def _iterLineageToState(self, cs, state='mark'):
        if cs is None: 
            cs = self.cs
        for v,s in cs.iterLineage():
            yield (v,)
            if s == state:
                break
    def fetchVersions(self, cs):
        cur = self.cur; ns = self.ns
        versions = self._iterLineageToState(cs, 'mark')
        r = cur.executemany("""\
            insert or ignore into %(ws_version)s
                select * from %(qs_version)s
                    where versionId=?;""" % ns, versions)
        return r.rowcount

    def clearWorkspace(self):
        self.checkCommited()

        ex = self.cur.execute; ns = self.ns
        n, = ex("select count(seqId) from %(ws_log)s;"%ns).fetchone()
        if n:
            raise WorkspaceError("Uncommited workspace log items")

        ex("delete from %(ws_version)s;"%ns)

    def tagRevIds(self):
        self.cur.execute("update %(ws_version)s set ws_versionId=versionId, ws_revId=revId;"%self.ns)
    def clearLog(self):
        ex = self.cur.execute; ns = self.ns
        ex("update %(ws_version)s set seqId=null,flags=0,ws_versionId=versionId,ws_revId=revId;" % ns)
        ex("delete from %(ws_log)s;" % ns)
        #self.tagRevIds()

    def _publishChanges(self):
        ex = self.cur.execute; ns = self.ns
        q = ('insert into %(qs_revlog)s (revId, oid, %(payloadCols)s) \n'
             '  select L.revId, L.oid, %(payloadCols)s from %(ws_log)s as L \n'
             '    inner join %(ws_version)s using (seqId)')
        r = ex(q%ns)
        nLog = r.rowcount

        q = ('insert into %(qs_version)s (versionId, revId, oid, flags) \n'
             '  select versionId, revId, oid, 1 as flags \n'
             '    from %(ws_version)s where seqId notnull;')
        r = ex(q%ns)
        nPtrs = r.rowcount
        if nPtrs != nLog:
            raise RuntimeError("Changes to revision log (%s) did not equal the changes to the version table(%s) " % (nLog, nPtrs))

        self.clearLog()

    def _dbCommit(self):
        self.conn.commit()

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    #~ OID data operations
    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def contains(self, oid):
        q = "select oid from %(ws_version)s where oid=? limit 1;" % self.ns
        r = self.conn.execute(q, (oid,))
        return r.fetchone() is not None

    def read(self, oid, asNS=True):
        ex = self.conn.execute; ns = self.ns

        q = "select seqId, revId from %(ws_version)s where oid=?"
        r = ex(q % ns, (oid,)).fetchone()
        if r is None: return None
        seqId, revId = r

        q = "select oid, %(payloadCols)s from \n  "
        if seqId is not None:
            q += "%(ws_log)s where seqId=? and oid=?"
            r = ex(q % ns, (seqId,oid))
        else:
            q += "%(qs_revlog)s where oid=? and revId=?"
            r = ex(q % ns, (oid,revId))

        if asNS:
            r = self._rowsAsNS(r, ns)
            return next(r, None)
        else: return r.fetchone()

    def _rowsAsNS(self, res, ns):
        return (ns.new(dict(zip(e.keys(), e))) for e in res)

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    grpId = None
    def nextGroupId(self, grpId=False):
        if grpId is False:
            grpId = self.grpId
        if grpId is None:
            q = 'select max(grpId) from %s'%(self.ns.ws_log,)
            r = self.conn.execute(q).fetchone()
            grpId = (r[0] or -1) + 1

        grpId += 1
        self.grpId = grpId
        return grpId

    def newOid(self):
        return self.ns.newOid()

    def write(self, oid, **data):
        if oid is None:
            oid = self.newOid()
        op = Ops.Write(self)
        return op.perform(oid, data)

    def backout(self, seqId):
        op = Ops.Backout(self)
        return op.perform(seqId)
    def postUpdate(self, seqId, **data):
        op = Ops.PostUpdate(self)
        return op.perform(seqId, data)

    def remove(self, oid):
        if oid is None:
            return False
        op = Ops.Remove(self)
        return op.perform(oid)

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def workspaceView(host, wsid='main'):
    return Workspace(host, wsid)

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~ Register adaptors
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

sqlite3.register_adapter(Changeset, lambda cs: cs.versionId)
sqlite3.register_adapter(Workspace, lambda ws: ws.cs.versionId)

