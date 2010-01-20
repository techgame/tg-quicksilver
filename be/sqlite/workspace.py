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

    def _updateCurrentChangeset(self):
        return self.saveCurrentChangeset()

    def _iterLineageToState(self, cs, state='mark'):
        if cs is None: 
            cs = self.cs
        for v,s in cs.iterLineage():
            yield (v,)
            if s == state:
                break
    def fetchVersions(self, cs):
        versions = self._iterLineageToState(cs, 'mark')
        res = self.cur.executemany("""\
            insert or ignore into %(ws_version)s
                select * from %(qs_manifest)s
                    where versionId=?;""" % self.ns, versions)
        return res

    def clearWorkspace(self):
        self.checkCommited()

        ns = self.ns
        ex = self.cur.execute
        n, = ex("select count(seqId) from %(ws_log)s;"%ns).fetchone()
        if n:
            raise WorkspaceError("Uncommited workspace log items")

        ex("delete from %(ws_version)s;"%ns)

    def clearLog(self):
        self.cur.execute("delete from %(ws_log)s;" % self.ns)

    def _dbCommit(self):
        self.conn.commit()

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    #~ OID data operations
    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def read(self, oid, asNS=True):
        ns = self.ns
        q = "select oid, %(payloadCols)s from %(ws_view)s where oid=?" % ns
        res = self.conn.execute(q, (oid,))
        if asNS:
            res = self._rowsAsNS(res, ns)
        return next(res, None)

    def readAll(self, asNS=True):
        ns = self.ns
        q = "select oid, %(payloadCols)s from %(ws_view)s" % ns
        res = self.conn.execute(q)
        if asNS:
            res = self._rowsAsNS(res, ns)
        return res

    def _rowsAsNS(self, res, ns):
        return (ns.new(dict(zip(e.keys(), e))) for e in res)

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def newOid(self):
        return self.ns.newOid()

    def write(self, oid, **data):
        if oid is None:
            oid = self.newOid()
        op = Ops.Write(self)
        return op.perform(oid, data)

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

