#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~ Imports 
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

import sqlite3

from .utils import OpBase, HostDataView, QuicksilverError
from .workspaceSchema import WorkspaceSchema
from .changeset import Changeset

from . import workspaceChangeOps as Ops

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~ Workspace concept
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

class WorkspaceError(QuicksilverError):
    pass

class Workspace(HostDataView):
    Schema = WorkspaceSchema
    temporary = False

    def __init__(self, host, wsid='main'):
        self.setHost(host)
        self.wsid = wsid
        self._initSchema()

    def setHost(self, host):
        self.ns = host.ns.copy()
        self.conn = host.conn
        self.cur = host.cur

    def _initSchema(self):
        schema = self.Schema(self.ns, self)
        schema.initStore(self.conn)

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    csCheckout = None

    _csWorking = None
    def getCSWorking(self, create=True):
        cs = self._csWorking
        if cs is None and create:
            cs = self.csCheckout.newChild()
            self._csWorking = cs
        return cs
    csWorking = property(getCSWorking)

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    _changesetFromArg = staticmethod(Changeset.fromArg)
    def checkout(self, cs):
        cs = self._changesetFromArg(self, cs)

        self.clearWorkspace()
        versions = [v for v,s in cs.iterLineage() if s != 'mark']

        self.fetchVersions(versions, 'ignore')

        self.csCheckout = cs
        self._versionsToMark = versions
        return cs

    def fetchVersions(self, iterVersions, onConflict='ignore'):
        ns = self.ns.copy()
        ns.update(onConflict=onConflict)

        res = self.cur.executemany("""\
            insert or %(onConflict)s into %(ws_version)s
                select * from %(qs_manifest)s
                    where versionId=?;""" % ns, 
                    ((v,) for v in iterVersions))
        return res

    def clearWorkspace(self):
        csw = self.getCSWorking(False)
        if csw is not None:
            raise WorkspaceError("Uncommited changeset")

        ns = self.ns
        ex = self.cur.execute
        n, = ex("select count(seqId) from %(ws_log)s;"%ns).fetchone()
        if n:
            raise WorkspaceError("Uncommited workspace log items")

        ex("delete from %(ws_version)s;"%ns)

    def revertAll(self):
        csWorking = self.getCSWorking(False)
        if csWorking is not None:
            csWorking.updateState('reverted')
        self.cur.execute("delete from %(ws_log)s;")
        self._csWorking = None
        self.clearWorkspace()

    def commit(self, **kw):
        csWorking = self.getCSWorking(False)
        if csWorking is None:
            return False
        ns = self.ns

        if kw: csWorking.update(kw)
        csWorking.updateState('closed')
        self.cur.execute("delete from %(ws_log)s;" % ns)
        self.csCheckout = csWorking
        self._csWorking = None
        self.conn.commit()
        return True

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def read(self, oid, asNS=True):
        ns = self.ns
        q = "select oid, %(payloadCols)s from %(ws_view)s where oid=?" % ns
        res = self.conn.execute(q, (oid,))
        if asNS:
            res = self._rowsAsNS(res, ns)
        return res

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

    def write(self, oid, **data):
        if oid is None:
            oid = self.ns.newOid()
        op = Ops.Write(self)
        return op.perform(oid, data)

    def remove(self, oid):
        if oid is None:
            return False
        op = Ops.Remove(self)
        return op.perform(oid)

