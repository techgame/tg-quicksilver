#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~ Imports 
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

import sqlite3

from ..base.errors import WorkspaceError
from .metadata import metadataView
from .workspaceSchema import WorkspaceSchema
from .changeset import Changeset

from . import workspaceChangeOps as Ops

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~ Workspace concept
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

class Workspace(object):
    Schema = WorkspaceSchema
    temporary = False

    def __init__(self, host, wsid='main'):
        self.setHost(host)
        self.wsid = wsid
        self._initSchema()
        self._initMeta()

    def setHost(self, host):
        self.ns = host.ns.copy()
        self.conn = host.conn
        self.cur = host.cur

    def _initSchema(self):
        schema = self.Schema(self.ns, self)
        schema.initStore(self.conn)

    def _initMeta(self):
        cs = self.getMetaChangeset()
        if cs is None:
            return

        cs.init()
        if cs.isChangesetClosed():
            self.csCheckout = cs
            self.csWorking = None
        else:
            self.csCheckout = cs.csParent
            self.csWorking = cs

    metadataView = metadataView

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    csCheckout = None

    _csWorking = None
    def getCSWorking(self, create=True):
        cs = self._csWorking
        if cs is None and create:
            csParent = self.csCheckout
            if csParent is None:
                csParent = self._changesetFromArg(self, None)

            cs = csParent.newChild()
            self._csWorking = cs
            self.updateMetaChangeset()
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
        cs = self._changesetFromArg(self, cs)
        self.csCheckout = cs
    cs = property(getCS, setCS)

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def getMetaChangeset(self):
        wsMeta = self.metadataView("workspaces")
        cs = wsMeta.getAt(self.wsid, None)
        if cs is not None:
            return self._changesetFromArg(self, cs)
    def setMetaChangeset(self, cs, force=False):
        if self.temporary and not force:
            return False

        wsMeta = self.metadataView("workspaces")
        if cs is not None:
            wsMeta[self.wsid] = int(cs)
        else: del wsMeta[self.wsid]
        return True
    metaChangeset = property(getMetaChangeset, setMetaChangeset)

    def updateMetaChangeset(self):
        cs = self.cs
        self.setMetaChangeset(cs)

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    _changesetFromArg = staticmethod(Changeset.fromArg)
    def checkout(self, cs=False):
        self.checkCommited()

        if cs is False:
            cs = self.csCheckout
        else: cs = self._changesetFromArg(self, cs)

        self.clearWorkspace()
        versions = [v for v,s in cs.iterLineage() if s != 'mark']

        self.fetchVersions(versions, 'ignore')

        self.csCheckout = cs
        self.updateMetaChangeset()
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

    def checkCommited(self, fail=True):
        csw = self.getCSWorking(False)
        if csw is None: return True
        if not fail: return False
        raise WorkspaceError("Uncommited changeset")

    def clearWorkspace(self):
        self.checkCommited()

        ns = self.ns
        ex = self.cur.execute
        n, = ex("select count(seqId) from %(ws_log)s;"%ns).fetchone()
        if n:
            raise WorkspaceError("Uncommited workspace log items")

        ex("delete from %(ws_version)s;"%ns)

    def revertAll(self):
        cs = self.getCSWorking(False)
        if cs is not None:
            cs.revert()
        self.csWorking = None
        self.cur.execute("delete from %(ws_log)s;")
        self.clearWorkspace()
        self.updateMetaChangeset()

    def commit(self, **kw):
        cs = self.getCSWorking(False)
        if cs is None:
            return False
        ns = self.ns

        if kw: cs.update(kw)
        cs.updateState('closed')
        self.cur.execute("delete from %(ws_log)s;" % ns)
        self.csWorking = None
        self.csCheckout = cs
        self.updateMetaChangeset()

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

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def workspaceView(host, wsid='main'):
    return Workspace(host, wsid)

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~ Register adaptors
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

sqlite3.register_adapter(Changeset, lambda cs: cs.versionId)
sqlite3.register_adapter(Workspace, lambda ws: ws.cs.versionId)

