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

class WorkspaceFlags(object):
    clear = 0
    marked = 1
    changed = 4

class Workspace(WorkspaceBase):
    Schema = workspaceSchema.WorkspaceSchema
    metadataView = metadata.metadataView
    temporary = False
    _flags = WorkspaceFlags

    def __init__(self, host, wsid=None):
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

    def tagRevIds(self):
        self.cur.execute("update %(ws_version)s set ws_versionId=versionId, ws_revId=revId;"%self.ns)

    def _iterLineageToState(self, cs, state='mark'):
        if cs is None: 
            cs = self.cs
        for v,s in cs.iterLineage():
            yield (v,)
            if state in s:
                break
    def fetchVersions(self, cs, state='mark'):
        ex = self.cur.execute; ns = self.ns
        self.checkCommited()

        versions = self._iterLineageToState(cs, state)
        r = ex("""\
            insert or ignore into %(ws_version)s
                select * from %(qs_version)s
                    where versionId=?;""" % ns, versions)
        return r.rowcount

    def markCheckout(self):
        self.checkCommited()

        cs = self.cs
        if not cs.isChangesetClosed():
            raise WorkspaceError("Can only mark a committed and closed changeset")

        ex = self.cur.execute; ns = self.ns
        q = ('insert or ignore into %(qs_version)s (versionId, revId, oid, flags) \n'
             '  select ?, revId, oid, ? \n'
             '    from %(ws_version)s where ')
        q += 'versionId != ?'
        r = ex(q%ns, (cs, self._flags.marked, cs))
        count = r.rowcount
        if count:
            cs.markChangeset()
        return count

    def clearWorkspace(self):
        self.checkCommited()
        ex("delete from %(ws_version)s;"%ns)

    def publishChanges(self):
        ex = self.cur.execute; ns = self.ns
        nLog = self._publishChangeLog(ex, ns)
        nPtrs = self._publishChangeVersions(ex, ns)
        if nPtrs != nLog:
            raise RuntimeError("Changes to revision log (%s) did not equal the changes to the version table(%s) " % (nLog, nPtrs))
        return nLog

    def _publishChangeLog(self, ex, ns):
        q = ('insert into %(qs_revlog)s (revId, oid, %(payloadCols)s) \n'
             '  select L.revId, L.oid, %(payloadCols)s from %(ws_log)s as L \n'
             '    inner join %(ws_version)s using (seqId)')
        r = ex(q%ns)
        return r.rowcount

    def _publishChangeVersions(self, ex, ns):
        q = ('insert into %(qs_version)s (versionId, revId, oid, flags) \n'
             '  select versionId, revId, oid, ? \n'
             '    from %(ws_version)s where seqId notnull;')
        r = ex(q%ns, (self._flags.changed,))
        return r.rowcount

    def clearChangeLog(self):
        ex = self.cur.execute; ns = self.ns
        ex("update %(ws_version)s set seqId=null,flags=?;" % ns, 
                (self._flags.clear,))
        ex("delete from %(ws_log)s;" % ns)

    def lastChangeLogId(self):
        r = self.cur.execute("select max(seqId) from %(ws_log)s;" % self.ns)
        r = r.fetchone()
        if r is not None:
            return r[0]

    def _dbCommit(self):
        self.conn.commit()

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    #~ OID data operations
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

    def contains(self, oid):
        q = "select oid, revId, seqId from %(ws_version)s where oid=? limit 1;" % self.ns
        r = self.conn.execute(q, (oid,))
        r = r.fetchone()
        if r is not None: 
            return tuple(r)
        else: return False

    def read(self, oid, cols=False):
        ex = self.conn.execute; ns = self.ns
        if not isinstance(oid, tuple):
            r = self.contains(oid)
        else: r = oid
        if not r: return None
        oid, revId, seqId = r

        if cols is False:
            cols = ns.payloadCols
        elif not isinstance(cols, basestring):
            cols = ','.join(cols)

        q = "select oid, %s from \n  "
        if seqId is not None:
            q += "%s where seqId=? and oid=?"
            q %= (cols, ns.ws_log)
            args = (seqId,oid)
        else:
            q += "%s where oid=? and revId=?" 
            q %= (cols, ns.qs_revlog)
            args = (oid,revId)

        q += ' limit 1;'
        r = ex(q, args)
        r = r.fetchone()
        if r is not None:
            return r

    def write(self, oid, **data):
        return self.writeEx(oid, data.items())
    def writeEx(self, oid, dataItems):
        if oid is None:
            oid = self.newOid()
        op = Ops.Write(self)
        return op.perform(oid, dataItems)

    def remove(self, oid):
        if oid is None:
            return False
        op = Ops.Remove(self)
        return op.perform(oid)

    def postUpdate(self, seqId, **data):
        op = Ops.PostUpdate(self)
        return op.perform(seqId, data)
    def postBackout(self, seqId):
        op = Ops.Backout(self)
        return op.perform(seqId)

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def workspaceView(host, wsid=None):
    return Workspace(host, wsid)

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~ Register adaptors
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

sqlite3.register_adapter(Changeset, lambda cs: cs.versionId)
sqlite3.register_adapter(Workspace, lambda ws: ws.cs.versionId)

