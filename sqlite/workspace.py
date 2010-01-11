#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~ Imports 
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

import sqlite3

from .workspaceSchema import WorkspaceSchema
from .utils import OpBase

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~ Workspace concept
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

class Workspace(object):
    Schema = WorkspaceSchema
    temporary = False

    def __init__(self, host, wsid=None):
        self.ns = host.ns.copy()
        self.wsid = wsid or 42 #id(self) # TODO
        self.host = host
        self.conn = host.conn
        self._initSchema()

    def _initSchema(self):
        schema = self.Schema(self.ns, self)
        schema.initStore(self.conn)

    def _updateWorking(self, iterVersions, onConflict='ignore'):
        ns = self._ns.copy()
        ns.update(onConflict=onConflict)

        r = conn.executemany("""\
            insert or %(onConflict)s into %(ws_version)s
                select * from %(qs_manifest)s
                    where versionId=?;""" % ns, iterVersions)
        return r

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def _rowsAsNS(self, res, ns):
        for e in res:
            e = ns.new(dict(zip(ns.payload, e)))
            yield e

    def read(self, oid, asNS=True):
        ns = self.ns
        q = "select %(payloadCols)s from %(ws_view)s where oid=?" % ns
        res = self.conn.execute(q, (oid,))
        if asNS:
            res = self._rowsAsNS(res, ns)
        return res

    def readAll(self, asNS=True):
        ns = self.ns
        q = "select %(payloadCols)s from %(ws_view)s" % ns
        res = self.conn.execute(q)
        if asNS:
            res = self._rowsAsNS(res, ns)
        return res

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    _revId = None
    def getRevId(self, create=True):
        r = self._revId
        if r is None and create:
            r = self.newChangeset();#TODO
            self._revId = r
        return r
    def setRevId(self, revId):
        self._revId = revId
    revId = property(getRevId, setRevId)

    def write(self, oid, **data):
        revId = 360 # TODO: replace
        op = WriteRevisionOp(self, oid, revId)
        return op.writeRev(data)

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

class WriteRevisionOp(OpBase):
    def __init__(self, host, oid, revId):
        ns = host.ns
        if oid is None:
            oid = ns.newOid()
        self.ns = ns
        self.cur = host.conn.cursor()
        self.oid = oid
        self.revId = revId

    def writeRev(self, kwData):
        cols, data = self.splitColumnData(kwData, self.ns.payload)
        if kwData: 
            raise ValueError("Unknown keys: %s" % (data.keys(),))
        if not data:
            raise ValueError("No data specified for revision")

        # TODO: Investigate and apply savepoint
        self._writeDatalog(cols, data)
        self._writeManifest()
        return True

    def _writeDatalog(self, cols, data):
        stmt  = '%s into %s '
        stmt += '(revId, oid, %s) values (?,?%s)' % (', '.join(cols), ',?'*len(cols))
        data[0:0] = [self.revId, self.oid]

        ns = self.ns; cur = self.cur
        cur.execute(stmt%('insert',  ns.ws_log), data)
        cur.execute(stmt%('replace', ns.qs_revlog), data)

    def _writeManifest(self, flags=2):
        stmt  = '%s into %s \n'
        stmt += '  (versionId, oid, revId, flags) values (?,?,?,?)'
        data = (self.revId, self.oid, self.revId, flags)

        ns = self.ns; cur = self.cur
        cur.execute(stmt%('replace', ns.ws_version), data)
        cur.execute(stmt%('replace', ns.qs_manifest), data)

