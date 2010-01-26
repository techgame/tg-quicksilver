#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~ Imports 
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

from ..base.utils import splitColumnData
from .utils import OpBase

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~ Definitions 
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

class ChangeOpBase(OpBase):
    flags = 2
    csWorking = None

    def __init__(self, host):
        self.setHost(host)
        self.csWorking = host.csWorking

    def setHost(self, host):
        self.cur = host.cur
        self.ns = host.ns
        self.grpId = host.grpId

    def perform(self, oid):
        raise NotImplementedError('Subclass Responsibility: %r' % (self,))

    def updateDatalog(self, cols, data, copyForward=True):
        ex = self.cur.execute; ns = self.ns
        oid = self.oid

        seqId, isNew = self.newSeqIdEntry(oid, copyForward)

        cols[0:0] = ['grpId', 'revId']
        data[0:0] = [self.grpId, self.revId]
        q  = ('update %s set %s where seqId=?')
        q %= (ns.ws_log, ','.join('%s=?'%(c,) for c in cols))
        data.append(seqId)
        ex(q, data)

        self.updateRevLogFromSeqId(seqId)
        self.updateManifest(isNew)
        return seqId

    def newSeqIdEntry(self, oid, copyForward=True):
        ex = self.cur.execute; ns = self.ns
        cols = ['revId', 'oid']
        if copyForward:
            cols.extend(ns.payload)
        cols = ','.join(cols)

        q =('insert into %s (%s) \n'
            '  select %s \n'
            '    from %s where oid=?;')
        q %= (ns.ws_log, cols, cols, ns.ws_view)
        r = ex(q, (oid,))
        if r.rowcount != 0:
            return r.lastrowid, False

        r = ex('insert into %s (oid) values (?)'%ns.ws_log, (oid,))
        return r.lastrowid, True

    def updateRevLogFromSeqId(self, seqId):
        q = ('replace into %(qs_revlog)s \n'
             '  select revId, oid, %(payloadDefs)s \n'
             '    from %(ws_log)s where seqId=?;')
        r = self.cur.execute(q % self.ns, [seqId])
        return seqId

    def updateManifest(self, isNew):
        ns = self.ns; ex = self.cur.execute
        stmt  = 'replace into %s \n'
        stmt += '  (versionId, revId, flags, oid) values (?,?,?,?)'

        data = [int(self.csWorking), self.revId, self.flags, self.oid]

        q = stmt
        if not isNew:
            q  = 'update %s set versionId=?, revId=?, flags=? where oid=?'

        r = ex(q % (ns.ws_version,), data)
        assert r.rowcount == 1, (r.rowcount, q, data)
        r = ex(stmt % (ns.qs_manifest,), data)
        assert r.rowcount == 1, (r.rowcount, stmt, data)

    #~ log queries ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def logForOid(self, oid, seqId=None):
        ns = self.ns; ex = self.cur.execute
        if oid is None:
            if seqId is None:
                r = ex('select max(seqId), oid from %(ws_log)s;' % ns)
            else:
                r = ex('select seqId, oid from %(ws_log)s where seqId=?;' % ns, (seqId,))

            r = r.fetchone()
            if r is not None:
                seqId, oid = r

        return self._logForOid(oid, ex, ns)

    def _logForOid(self, oid, ex=None, ns=None):
        if ex is None: ns = self.ns; ex = self.cur.execute
        q = ("select seqId, grpId, oid, revId \n"
            "  from %(ws_log)s\n"
            "  where oid=? order by seqId asc;")
        return ex(q % ns, (oid,))

    def logForGrpId(self, grpId, ex=None, ns=None):
        if ex is None: ns = self.ns; ex = self.cur.execute
        q = ("select seqId, grpId, oid, revId \n"
            "  from %(ws_log)s\n"
            "  where grpId=? order by seqId asc;")
        return self.cur.execute(q % self.ns, (grpId,))

    def logForSeqId(self, seqId):
        ns = self.ns; ex = self.cur.execute
        if seqId is None:
            r = ex('select max(seqId), oid from %(ws_log)s;' % ns)
        else:
            r = ex('select seqId, oid from %(ws_log)s where seqId=?;' % ns, (seqId,))

        r = r.fetchone()
        if r is not None:
            seqId, oid = r
        return self._logForOid(oid, ex, ns)

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

class Write(ChangeOpBase):
    def perform(self, oid, kwData):
        if oid is None:
            raise ValueError("OID cannot be None")
        self.oid = oid
        self.revId = int(self.csWorking)

        cols, data = splitColumnData(kwData, self.ns.payload)
        if kwData: 
            raise ValueError("Unknown keys: %s" % (data.keys(),))
        if not data:
            raise ValueError("No data specified for revision")

        # TODO: Investigate and apply savepoint
        seqId = self.updateDatalog(cols, data)
        return seqId

class Remove(ChangeOpBase):
    def perform(self, oid):
        if oid is None:
            raise ValueError("OID cannot be None")

        # we're reverting oid back to non-existance
        self.oid = oid
        self.revId = None
        seqId = self.updateDatalog([], [], False)
        return seqId

class Backout(ChangeOpBase):
    def perform(self, seqId):
        log = list(self.logForSeqId(seqId))
        if len(log) == 1:
            print seqId, 'remove and revert to revlog'
            print log
        elif log:
            print seqId, 'remove and revert'
            print log
        else:
            print seqId, 'not present?'

