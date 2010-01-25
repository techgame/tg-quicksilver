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

    def updateDatalog(self, cols, data):
        ex = self.cur.execute; ns = self.ns

        #r = ex('insert into %(ws_log)s (revId, oid, %(payloadCols)s) \n'
        q =('insert into %(ws_log)s \n'
            '    (revId, oid, %(payloadCols)s) \n'
            '  select revId, oid, %(payloadCols)s \n'
            '    from %(ws_view)s where oid=?;')

        r = ex(q % ns, (self.oid,))
        self.seqId = seqId = r.lastrowid

        cols[0:0] = ['seqId', 'grpId', 'oid', 'revId']
        data[0:0] = [seqId, self.grpId, self.oid, self.revId]
        q  = 'replace into %s (%s) values (%s)'
        q %= (ns.ws_log, ','.join(cols), ('?,'*len(cols))[:-1])
        ex(q, data)

        self.updateRevlog(seqId)
        return seqId

    def updateRevlog(self, seqId):
        q = ('replace into %(qs_revlog)s \n'
             '  select revId, oid, %(payloadDefs)s \n'
             '    from %(ws_log)s where seqId=?;')
        r = self.cur.execute(q % self.ns, [seqId])
        return seqId

    def updateManifest(self):
        stmt  = 'replace into %s \n'
        stmt += '  (versionId, oid, revId, flags) values (?,?,?,?)'
        data = (int(self.csWorking), self.oid, self.revId, self.flags)

        ns = self.ns; cur = self.cur
        cur.execute(stmt % (ns.ws_version,), data)
        cur.execute(stmt % (ns.qs_manifest,), data)

    #~ log queries ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def logFor(self, oid, seqId=None):
        ns = self.ns; ex = self.cur.execute

        if oid is None:
            if seqId is None:
                r = ex('select max(seqId), oid from %(ws_log)s;' % ns)
            else:
                r = ex('select seqId, oid from %(ws_log)s where seqId=?;' % ns, (seqId,))

            r = r.fetchone()
            if r is not None:
                seqId, oid = r

        q  = "select seqId, grpId, oid, revId from %(ws_log)s \n"
        q += "  where oid=? order by seqId asc;"
        return ex(q % ns, (oid,))

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
        self.updateManifest()
        return seqId

class Remove(ChangeOpBase):
    def perform(self, oid):
        if oid is None:
            raise ValueError("OID cannot be None")

        # we're reverting oid back to non-existance
        self.oid = oid
        self.revId = None
        seqId = self.updateDatalog([],[])
        self.updateManifest()
        return seqId

class Rollback(ChangeOpBase):
    def perform(self, oid=None, seqId=None):
        self.oid = oid
        for e in self.logFor(oid, seqId):
            print zip(e.keys(), e)

