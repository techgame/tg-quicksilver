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

    def perform(self, oid):
        raise NotImplementedError('Subclass Responsibility: %r' % (self,))

    def updateDatalog(self, cols, data):
        if cols:
            args = (', ' + ', '.join(cols), ',?'*len(cols))
        else: args = ('', '')

        stmt  = '%s into %s '
        stmt += '(revId, oid%s) values (?,?%s)' % args
        data[0:0] = [self.revId, self.oid]

        ns = self.ns; cur = self.cur
        cur.execute(stmt%('insert',  ns.ws_log), data)
        cur.execute(stmt%('replace', ns.qs_revlog), data)

    def updateManifest(self):
        stmt  = '%s into %s \n'
        stmt += '  (versionId, oid, revId, flags) values (?,?,?,?)'
        data = (int(self.csWorking), self.oid, self.revId, self.flags)

        ns = self.ns; cur = self.cur
        cur.execute(stmt%('replace', ns.ws_version), data)
        cur.execute(stmt%('replace', ns.qs_manifest), data)

    #~ log queries ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def logFor(self, oid, seqId=None):
        q = "select seqId, oid, revId from %(ws_log)s \n"
        if oid is None:
            if seqId is None:
                q += '  where oid=(select oid from %(ws_log)s where seqId=max(seqId)) \n'
            else:
                q += '  where oid=(select oid from %(ws_log)s where seqId=?) \n'
                args = (seqId,)
        else:
            if seqId is None:
                q += '  where oid=? \n'
                args = (oid,)
            else:
                q += '  where oid=? and seqId <= ?\n'
                args = (oid, seqId)

        q += '  order by seqId;'
        return self.cur.execute(q % self.ns, args)

    def oidForSeqId(self, seqId):
        r = ex("select oid from %(ws_log)s where seqId=?;"%ns, (seqId,))
        r = r.fetchone()
        if r is not None:
            return r[0]
        else: return None

    def lastLogEntry(self):
        # find the last oid we modified
        r = ex("select max(seqId), oid from %(ws_log)s;"%ns)
        r = r.fetchone()
        if r is None:
            # there is no last action because the list is empty
            r = (None, None)
        return r

    def lastLogByOid(self, oid):
        if oid is None:
            return self.lastLogEntry()

        r = ex("select max(seqId), oid from %(ws_log)s where oid=?;"%ns, (oid,))
        r = r.fetchone()
        if r is None:
            # there is no last change for the oid in question
            r = (None, oid)
        return r

    def prevLogByOid(self, seqId, oid):
        r = ex("select max(seqId), revId, oid, ts, %(payloadCols)s from %(ws_log)s where oid=? and seqId<?;"%ns, (oid, seqId))
        return r.fetchone()

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
        self.updateDatalog(cols, data)
        self.updateManifest()
        return True

class Remove(ChangeOpBase):
    def perform(self, oid):
        if oid is None:
            raise ValueError("OID cannot be None")

        # we're reverting oid back to non-existance
        self.oid = oid
        self.revId = None
        self.updateDatalog([],[])
        self.updateManifest()

class Rollback(ChangeOpBase):
    def perform(self, oid, seqId=None):
        if seqId is None:
            if oid is None:
                return False
            oid = self.oidForSeqId(seqId)
        else:
            seqId, oid = self.lastLogByOid(oid)
        if oid is None:
            return False

        if seqId is not None: 
            res = self.prevLogByOid(seqId, oid)
        else: res = None

        if res is not None:
            # the last revision is in res
            pass

        else: 
            # the last revision is in ws_version.ws_revId
            delete


        self.oid = oid
        self.revId = int(self.csWorking)

        # TODO: Implement
        return False
        return True

