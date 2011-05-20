##~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~##
##~ Copyright (C) 2002-2010  TechGame Networks, LLC.              ##
##~                                                               ##
##~ This library is free software; you can redistribute it        ##
##~ and/or modify it under the terms of the MIT style License as  ##
##~ found in the LICENSE file included with this distribution.    ##
##~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~##

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~ Imports 
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

from ..base.utils import splitColumnData
from .utils import OpBase

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~ Definitions 
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

class ChangeOpBase(OpBase):
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

    def addLogEntry(self, cols, data, copyForward=True):
        ex = self.cur.execute; ns = self.ns
        oid = self.oid

        seqId, isNew = self.newSeqIdEntry(oid, copyForward)

        cols[0:0] = ['grpId', 'revId']
        data[0:0] = [self.grpId, self.revId]
        self.updateLogEntry(seqId, cols, data)
        self.updateVersions(oid, seqId, isNew)
        return seqId

    def newSeqIdEntry(self, oid, copyForward=True):
        ex = self.cur.execute; ns = self.ns
        cols = ['revId', 'oid']
        if copyForward:
            cols.extend(ns.payload)
        cols = ','.join(cols)

        q = "select seqId, revId from %(ws_version)s where oid=?"
        r = ex(q % ns, (oid,)).fetchone()
        if r is None: 
            r = ex('insert into %s (oid) values (?)'%ns.ws_log, (oid,))
            return r.lastrowid, True

        seqId, revId = r
        q = 'insert into %s (%s) select %s ' % (ns.ws_log, cols, cols)

        if seqId is not None:
            q += 'from %s where seqId=? and oid=?' % ns.ws_log
            r = ex(q, (seqId, oid))
        else:
            q += 'from %s where oid=? and revId=?' % ns.qs_revlog
            r = ex(q, (oid, revId))
        return r.lastrowid, False

    def updateLogEntry(self, seqId, cols, data):
        ex = self.cur.execute; ns = self.ns
        q  = ('update %s set %s where seqId=?')
        q %= (ns.ws_log, ','.join('%s=?'%(c,) for c in cols))
        r = ex(q, data + [seqId])
        return r.rowcount

    def updateVersions(self, oid, seqId, isNew):
        ns = self.ns; ex = self.cur.execute
        verId = int(self.csWorking)
        if isNew:
            q = ('replace into %s (oid, versionId, revId, seqId) values (?,?,?,?)')
            data = [oid, verId, self.revId, seqId]
        elif seqId is not None:
            q = ('update %s set versionId=?,revId=?,seqId=? where oid=?')
            data = [verId, self.revId, seqId, oid]
        else:
            # revert back to ws_versionId and ws_revId
            q = ('update %s set versionId=ws_versionId,revId=ws_revId,seqId=null where oid=?')
            data = [oid]

        r = ex(q % (ns.ws_version,), data)
        return r.rowcount

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~ Log-oriented operations
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

class ChangeLogOpBase(ChangeOpBase):
    #~ log queries ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def oidForSeqId(self, seqId):
        q = 'select oid from %(ws_log)s where seqId=?'
        r = self.cur.execute(q% self.ns, (seqId,))
        r = r.fetchone()
        if r is not None:
            return r[0]

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
#~ Workhorse ChangeOps
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

class Touch(ChangeOpBase):
    def perform(self, oid):
        if oid is None:
            raise ValueError("OID cannot be None")
        self.oid = oid
        self.revId = int(self.csWorking)
        seqId = self.addLogEntry([], [])
        return seqId

class Write(ChangeOpBase):
    def perform(self, oid, dataItems):
        if oid is None:
            raise ValueError("OID cannot be None")
        self.oid = oid
        self.revId = int(self.csWorking)

        kwData = dict(dataItems)
        cols, data = splitColumnData(kwData, self.ns.payload)
        if kwData: 
            raise ValueError("Unknown keys: %s" % (data.keys(),))
        if not data:
            raise ValueError("No data specified for revision")

        # TODO: Investigate and apply savepoint
        seqId = self.addLogEntry(cols, data)
        return seqId

class Remove(ChangeOpBase):
    def perform(self, oid):
        if oid is None:
            raise ValueError("OID cannot be None")

        # we're reverting oid back to non-existance
        self.oid = oid
        self.revId = None
        seqId = self.addLogEntry([], [], False)
        return seqId

class PostUpdate(ChangeLogOpBase):
    def perform(self, seqId, kwData):
        self.oid = self.oidForSeqId(seqId)
        self.revId = int(self.csWorking)

        cols, data = splitColumnData(kwData, self.ns.payload)
        if kwData: 
            raise ValueError("Unknown keys: %s" % (data.keys(),))
        if not data:
            raise ValueError("No data specified for revision")

        self.updateLogEntry(seqId, cols, data)
        return seqId

class Backout(ChangeLogOpBase):
    def perform(self, seqId):
        ex = self.cur.execute; ns = self.ns
        self.oid = oid = self.oidForSeqId(seqId)
        self.revId = int(self.csWorking)

        r = ex("select max(seqId) from %(ws_log)s where oid=? and seqId!=?"%ns, (oid,seqId))
        newSeqId = r.fetchone()[0]

        r = ex("delete from %(ws_log)s where seqId=?"%ns, (seqId,))
        if r.rowcount == 0:
            return False

        if newSeqId < seqId:
            # The revision for oid needs to be corrected
            self.updateVersions(oid, newSeqId, False)
        return newSeqId

