#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~ Imports 
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

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

    def init(self, oid, revId=False):
        self.oid = oid
        if revId is False:
            revId = int(self.csWorking)
        self.revId = revId

    def perform(self, oid):
        raise NotImplementedError('Subclass Responsibility: %r' % (self,))

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

class Write(ChangeOpBase):
    def perform(self, oid, kwData):
        if oid is None:
            raise ValueError("OID cannot be None")
        self.oid = oid
        self.revId = int(self.csWorking)

        cols, data = self.splitColumnData(kwData, self.ns.payload)
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

