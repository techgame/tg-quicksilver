#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~ Imports 
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

from functools import partial
from sqlite3 import IntegrityError

from ..base.utils import identhash

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~ Definitions 
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

class NodeOidError(Exception):
    pass

class NodeOidPool(object):
    def __init__(self, host, conn, nodeId):
        ns = host.ns
        self.conn = conn
        self.nodeId = nodeId

        qinc = "update %s set oid=oid+1 where nodeid=%s"
        qinc %= (ns.qs_oidpool, nodeId)
        self._qinc = qinc

        qs = "select oid from %s where nodeid=%s"
        qs %= (ns.qs_oidpool, nodeId)
        self._qs = qs

        self.bindProvisionNode(nodeId, ns)

    def newOid(self):
        return self._incNodeOid()
    __call__ = next = newOid

    def _incNodeOid(self):
        conn = self.conn
        with conn:
            r = conn.execute(self._qinc)
            if r.rowcount != 1:
                if r.rowcount > 1:
                    raise RuntimeError("Update modified multiple rows")
                raise NodeOidError("Unable to increment next oid")

            r = conn.execute(self._qs).fetchone()
            if r is not None:
                r = r[0]
        return r

    def current(self):
        r = self.conn.execute(self._qs).fetchone()
        if r is not None:
            return r[0]

    def bindProvisionNode(self, nodeId, ns):
        if self.current() is not None:
            return False

        def provisionNode():
            h = identhash.Int64IdentHasher()
            h.addInt(nodeId)
            h.addTimestamp()

            oidSpace, nodeSpace = ns.oidNodeSpace
            q = ("insert into %s (oid, oid0, nodeId) values (?, ?, %s)" % (ns.qs_oidpool, nodeId))

            ex = self.conn.execute
            while 1:
                oid0 = (int(h) % nodeSpace) * oidSpace
                try:
                    ex(q, (oid0, oid0))
                except IntegrityError: 
                    h.addTimestamp()
                else: break

            # remove monkeypatch, and continue as normal
            del self._incNodeOid
            return oid0

        # Create monkeypatch to provision the node on first access
        self._incNodeOid = provisionNode
        return True

