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

        genNodeOid = self.provisionNode(nodeId, ns)
        for e in genNodeOid: 
            self.next = partial(self._nextEx, genNodeOid)
            break

    def _nextEx(self, genNodeOid):
        # remove the monkeypatch
        del self.next

        # finish the generator
        for e in genNodeOid: 
            pass

        # return the result
        return self.next()

    def next(self):
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

    def provisionNode(self, nodeId, ns):
        r = self.conn.execute(self._qs).fetchone()
        if r is not None:
            return

        h = identhash.Int64IdentHasher()
        h.addInt(nodeId)
        h.addTimestamp()

        oidSpace, nodeSpace = ns.oidNodeSpace
        q = ("insert into %s (oid, oid0, nodeId) values (?, ?, %s)" % (ns.qs_oidpool, nodeId))
        yield

        ex = self.conn.execute
        while 1:
            oid0 = (int(h) % nodeSpace) * oidSpace
            try:
                ex(q, (oid0, oid0))
            except IntegrityError: 
                h.addTimestamp()
                continue
            return

