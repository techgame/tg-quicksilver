#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~ Imports 
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~ Definitions 
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

class NodeOidError(Exception):
    pass
class NodeOidPool(object):
    def __init__(self, host, conn, nodeId):
        self.conn = conn
        self.nodeId = nodeId

        qinc = "update %s set oid=oid+1 where nodeid=%s"
        qinc %= (host.ns.qs_oidpool, nodeId)
        self._qinc = qinc

        qs = "select oid from %s where nodeid=%s"
        qs %= (host.ns.qs_oidpool, nodeId)
        self._qs = qs

    def next(self):
        conn = self.conn
        with conn:
            r = conn.execute(self._qinc)
            if r.rowcount != 1:
                if r.rowcount > 1:
                    raise RuntimeError("Update modified multiple rows")
                raise NodeOidError("Unable to increment next oid")

            r = conn.execute(self._qs)
            r = r.fetchone()[0]
        return r

