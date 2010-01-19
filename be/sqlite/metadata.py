#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~ Imports 
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

from ..base.metadata import MetadataViewBase

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~ Definitions 
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

class MetadataView(MetadataViewBase):
    def __init__(self, host):
        self.cur = host.cur
        self.ns = host.ns

    def getAt(self, name, idx=None, default=None):
        stmt  = "select value from %(qs_meta)s \n" % self.ns
        where = "  where name=? and idx=? limit(1)"
        r = self.cur.execute(stmt+where, (name, idx))
        r = r.fetchone()
        if r is not None:
            r = r[0]
        else: r = default
        return r

    def setAt(self, name, idx, value):
        if idx is None: 
            idx = '' # null doesn't cause index collision
        stmt   = 'insert or replace into %(qs_meta)s \n' % self.ns
        update = '  values (?, ?, ?)'
        return self.cur.execute(stmt+update, (name, idx, value))

    def deleteAt(self, name, idx):
        ex = self.cur.execute
        stmt  = 'delete from %(qs_meta)s where name=?' % self.ns
        if idx is not None: 
            stmt += ' and idx=?'
            return ex(stmt, (name, idx))
        else: return ex(stmt, (name,))

    def iter(self, name):
        stmt  = "select idx, value from %(qs_meta)s \n" % self.ns
        where = "  where name=? order by idx"
        r = self.cur.execute(stmt+where, (name,))
        return r

def metadataView(host, key=None):
    view = MetadataView(host)
    if key is not None:
        view = view.keyView(key)
    return view
