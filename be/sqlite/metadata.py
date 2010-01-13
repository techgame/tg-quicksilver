#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~ Imports 
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

from .utils import HostDataView

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~ Definitions 
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

class MetadataView(HostDataView):
    def get(self, name, default=None):
        return self.getMeta(name, None, default)
    def getAt(self, name, idx=None, default=None):
        stmt  = "select value from %(qs_meta)s \n" % self.ns
        where = "  where name=? and idx=? limit(1)"
        r = self.cur.execute(stmt+where, (name, idx))
        r = r.fetchone()
        if r is not None:
            r = r[0]
        else: r = default
        return r

    def set(self, name, value):
        return self.setAt(name, None, value)
    def setAt(self, name, idx, value):
        if idx is None: 
            idx = '' # null doesn't cause index collision
        stmt   = 'insert or replace into %(qs_meta)s \n' % self.ns
        update = '  values (?, ?, ?)'
        return self.cur.execute(stmt+update, (name, idx, value))

    def delete(self, name):
        return self.deleteAt(name, None, value)
    def deleteAt(self, name, idx):
        ex = self.cur.execute
        stmt  = 'delete from %(qs_meta)s where name=?' % self.ns
        if idx is not None: 
            stmt += ' and idx=?'
            return ex(stmt, (name, idx))
        else: return ex(stmt, (name,))

    def default(self, name, value=None):
        return self.defaultAt(name, None, value)
    def defaultAt(self, name, idx, value=None):
        sentinal = object()
        r = self.getAt(name, idx, sentinal)
        if r is sentinal:
            self.setAt(name, idx, value)
            return value
        else: return r

    def iter(self, name):
        stmt  = "select idx, value from %(qs_meta)s \n" % self.ns
        where = "  where name=? order by idx"
        r = self.cur.execute(stmt+where, (name,))
        return r

    def view(self, metaKey):
        return KeyedMetadataView(self, metaKey)
    def __getitem__(self, metaKey):
        return self.view(metaKey)

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

class KeyedMetadataView(object):
    def __init__(self, host, metaKey):
        self._name = metaKey
        self._view = MetadataView(host)

    def get(self, default=None):
        return self.getAt(None, default)
    def set(self, value):
        return self.setAt(None, value)
    def delete(self):
        return self.deleteAt(None, value)
    def default(self, value=None):
        return self.defaultAt(None, value)

    def getAt(self, key, default=None):
        return self._view.getAt(self._name, key, default)
    def setAt(self, key, value):
        return self._view.setAt(self._name, key, value)
    def deleteAt(self, key):
        return self._view.deleteAt(self._name, key)

    def defaultAt(self, key, value=None):
        sentinal = object()
        r = self.getAt(key, sentinal)
        if r is sentinal:
            r = value
            self.setAt(key, r)
        return r

    def __iter__(self):
        return self._view.iter(self._name)
    def __getitem__(self, key):
        if isinstance(key, slice):
            return list(self)[key]
        else: return self.getAt(key)
    def __setitem__(self, key, value):
        if isinstance(key, slice):
            raise ValueError("KeyedMetadataView does not support assignment by slice")
        return self.setAt(key, value)
    def __delitem__(self, key, value):
        if isinstance(key, slice):
            raise ValueError("KeyedMetadataView does not support delete by slice")
        return self.delAt(key, value)


def metadataView(host, key=None):
    if key is None:
        return MetadataView(host)

    return KeyedMetadataView(host, key)

