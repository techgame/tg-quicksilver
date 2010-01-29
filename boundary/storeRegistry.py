#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~ Imports 
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~ Definitions 
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

class BoundaryStoreRegistry(object):
    def __init__(self):
        self.db_oid = {}
        self.db_ident = {}

    def allOids(self):
        return self.db_oid.values()

    def add(self, entry):
        oid = entry.oid
        self.db_oid[oid] = entry

        db_ident = self.db_ident
        db_ident[id(entry.pxy)] = oid
        if entry.obj is not None:
            db_ident[id(entry.obj)] = oid

    def remove(self, oid):
        entry = self.lookup(oid)
        if entry is None:
            return None

        oid = entry.oid
        entry = self.db_oid.pop(oid, None)
        self.db_ident.pop(id(entry.obj), None)
        self.db_ident.pop(id(entry.pxy), None)
        return entry

    def lookup(self, oid, default=None):
        if not isinstance(oid, (int, long)):
            oid = self.db_ident[id(oid)]
        return self.db_oid.get(oid, default)

    def oidForObj(self, obj, default=None):
        return self.db_ident.get(id(obj), default)
    def get(self, obj, default=None):
        return self.db_ident.get(id(obj), default)
    def __getitem__(self, obj):
        assert not isinstance(obj, (int, long, float)), "Registry's getitem is for objects only"
        return self.db_ident[id(obj)]
