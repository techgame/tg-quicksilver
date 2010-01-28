#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~ Imports 
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

from TG.quicksilver.be.sqlite import Versions

from .store import BoundaryStore
from .storeEx import StatsBoundaryStore

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~ Definitions 
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

class BoundaryVersions(Versions):
    ns = Versions.ns.copy()
    ns.payload = [
        ('hash','BLOB'),
        ('typeref','TEXT'),
        ('payload','BLOB'),
        ]
    ns.changeset = [
        ('note', 'text default null'),
        ]

    def boundaryWorkspace(self, wsid=None, **kw):
        ws = self.workspace(wsid)
        return self.boundaryStoreForWS(ws, **kw)
    boundaryWS = boundaryWorkspace

    def boundaryStoreForWS(self, ws, stats=False):
        if stats:
            bs = StatsBoundaryStore(ws)
        else:
            bs = BoundaryStore(ws)
        return bs

