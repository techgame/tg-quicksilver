#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~ Imports 
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

from collections import defaultdict
import sqlite3

from .utils import Namespace
from .metadata import metadataView
from .versionsSchema import VersionSchema
from .workspace import Workspace

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~ Definitions 
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

class Versions(object):
    ns = Namespace() # copied upon init
    ns.payload = [
        ('payload','BLOB'),
        ('tags', 'TEXT default null'),
        ]
    ns.changeset = [
        ('who', 'text default null'),
        ('node', 'text default null'),
        ('note', 'text default null'),
        ]

    Schema = VersionSchema
    metadataView = metadataView

    def __init__(self, conn, name='objectStore'):
        self.ns = self.ns.branch(name=name)
        self.name = name
        self.conn = conn
        conn.isolation_level = 'DEFERRED'
        self.cur = conn.cursor()

        self._initSchema()

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def _initSchema(self):
        schema = self.Schema(self.ns)
        schema.initStore(self.conn)
        schema.initOidSpace(self)

    def workspace(self):
        return Workspace(self)

    def lineage(self):
        stmt = "select versionId, parentId from %(qs_changesets)s;"
        res = self.cur.execute(stmt % self.ns)

        r = defaultdict(list)
        for vid, pid in res:
            r[pid].append((vid, [], r[vid]))

        for pid, cl in r.iteritems():
            if len(cl) <= 1 and pid:
                continue

            for (vid, ll, bl) in cl:
                while len(bl) == 1:
                    v = bl[0]
                    ll.append(v[0])
                    ll.extend(v[1])
                    bl[:] = v[2]

        return r[None]

#class VersionsView(object):
    def rootIds(self, incTS=False):
        stmt = """\
          select versionId, ts
            from %(qs_changesets)s
              where parentId is null
            order by ts desc;"""
        res = self.cur.execute(stmt % self.ns)
        if incTS: return iter(res)
        else: return (e[0] for e in res)

    def headIds(self, incTS=False):
        stmt = """\
          select versionId, ts
            from %(qs_changesets)s
              join (select versionId from %(qs_changesets)s 
                    except select parentId from %(qs_changesets)s)
                using (versionId)
            order by ts desc;"""
        res = self.cur.execute(stmt % self.ns)
        if incTS: return iter(res)
        else: return (e[0] for e in res)

    def orphanIds(self):
        res = self.cur.execute("""\
            select distinct parentId from %(qs_changesets)s 
                where parentId not null
                except select versionId from %(qs_changesets)s
            ;""" % self.ns)
        return res


