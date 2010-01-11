#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~ Imports 
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

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
        ('ts',  'TIMESTAMP default CURRENT_TIMESTAMP'),
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

#class VersionsView(object):
    def headVersions(self):
        res = self.cur.execute(
                "select unique versionId from %(qs_changesets)s \n"
                "  except select parentId from %(qs_changesets)s;")
        return res

    def rootVersions(self):
        res = self.cur.execute(
                "select versionId from %(qs_changesets)s \n"
                "  where parentId=null")
        return res

    def orphanVersions(self):
        res = self.cur.execute(
                "select unique parentId from %(qs_changesets)s \n"
                "  except select versionId from %(qs_changesets)s;")
        return res

