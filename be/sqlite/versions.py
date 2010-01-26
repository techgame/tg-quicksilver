#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~ Imports 
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

import sqlite3

from ..base.versions import VersionsAbstract
from .workspace import Workspace

from . import versionsSchema
from . import metadata
from . import changesView
from . import workspace

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~ Definitions 
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

class Versions(VersionsAbstract):
    ns = VersionsAbstract.ns.copy()
    ns.payload = [('payload','BLOB'),('hash', 'BLOB')]
    ns.changeset = [('who', 'STRING')]

    Schema = versionsSchema.VersionSchema
    metadata = metadata.metadataView
    versions = changesView.changesView
    workspace = workspace.workspaceView

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def __init__(self, db, name='objectStore'):
        self._loadDatabase(db)
        self.ns = self.ns.branch(name=name)
        self.name = name

        self._initSchema()

    conn = cur = None
    def _loadDatabase(self, db):
        if self.conn is not None:
            raise RuntimeError("Database is already loaded")

        if isinstance(db, basestring):
            conn = sqlite3.connect(db,
                    isolation_level=None,
                    detect_types=sqlite3.PARSE_DECLTYPES)
        else:
            db.executemany # ducktype test
            conn = db
            conn.isolation_level = None

        conn.row_factory = sqlite3.Row
        self.conn = conn
        self.cur = conn.cursor()

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def _initSchema(self):
        schema = self.Schema(self.ns)
        schema.initStore(self.conn)
        schema.initOidSpace(self)

    def __enter__(self):
        self.conn.execute('BEGIN TRANSACTION')

    def __exit__(self, excType, exc, tb):
        if excType is None:
            self.conn.commit()
        else:
            self.conn.rollback()

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~ Example verisons extension with more columns
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

class VersionsPlus(Versions):
    ns = Versions.ns.copy()
    ns.payload = [
        ('payload','BLOB'),
        ('hash','BLOB'),
        ('tags', 'TEXT default null'),
        ]
    ns.changeset = [
        ('who', 'text default null'),
        ('node', 'text default null'),
        ('note', 'text default null'),
        ]

