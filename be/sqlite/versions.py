##~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~##
##~ Copyright (C) 2002-2010  TechGame Networks, LLC.              ##
##~                                                               ##
##~ This library is free software; you can redistribute it        ##
##~ and/or modify it under the terms of the MIT style License as  ##
##~ found in the LICENSE file included with this distribution.    ##
##~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~##

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
    name = 'quicksilver'
    ns = VersionsAbstract.ns.copy()
    ns.payload = [('payload','BLOB'),('hash', 'BLOB')]
    ns.changeset = [('who', 'STRING')]

    Schema = versionsSchema.VersionSchema
    metadata = metadata.metadataView
    versions = changesView.changesView
    workspace = workspace.workspaceView

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def __init__(self, db, name=None):
        if name is None:
            name = self.name
        self.name = name

        self._loadDatabase(db)
        self.ns = self.ns.branch(name=name, dbname=self.dbname)

        self._initSchema()

    def __repr__(self):
        K = self.__class__
        return '<%s.%s %s@%s>' % (K.__module__, K.__name__, 
                self.ns.name, self.ns.dbname)

    conn = cur = None
    def _loadDatabase(self, db):
        if self.conn is not None:
            raise RuntimeError("Database is already loaded")

        if isinstance(db, basestring):
            conn = sqlite3.connect(db,
                    detect_types=sqlite3.PARSE_DECLTYPES)
        else:
            db.executemany # ducktype test
            conn = db

        conn.row_factory = sqlite3.Row
        self.conn = conn
        self.cur = self.conn.cursor()

        # pull the database name from the sqlite connection
        r = conn.execute('PRAGMA database_list;').fetchone()
        self.dbname = r[2]

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def _initSchema(self):
        schema = self.Schema(self.ns)
        schema.initStore(self.conn)
        schema.initOidSpace(self)

    def __enter__(self):
        return self.conn.__enter__()
    def __exit__(self, excType, exc, tb):
        return self.conn.__exit__(excType, exc, tb)

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

