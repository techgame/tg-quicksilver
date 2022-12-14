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
from . import oidPool

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~ Definitions 
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

class Versions(VersionsAbstract):
    name = 'quicksilver'
    ns = VersionsAbstract.ns.copy()
    ns.payload = [('payload','BLOB'),('hash', 'BLOB')]
    ns.changeset = [('who', 'STRING')]

    NodeOidPool = oidPool.NodeOidPool
    Schema = versionsSchema.VersionSchema
    metadata = metadata.metadataView
    versions = changesView.changesView
    workspace = workspace.workspaceView

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def __init__(self, db, name=None, mutable=True):
        if name is None:
            name = self.name
        self.name = name
        self.ns = self.ns.branch(name=name)

        if mutable:
            self._loadDatabase(db)
            self._initSchema()
        else:
            # use an un-named temp db, instead of a fully in-memory db
            self._loadDatabase('', db)
            self._initSchema(db)

        self._mutable = mutable

    _mutable = True
    def isMutable(self):
        return self._mutable

    def __repr__(self):
        K = self.__class__
        return '<%s.%s %s@%s>' % (K.__module__, K.__name__, 
                self.ns.name, self.ns.dbname)

    def _openDatabase(self, db):
        if isinstance(db, basestring):
            conn = sqlite3.connect(db,
                    check_same_thread=False,
                    detect_types=sqlite3.PARSE_DECLTYPES)
        else:
            db.executemany # ducktype test
            conn = db

        conn.row_factory = sqlite3.Row
        return conn

    conn = cur = None
    def _loadDatabase(self, db, dbname=None):
        if self.conn is not None:
            raise RuntimeError("Database is already loaded")

        self.conn = conn = self._openDatabase(db)
        self.cur = self.conn.cursor()

        # pull the database name from the sqlite connection
        if dbname is None:
            r = conn.execute('PRAGMA database_list;').fetchone()
            dbname = r[2]

        self.dbname = dbname
        self.ns = self.ns.branch(dbname=self.dbname)

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def _initSchema(self, dbBranch=None):
        with self.conn:
            schema = self.Schema(self.ns)
            schema.initStore(self.conn)
            schema.initOidSpace(self)

        if dbBranch is not None:
            schema.initFromBranch(dbBranch, self.conn)

        # use a new connection to isolate transactions, so oids are always increasing
        ns = self.ns

        poolConn = self.conn
        ns.newRootOid = self.NodeOidPool(self, poolConn, 0)
        ns.newNodeOid = self.NodeOidPool(self, poolConn, ns.nodeId)
        ns.newOid = ns.newNodeOid

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

