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

import itertools
import sqlite3

from ..base.utils import identhash
from . import utils
from .metadata import metadataView

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~ Versioning Schema
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

class VersionSchema(object):
    sqlObjects = [
        'qs_changesets',    # changeset documentation, linking to qs_version
        'qs_version',       # manifest of version to revlog entry
        'qs_revlog',        # oid,revId -> data entry
        'qs_meta',          # holds misc db information
        'qs_oidpool',       # maintains a next oid by nodeid
        ]

    def __init__(self, ns):
        self.ns = ns
        self.initNS(ns)

    def initNS(self, ns):
        name = ns.name
        for k in self.sqlObjects:
            ns[k] = '%s_%s' % (name, k)

        self._nsDefineItem(ns, 'payload')
        self._nsDefineItem(ns, 'changeset')

    def _nsDefineItem(self, ns, name):
        columns = ns[name]
        names = [e[0] for e in columns]
        defs = ['%s %s'%e for e in columns]

        ns[name] = names
        ns[name+'List'] = columns
        ns[name+'Cols'] = ', '.join(names)
        ns[name+'Defs'] = ', '.join(defs)

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def initStore(self, conn):
        self.cur = conn.cursor()
        self.initPragmas(conn)
        with conn:
            self.createRevisionlog()
            self.createVersion()
            self.createChangesets()
            self.createMeta()
            self.createOidPool()

    pragmas = {
        'synchronous': 'NORMAL',    # OFF | NORMAL | FULL
        'temp_store': 'MEMORY',     # DEFAULT | FILE | MEMORY
        'encoding': '"UTF-8"',
        }

    def initPragmas(self, conn):
        pragmas = self.pragmas.items()
        pragmas.sort()
        pragmas = '\n'.join('PRAGMA %s = %s;' % e for e in pragmas)
        conn.executescript(pragmas)

    def createRevisionlog(self):
        self.cur.execute("""\
            create table if not exists %(qs_revlog)s (
              oid INTEGER not null,
              revId INTEGER, 

              %(payloadDefs)s,
              
              primary key (revId, oid) on conflict replace
              );""" % self.ns)

        self.addColumnsTo('qs_revlog', 'payload')

    def createVersion(self):
        ns = self.ns
        self.cur.execute("""\
            create table if not exists %(qs_version)s (
              oid INTEGER,
              revId INTEGER,
              versionId INTEGER,
              flags INTEGER,

              primary key (versionId, oid) on conflict replace
            );""" % ns)
        self.cur.execute("""\
            create index if not exists %(qs_version)s_index 
                on %(qs_version)s (versionId);""" % ns)

    def createChangesets(self):
        ns = self.ns
        defs = self.ns.changesetDefs
        comma = (',' if defs else '')

        self.cur.execute("""\
            create table if not exists %s (
              versionId INTEGER primary key on conflict fail,
              fullVersionId TEXT,
              state TEXT,
              branch TEXT,
              ts TIMESTAMP default CURRENT_TIMESTAMP,

              parentId INTEGER,
              mergeId INTEGER,
              mergeFlags TEXT%s

              %s);""" % (ns.qs_changesets, comma, defs))
        self.addColumnsTo('qs_changesets', 'changeset')

    def createMeta(self):
        self.cur.execute("""\
            create table if not exists %(qs_meta)s (
                name text not null,
                idx,
                value,
                primary key (name, idx));""" % self.ns)

    def createOidPool(self):
        self.cur.execute("""\
            create table if not exists %(qs_oidpool)s (
                nodeid integer primary key,
                oid integer,
                oid0 integer unique
                );""" % self.ns)
        self.cur.execute("insert or ignore into %(qs_oidpool)s "
                "(oid, oid0, nodeid) values (1000, 1000, 0)" % self.ns)

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def addColumnsTo(self, table, columnKey):
        """Allows adding payload columns to a revlog or changeset tables"""
        ns = self.ns
        table = ns[table]
        ex = self.cur.execute
        for cName, cDef in ns[columnKey+'List']:
            try:
                ex("alter table %s add column %s %s;" % (table, cName, cDef))
            except sqlite3.OperationalError:
                pass

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    #~ oid/node address space partitioning
    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    metadataView = metadataView

    def initOidSpace(self, host, oidSpace=1<<36, nodeSpace=1<<27):
        if nodeSpace*oidSpace > (1L<<63):
            raise ValueError(repr(nodeSpace, oidSpace, nodeSpace*oidSpace))

        meta = self.metadataView()
        oidSpace = meta.default('oid_space', oidSpace)
        nodeSpace = meta.default('node_space', nodeSpace)
        if nodeSpace*oidSpace > (1L<<63):
            raise ValueError(repr(nodeSpace, oidSpace, nodeSpace*oidSpace))

        self.ns.globalId = 0
        self.ns.nodeId = nodeId = utils.getnode()
        self.provisionNode(nodeId, nodeSpace, oidSpace)

    def provisionNode(self, nodeId, nodeSpace, oidSpace):
        ex = self.cur.execute; ns = self.ns
        r = ex("select oid from %(qs_oidpool)s where nodeid=?" % ns, (nodeId,))
        r = r.fetchone()
        if r is not None:
            return r[0]

        h = identhash.Int64IdentHasher()
        h.addInt(nodeId)
        h.addTimestamp()

        q = ("insert into %(qs_oidpool)s (oid, oid0, nodeId) values (?, ?, ?)" % ns)
        while 1:
            oid0 = (int(h) % nodeSpace) * oidSpace
            try:
                ex(q, (oid0, oid0, nodeId))
            except sqlite3.IntegrityError: 
                h.addTimestamp()
                continue

            return oid0

