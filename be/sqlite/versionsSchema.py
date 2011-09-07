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
        #'qs_view',
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

        ns.nodeId = utils.getnode()
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
            self.createView()
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

    def createView(self):
        return 
        self.cur.execute("""\
            create view if not exists %(qs_view)s as 
                select * from %(qs_version)s as S
                join %(qs_revlog)s using (revId, oid)
            ;""" % self.ns)
    
    def createMeta(self):
        self.cur.execute("""\
            create table if not exists %(qs_meta)s (
                name text not null,
                idx,
                value,
                primary key (name, idx));""" % self.ns)

    def createOidPool(self):
        ex = self.cur.execute; ns = self.ns
        ex("""create table if not exists %(qs_oidpool)s (
                nodeid integer primary key,
                oid integer,
                oid0 integer unique
                );""" % ns)
        r = ex("select oid0 from %(qs_oidpool)s where nodeid=0" % ns).fetchone()
        if r is None:
            ex("insert or ignore into %(qs_oidpool)s "
                "(oid, oid0, nodeid) values (1000, 1000, 0)" % ns)

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def addColumnsTo(self, table, columnKey):
        """Allows adding payload columns to a revlog or changeset tables"""
        ex = self.cur.execute; ns = self.ns
        table = ns[table]
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
        meta = self.metadataView()
        oidSpace = meta.default('oid_space', oidSpace)
        nodeSpace = meta.default('node_space', nodeSpace)
        if nodeSpace*oidSpace > (1L<<63):
            raise ValueError(repr(nodeSpace, oidSpace, nodeSpace*oidSpace))
        self.ns.oidNodeSpace = oidSpace, nodeSpace

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    #~ Entire database branching
    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def initFromBranch(self, dbBranch, conn):
        if dbBranch is None:
            return None
        
        dbName = 'db_branch'
        self.ns['db_branch'] = dbName
        conn.execute('ATTACH DATABASE ? as ?', (dbBranch, dbName))
        with conn:
            ns = self.ns
            for k in self.sqlObjects:
                k = ns[k]
                try: conn.execute('insert or replace into %s select * from %s.%s' % (k,dbName,k))
                except sqlite3.OperationalError: 
                    pass

        with conn:
            # recreate and copy the more dynamic extern tables
            srcMeta = self.metadataView(qs_meta='%(db_branch)s.%(qs_meta)s'%self.ns)
            tables = [(k, conn.execute(q))[0] for k,q in srcMeta.iter('extern:sql-tables')]
            indexes = [(k, conn.execute(q))[0] for k,q in srcMeta.iter('extern:sql-indexes')]

        with conn:
            for k in tables:
                try: conn.execute('insert or replace into %s select * from %s.%s' % (k,dbName,k))
                except sqlite3.OperationalError:
                    pass

