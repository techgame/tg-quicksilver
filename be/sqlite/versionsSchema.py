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
        'qs_changesets',
        'qs_revlog',
        'qs_manifest',
        'qs_view',
        'qs_meta', ]

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
            self.createManifest()
            self.createChangesets()
            self.createView()
            self.createMeta()

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
              revId INTEGER, 
              oid INTEGER not null,

              %(payloadDefs)s,
              
              primary key (revId, oid) on conflict replace
              );""" % self.ns)

        self.addColumnsTo('qs_revlog', 'payload')

    def createManifest(self):
        ns = self.ns
        self.cur.execute("""\
            create table if not exists %(qs_manifest)s (
              versionId INTEGER,
              oid INTEGER,
              revId INTEGER,
              flags INTEGER,

              primary key (versionId, oid) on conflict replace
            );""" % ns)
        self.cur.execute("""\
            create index if not exists %(qs_manifest)s_index 
                on %(qs_manifest)s (versionId);""" % ns)

    def createView(self):
        self.cur.execute("""\
            create view if not exists %(qs_view)s as 
              select * from %(qs_manifest)s as S
                join %(qs_revlog)s using (revId, oid)
            ;""" % self.ns)
                
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

              %s
            );""" % (ns.qs_changesets, comma, defs))
        self.addColumnsTo('qs_changesets', 'changeset')

    def createMeta(self):
        self.cur.execute("""\
            create table if not exists %(qs_meta)s (
                name text not null,
                idx,
                value,
                primary key (name, idx)
            );""" % self.ns)

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

    def findOidRangeMax(self, oid0, oidSpace):
        oid1 = oid0+oidSpace
        stmt  = 'select max(oid) from %(qs_revlog)s \n' % self.ns
        where = '  where ? <= oid and oid < ?'
        r = self.cur.execute(stmt+where, (oid0, oid1))
        oid = r.fetchone()[0]
        if oid is not None: oid += 1
        else: oid = oid0
        return (oid, oid1)

    def provisionNode(self, nodeId, nodeSpace, oidSpace, nodeTable):
        oid0 = nodeTable[nodeId]
        if oid0 is not None: 
            return oid0

        h = identhash.Int64IdentHasher()
        h.addInt(nodeId)
        h.addTimestamp()

        allOidBases = set(e[1] for e in nodeTable)
        allOidBases.add(0)

        while 1:
            oid0 = (int(h) % nodeSpace) * oidSpace
            if oid0 not in allOidBases:
                break

        nodeTable[nodeId] = oid0
        return oid0

    def initOidSpace(self, host, oidSpace=1<<36, nodeSpace=1<<27):
        if nodeSpace*oidSpace > (1L<<63):
            raise AssertionError(repr(nodeSpace, oidSpace, nodeSpace*oidSpace))

        meta = self.metadataView()
        oidSpace = meta.default('oid_space', oidSpace)
        nodeSpace = meta.default('node_space', nodeSpace)
        if nodeSpace*oidSpace > (1L<<63):
            raise AssertionError(repr(nodeSpace, oidSpace, nodeSpace*oidSpace))

        oidRoot = self.findOidRangeMax(0, oidSpace)[0]

        nodeId = utils.getnode()
        oidNode = self.provisionNode(nodeId, nodeSpace, oidSpace, meta['nodeid_offset'])
        oidNode = self.findOidRangeMax(oidNode, oidSpace)[0]

        ns = self.ns
        ns.nodeId = nodeId
        ns.newRootOid = itertools.count(oidRoot).next
        ns.newNodeOid = itertools.count(oidNode).next
        ns.newOid = ns.newNodeOid

