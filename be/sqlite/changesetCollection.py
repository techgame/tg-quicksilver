#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~ Imports 
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

from itertools import islice
from collections import defaultdict
from .changeset import Changeset

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~ Definitions 
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

class ChangesetCollectionView(object):
    def __init__(self, host):
        self.cur = host.cur
        self.ns = host.ns

    def root(self, i=None, cols=[]):
        res = self.roots(cols)
        if i is not None:
            res = islice(res, i, i+1)
        return next(res, None) or self._asChangeset(None)
    def roots(self, cols=[]):
        stmt = "select versionId"
        if cols:
            stmt += "," + ",".join(cols)
        stmt += """
            from %(qs_changesets)s
              where parentId is null
            order by ts desc;"""
        res = self.cur.execute(stmt % self.ns)
        res = (self._asChangeset(*e) for e in res)
        return res

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def head(self, i=None, cols=[]):
        res = self.heads(cols)
        if i is not None:
            res = islice(res, i, i+1)
        return next(res, None) or self._asChangeset(None)
    def heads(self, cols=[]):
        stmt = "select versionId"
        if cols:
            stmt += "," + ",".join(cols)
        stmt += """
            from %(qs_changesets)s
              join (select versionId from %(qs_changesets)s 
                    except select parentId from %(qs_changesets)s
                    except select mergeId from %(qs_changesets)s
                    )
                using (versionId)
            order by ts desc;"""
        res = self.cur.execute(stmt % self.ns)
        res = (self._asChangeset(*e) for e in res)
        return res

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def orphanIds(self):
        res = self.cur.execute("""\
            select distinct parentId from %(qs_changesets)s 
                where parentId not null
                except select versionId from %(qs_changesets)s
            ;""" % self.ns)
        return res

    def allIds(self):
        stmt = "select versionId, parentId, mergeId from %(qs_changesets)s;"
        return self.cur.execute(stmt % self.ns)

    def lineage(self):
        res = self.allIds()

        r = defaultdict(list)
        for vid, pid, mid in res:
            v = Changeset(self, vid)
            e = (v, [], r[vid])
            r[pid].append(e)
            if mid is not None:
                r[mid].append(e)

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

    def _asChangeset(self, vid, *rest):
        cs = Changeset(self, vid)
        if rest: return (cs,)+rest
        else: return cs

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def changesetCollectionView(host):
    return ChangesetCollectionView(host)

