#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~ Imports 
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

from itertools import islice
from collections import defaultdict

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~ Definitions 
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

class ChangesViewAbstract(object):
    def root(self, i=None, columns=[]):
        res = self.roots(columns)
        if i is not None:
            res = islice(res, i, i+1)
        return next(res, None) or self._asChangeset(None)
    def roots(self, columns=[]):
        raise NotImplementedError('Subclass Responsibility: %r' % (self,))

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def head(self, i=None, columns=[]):
        res = self.heads(columns)
        if i is not None:
            res = islice(res, i, i+1)
        return next(res, None) or self._asChangeset(None)
    def heads(self, columns=[]):
        raise NotImplementedError('Subclass Responsibility: %r' % (self,))

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def orphanIds(self):
        raise NotImplementedError('Subclass Responsibility: %r' % (self,))

    def allIds(self):
        raise NotImplementedError('Subclass Responsibility: %r' % (self,))

    def lineage(self):
        res = self.allIds()

        Changeset = self.Changeset
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
        cs = self.Changeset(self, vid)
        if rest: return (cs,)+rest
        else: return cs

