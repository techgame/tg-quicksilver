#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~ Imports 
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

from ..base.changeset import ChangesetBase
from .utils import OpBase
 
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~ Definitions 
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

class Changeset(OpBase, ChangesetBase):
    def markChangeset(self, mark='mark'):
        self.updateState(mark)

    def _selectEntryItems(self, columns=None):
        """Returns a list of the form [(name, value)]"""
        if columns is not None:
            columns = ['%s=?'%(c,) for c in columns]
        else: columns = ['*']

        stmt  = "select %s \n" % (', '.join(columns),)
        stmt += "  from %s \n" % (self.ns.qs_changesets,)
        stmt += "  where versionId=?"
        res = self.cur.execute(stmt, [self.versionId])
        r = res.fetchone()
        return zip(r.keys(), r)

    def _insertEntryColumnsValues(self, columns, values):
        columns = list(columns)
        values = list(values)
        if len(columns) != len(values):
            raise ValueError("Mismatched columns and values")

        values.insert(0, long(self.versionId))
        stmt  = "insert into %s \n" % (self.ns.qs_changesets,)
        stmt += "  (versionId%s) values (?%s) \n" % (
                  (','+','.join(columns)), (',?'*len(columns)))
        return self.cur.execute(stmt, values)

    def _updateEntryColumnsValues(self, columns, values):
        columns = ['%s=?'%(c,) for c in columns]
        values = list(values)
        if len(columns) != len(values):
            raise ValueError("Mismatched columns and values")

        stmt  = "update %s \n" % (self.ns.qs_changesets,)
        stmt += "  set %s \n" % (', '.join(columns),)
        stmt += "  where versionId=? "
        values.append(self.versionId)
        return self.cur.execute(stmt, values)

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def _bindSelectParent(self):
        """Bind fn(versionId) => [(parent versionId, state)] for the matching parents"""
        return self._bindExecute(
            'select parentId,state  \n'
            '  from %(qs_changesets)s \n'
            '    where versionId=?')

    def _bindSelectMergeParent(self):
        """Bind fn(versionId) => [(merge parent versionId, state)] for the matching merge parents"""
        return self._bindExecute(self.cur, 
            'select parentId,state  \n'
            '  from %(qs_changesets)s \n'
            '    where mergeId=?')

    def _bindSelectChildren(self):
        """Bind fn(versionId) => [(child versionId, state)] for the matching children"""
        return self._bindExecute(self.cur, 
            'select versionId, state \n'
            '  from %(qs_changesets)s \n'
            '  where parentId=?')

