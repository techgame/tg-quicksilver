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

from ..base.changesView import ChangesViewAbstract
from .changeset import Changeset

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~ Definitions 
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def changesView(host):
    return ChangesView(host)

class ChangesView(ChangesViewAbstract):
    Changeset = Changeset

    def __init__(self, host):
        self.cur = host.cur
        self.ns = host.ns

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

