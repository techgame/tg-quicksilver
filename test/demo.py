#!/usr/bin/env python
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~ Imports 
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

from pprint import pprint
import datetime
import sqlite3

from TG.quicksilver.be.sqlite import Versions
from TG.quicksilver.be.sqlite import Changeset

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~ Main 
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

if __name__=='__main__':
    qsh = Versions('db_quicksilver.qag', 'obj')

    print 'root:', list(qsh.roots())
    print 'head:', list(qsh.heads())
    print 'orphan:', list(qsh.orphanIds())

    ws = qsh.workspace()
    #ws.cs = None
    ws.checkout()
    #cs = ws.checkout(qsh.head())

    for e in ws.readAll():
        #print e
        print e.oid, e.payload, str(e.payload).decode('hex')

    if 1:
        oid = None
        #oid = 42
        print
        print "adding new thingy at:", oid
        print
        with qsh:
            dt = datetime.datetime.now()
            data = "charlie tuna!"

            ws.write(oid, payload=buffer(data.encode('hex')))
            ws.commit()

        for e in ws.readAll():
            #print e
            print e.oid, e.payload, str(e.payload).decode('hex')

    if 0:
        r = qsh.lineage()
        pprint(r)

    if 0:
        print
        print 'iterParentIds:', cs, cs.csParent
        for vid, state in cs.iterParentIds():
            print '  ', vid, state
        print

    if 0:
        print
        print 'iterChildIds:'
        for vid, state in cs.parentCS.iterChildIds():
            print '  ', vid, state
        print

    if 0:
        with qsh:
            csParent = Changeset(ws, cs.parentId)
            csPeer = csParent.newChild()

    if 0:
        with qsh:
            hi = qsh.heads()
            cs0 = next(hi)
            cs1 = next(hi)

            cc = cs0.newChild()
            cc.merge(cs1)

