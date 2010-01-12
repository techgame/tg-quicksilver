#!/usr/bin/env python
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~ Imports 
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

from pprint import pprint
import datetime
import sqlite3

from TG.quicksilver.sqlite.versions import Versions
from TG.quicksilver.sqlite.changeset import Changeset

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~ Main 
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

if __name__=='__main__':
    db = sqlite3.connect('db_quicksilver.qag',
            detect_types=sqlite3.PARSE_DECLTYPES)
    #db.detect_types=sqlite3.PARSE_DECLTYPES
    qsh = Versions(db, 'obj')

    print 'root:', list(qsh.rootIds())
    print 'head:', list(qsh.headIds())
    print 'orphan:', list(qsh.orphanIds())

    ws = qsh.workspace()
    for e in ws.readAll():
        print e.tags, str(e.payload).decode('hex')

    with db:
        dt = datetime.datetime.now()
        payload = str(dt).encode('hex')
        #ws.write(None, payload=buffer(payload))

    if 1:
        r = qsh.lineage()
        pprint(r)
    if 0:
        with db:
            cs = Changeset(ws)
            cs.init(next(qsh.headIds(), None))
            print (cs.versionId, cs.parentId)
            pcs = Changeset(ws, cs.parentId)
            pcs.init()

            if 1:
                ccs = cs.newChild()
                print (ccs.versionId, ccs.parentId)

            if 1:
                print 'iterParentIds:'
                for vid, state in pcs.iterParentIds():
                    print '  ', vid, state

                print 'iterChildIds:'
                for vid, state in pcs.iterChildIds():
                    print '  ', vid, state

