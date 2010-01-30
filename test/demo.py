##~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~##
##~ Copyright (C) 2002-2010  TechGame Networks, LLC.              ##
##~                                                               ##
##~ This library is free software; you can redistribute it        ##
##~ and/or modify it under the terms of the MIT style License as  ##
##~ found in the LICENSE file included with this distribution.    ##
##~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~##

#!/usr/bin/env python
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~ Imports 
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

import os
from pprint import pprint
import datetime
import sqlite3

from TG.quicksilver.be.sqlite import Versions

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~ Main 
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

if __name__=='__main__':
    qsh = Versions('db_quicksilver.qag', 'obj')

    print 'heads:', list(qsh.heads())
    print 'roots:', list(qsh.versions().roots())
    print 'orphan:', list(qsh.versions().orphanIds())

    ws = qsh.workspace()
    print ws.cs

    if ws.cs is None:
        ws.cs = qsh.head()

    if ws.cs:
        print ws.cs
        ws.checkout()

    for e in ws.readAll():
        #print e
        print e.oid, str(e.payload)

    if 1:
        oid = None
        #oid = 42
        print
        print "adding new thingy at:", oid
        print
        with qsh:
            dt = datetime.datetime.now()
            data = "data/" + os.urandom(4).encode('hex')+'/end'

            ws.write(oid, payload=buffer(data))
            ws.commit()

        for e in ws.readAll():
            #print e
            print e.oid, str(e.payload)

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
            hi = qsh.heads()
            cs0 = next(hi)
            cs1 = next(hi)

            cc = cs0.newChild()
            cc.merge(cs1)

