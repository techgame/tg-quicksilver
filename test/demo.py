#!/usr/bin/env python
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~ Imports 
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

import datetime
import sqlite3
from TG.quicksilver.sqlite.versions import Versions

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~ Main 
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

if __name__=='__main__':
    db = sqlite3.connect('db_quicksilver.qag')
    qsh = Versions(db, 'obj')

    ws = qsh.workspace()
    for e in ws.readAll():
        print e.tags, str(e.payload).decode('hex')

    with db:
        dt = datetime.datetime.now()
        payload = str(dt).encode('hex')
        ws.write(None, payload=buffer(payload))

