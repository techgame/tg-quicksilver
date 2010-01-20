#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~ Imports 
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

import timeit
import myModule

from TG.quicksilver.be.sqlite import Versions
from TG.quicksilver.boundary import BoundaryStore

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~ Definitions 
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

out = None
def test(bs, aList):
    global out
    oid = bs.set(None, aList)
    print 'stored at:', oid
    print 'aList:', len(aList)
    print 'out:', len(out)
    bm = len(out)
    for e in ['zlib', 'bz2']:
        sc = len(out.encode(e))
        print '  %s: %s B,  %.2f%% compressed' % (e, sc, 100*float(sc)/bm)
        t = timeit.Timer('out.encode(%r)'%(e,), 'from __main__ import out')
        t = t.timeit(10)/10.
        print '    @ %6.3f s, %.4f MBps' % (t, len(out)/(t*(1<<20)))

    print

    rezList = bs.decode(out)
    assert aList == rezList

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~ Main 
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def main():
    qsh = Versions('db_quicksilver.qag', 'obj')
    ws = qsh.workspace()
    bs = BoundaryStore(ws)

    print ws.cs
    if ws.cs is None:
        ws.cs = qsh.head()
    if ws.cs:
        print ws.cs
        #ws.checkout()

    root = bs.get(100, None)
    if root is None:
        root = myModule.Root()
        root.data = None
        bs.set(100, root)
        bs.saveAll()
        bs.commit()
        print 'created:', root
    else:
        print 'found:', root

    print
    if root.data:
        list(root.data)
        print 'data:', len(root.data)

    raw = bs.raw(root)
    if raw is not None:
        print 'uncompressed: %12i' % (len(raw.payload),)
        raw = bs.raw(root, False)
        print 'compressed:   %12i' % (len(raw.payload),)
        print

    if root.data is None:
        print 'adding data:'
        root.data = myModule.myData
    elif 1:
        root.data.extend(myModule.genList(10))
    elif 0:
        root.data.extend(myModule.genList(5, [myModule.C, myModule.C, myModule.C, myModule.A]))
    elif 0:
        root.data.extend(myModule.genList(100))

    with qsh:
        for entry, changed, datalen in bs.iterSaveAll():
            print 'save:', entry.oid, changed, datalen
        bs.commit()
    #test(bs, myModule.myData)

if __name__=='__main__':
    main()

