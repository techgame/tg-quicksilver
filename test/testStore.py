#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~ Imports 
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

import timeit
import random
import myModule

from TG.quicksilver.be.sqlite import Versions
from TG.quicksilver.boundary import BoundaryStore, StatsBoundaryStore
random.seed()

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
    if 0:
        bs = BoundaryStore(ws)
    else:
        bs = StatsBoundaryStore(ws)

    print ws.cs
    if ws.cs is None:
        ws.cs = qsh.head()
    if ws.cs:
        print ws.cs
        #ws.checkout()

    root = bs.get(100, None)
    if root is None:
        root = myModule.Root()
        root.data = []
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

    mark = True
    if not root.data:
        print 'adding data:'
        root.data = myModule.myData
    elif 1:
        root.data.extend(myModule.genList(10))
    elif 0:
        root.data.extend(myModule.genList(5, [myModule.C, myModule.C, myModule.C, myModule.A]))
    elif 0:
        for x in xrange(200):
            root.data.extend(myModule.genList(5))
    elif 0:
        root.data.extend(myModule.genList(100))
    elif 0:
        for x in xrange(200):
            e = random.choice(root.data)
            e.data.extend(myModule.genList(5))
    elif 0:
        n = 0
        if len(root.data) < 50:
            root.data.extend(myModule.genList(5))
        population = random.sample(root.data, min(len(root.data), 50))
        random.shuffle(population)
        while population:
            e = population.pop()
            if len(e.data)>4:
                random.shuffle(e.data)
                #e.data.append(myModule.genList(1, [myModule.A, myModule.B]))
            else:
                e.data.extend(myModule.genList(5))

            try: bs.mark(e)
            except LookupError: pass
            else: n += 1
        print 'items shuffled:', n
    else: 
        mark = False

    if mark:
        print 'marking root modified'
        bs.mark(root)

    bs.saveDirtyOnly = False
    with qsh:
        if 1:
            bs.saveAll()
        else:
            for entry, r in bs.iterSaveAll():
                print 'save:', entry.oid, r

        if 0:
            print "ws.backout(None):"
            ws.backout(None)

        if 1:
            bs.commit()


    stats = getattr(bs, 'stats', None)
    if stats is not None:
        stats.printStats()
    #test(bs, myModule.myData)

if __name__=='__main__':
    main()

