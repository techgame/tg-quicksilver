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

import timeit
import random
import myModule

from TG.quicksilver.boundary import BoundaryVersions
random.seed()

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~ Definitions 
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def changeData(bs, root):
    mark = True
    kl = [myModule.A, myModule.B, myModule.B, myModule.C]
    if 0:
        root.data.extend(myModule.genList(10))
    elif 0:
        root.data.extend(myModule.genList(5, kl))
    elif 0:
        for x in xrange(3165/5):
            root.data.extend(myModule.genList(5, kl))
    elif 0:
        for x in xrange(200):
            root.data.extend(myModule.genList(5))
    elif 0:
        root.data.extend(myModule.genList(100))
    elif 0:
        for x in xrange(200):
            e = random.choice(root.data)
            if not isinstance(e, myModule.C):
                continue
            for x in xrange(10):
                e.data.extend(myModule.genList(5, kl))
                bs.mark(e)
    elif 1:
        n = 0; nAdded = 0
        if len(root.data) < 50:
            root.data.extend(myModule.genList(5))
        population = random.sample(root.data, min(len(root.data), 50))
        random.shuffle(population)
        while population:
            e = population.pop()

            if len(e.data)>4:
                count = 0
                random.shuffle(e.data)
                #e.data.append(myModule.genList(1, [myModule.A, myModule.B]))
            else:
                count = 5
                e.data.extend(myModule.genList(count))

            try: bs.mark(e)
            except LookupError, err: 
                nAdded += count
            else: 
                if not count: n += 1
                else: nAdded += count
        print 'items shuffled:', n, 'added:', nAdded
    else: 
        mark = False

    if mark:
        print 'marking root modified'
        bs.mark(root)


def saveData(bs, qsh, saveDirtyOnly=None):
    if saveDirtyOnly is not None:
        bs.saveDirtyOnly = saveDirtyOnly

    with qsh:
        bs.saveAll()


def main():
    qsh = BoundaryVersions('db_quicksilver.qag', 'obj')
    bs = qsh.boundaryWorkspace(stats=True)

    print 'bs.ws.cs:', bs.ws.cs
    if bs.ws.cs is None:
        bs.ws.cs = qsh.head()
        print bs.ws.cs

    print 'boundary store:', bs
    print 'workspace:', bs.ws

    root = bs.get(100, None)
    if root is None:
        root = myModule.Root()
        root.data = []
        bs.set(100, root)
        bs.saveAll()
        bs.commit()
        print 'created:', root
    else:
        print 'found:', root, len(root.data)

    raw = bs.raw(root)
    if raw is not None:
        print 'uncompressed: %12i' % (len(raw['payload']),)
        raw = bs.raw(root, False)
        print 'compressed:   %12i' % (len(raw['payload']),)
        print

    print root.data
    if not root.data:
        print 'adding data:'
        root.data = myModule.myData
        bs.mark(root)

    if 0:
        changeData(bs, root)
        saveData(bs, qsh, not False)
        if 0: bs.commit()

    changeData(bs, root)
    saveData(bs, qsh, not False)
    if 1: bs.commit()
    if 0: bs.ws.markCheckout()

    stats = getattr(bs, 'stats', None)
    if stats is not None:
        stats.printStats()

if __name__=='__main__':
    main()

