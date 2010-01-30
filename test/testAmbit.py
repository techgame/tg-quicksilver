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
import myModule

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~ Definitions 
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

out = None
def test(aList):
    global out
    out, hash = bs.dump(aList)
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

    rezList = bs.load(out)
    assert aList == rezList

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~ Main 
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

if __name__=='__main__':
    bs = myModule.DemoCodec()
    bs.init()
    test(myModule.myData)

    if 1:
        bl = myModule.genList(20)
        test(bl)

    if 1:
        bl = myModule.genList(100)
        test(bl)

