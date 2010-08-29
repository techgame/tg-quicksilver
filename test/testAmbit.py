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
def ambit_list(aList):
    global out
    out = bs.dump(aList)
    hash = bs.hashDigest(out)
    bm = len(out)
    for e in ['zlib', 'bz2']:
        sc = len(out.encode(e))
        out.encode(e)

    rezList = bs.load(out)
    assert aList == rezList

bs = None
def setup():
    global bs
    bs = myModule.DemoCodec()

def testAmbit():
    ambit_list(myModule.myData)

    if 1:
        bl = myModule.genList(20)
        ambit_list(bl)

    if 1:
        bl = myModule.genList(100)
        ambit_list(bl)


def teardown():
    global bs
    del bs

