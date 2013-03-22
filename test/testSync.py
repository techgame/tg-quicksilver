# -*- coding: utf-8 -*- vim: set ts=4 sw=4 expandtab
##~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~##
##~ Copyright (C) 2002-2013  TechGame Networks, LLC.              ##
##~                                                               ##
##~ This library is free software; you can redistribute it        ##
##~ and/or modify it under the terms of the MIT style License as  ##
##~ found in the LICENSE file included with this distribution.    ##
##~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~##

import os, sys, random
import contextlib

from TG.quicksilver.be.flat import FlatWorkspace
from TG.quicksilver.boundary import BoundaryStore

import myModule

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~ Definitions 
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def openBoundaryStore(name):
    ws = FlatWorkspace(None)
    bs = BoundaryStore(ws)
    ws.on_updatedOids = bs.bindSyncUpdateMap('updated')
    return bs

def main():
    a = openBoundaryStore('a')
    b = openBoundaryStore('b')

    print
    print 'A:'
    a.set(100, myModule.myData)
    a_l = a.get(100)
    print id(a_l), a_l

    b.ws.syncUpdateFrom(a)

    print
    print 'B:'
    b_l = b.get(100)
    del b_l[2:]
    print id(b_l), b_l
    b.mark(100)
    print b.write(100)
    b.commit()

    print
    print 'B->A:'
    a2_l = a.get(100)
    print id(a2_l), a2_l
    b.ws.syncUpdateTo(a)
    a3_l = a.get(100)
    print id(a2_l), a2_l
    print id(a3_l), a3_l

if __name__=='__main__':
    main()


