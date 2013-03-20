# -*- coding: utf-8 -*- vim: set ts=4 sw=4 expandtab
##~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~##
##~ Copyright (C) 2002-2013  TechGame Networks, LLC.              ##
##~                                                               ##
##~ This library is free software; you can redistribute it        ##
##~ and/or modify it under the terms of the MIT style License as  ##
##~ found in the LICENSE file included with this distribution.    ##
##~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~##

import os, sys, random

from TG.quicksilver.be.flat import FlatWorkspace
from TG.quicksilver.be.transfer import TransferWorkspace
from TG.quicksilver.boundary import BoundaryStore

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~ Definitions 
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def openBoundaryStore():
    dbFn = os.path.splitext(__file__)[0]+'.qap'
    ws0 = FlatWorkspace(dbFn)
    ws1 = FlatWorkspace(dbFn)
    ws = TransferWorkspace(ws0, ws1)
    qsh = BoundaryStore(ws)
    return qsh

def main():
    qsh = openBoundaryStore()

if __name__=='__main__':
    main()

