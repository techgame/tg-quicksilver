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

import os, sys
from TG.quicksilver.boundary import BoundaryVersions

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~ Definitions 
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

class GhostItem(object):
    def isOptionalItem(self):
        return False
    def isGhost(self):
        return True
    def answer(self):
        return (6,7)

class OptionalItem(object):
    def _boundary_(self, bndS, bndCtx):
        return True

    def _awakenBoundary_(self, bndEntry, bndCtx):
        if bndEntry.ghostOid:
            ghost = bndEntry.ghostObj
            assert ghost.answer() == (6,7)
    def _updateBoundary_(self, bndEntry, bndCtx):
        if not bndEntry.hasGhost():
            gi = GhostItem()
            bndEntry.setGhostObj(gi)

    def answer(self):
        return 42

    def isOptionalItem(self):
        return True

_OptionalItem = OptionalItem

def openBoundaryStore():
    dbFn = os.path.splitext(__file__)[0]+'.qag'
    qsh = BoundaryVersions(dbFn, 'obj')
    bs = qsh.boundaryWorkspace()
    return bs


def testStore():
    bs = openBoundaryStore()
    root = OptionalItem()
    bs.set(100, root)
    bs.commit()

    assert root.isOptionalItem(), root
    assert root.answer() == 42

def testReadback():
    bs = openBoundaryStore()
    root = bs.get(100)
    assert root.isOptionalItem(), root
    assert root.answer() == 42

def testGhost():
    global OptionalItem
    del OptionalItem

    bs = openBoundaryStore()
    root = bs.get(100)
    assert not root.isOptionalItem(), root
    assert root.isGhost(), root
    assert root.answer() == (6,7)

def testGhostRestore():
    global OptionalItem
    OptionalItem = _OptionalItem

    bs = openBoundaryStore()
    root = bs.get(100)
    assert root.isOptionalItem(), root
    assert root.answer() == 42

