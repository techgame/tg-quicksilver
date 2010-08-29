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
from TG.quicksilver.boundary import BoundaryVersions, OidLookupError

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

class NormalItem(object):
    def _boundary_(self, bndS, bndCtx):
        return True

    def isOptionalItem(self):
        return False

    def answer(self):
        return 1942

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

def openBoundaryStore():
    dbFn = os.path.splitext(__file__)[0]+'.qag'
    qsh = BoundaryVersions(dbFn, 'obj')
    bs = qsh.boundaryWorkspace()
    return bs


def testStore():
    bs = openBoundaryStore()
    root = OptionalItem()
    root.state = (2,3,7)
    bs.set(100, root)
    bs.set(101, NormalItem())
    bs.commit()

    assert root.isOptionalItem(), root
    assert root.answer() == 42

def testReadback():
    bs = openBoundaryStore()
    root = bs.get(100)
    assert root.isOptionalItem(), root
    assert root.answer() == 42
    assert root.state == (2,3,7)

    r2 = bs.get(101)
    assert not r2.isOptionalItem(), r2
    assert r2.answer() == 1942

def testNoGhost():
    global NormalItem
    _NormalItem = NormalItem
    del NormalItem

    try:
        bs = openBoundaryStore()
        root = bs.get(100)
        assert root.isOptionalItem(), root
        assert root.answer() == 42
        assert root.state == (2,3,7)

        try:
            r2 = bs.get(101)
            assert False, "Should have raised an error"
        except OidLookupError, err:
            print err
    finally:
        NormalItem = _NormalItem


def testGhost():
    global OptionalItem
    _OptionalItem = OptionalItem
    del OptionalItem

    try:
        bs = openBoundaryStore()
        root = bs.get(100)
        assert not root.isOptionalItem(), root
        assert root.isGhost(), root
        assert root.answer() == (6,7)
    finally:
        OptionalItem = _OptionalItem

def testGhostFail():
    global GhostItem
    _GhostItem = GhostItem
    del GhostItem

    try:
        bs = openBoundaryStore()
        try:
            root = bs.get(100)
            assert False, "Should have raised an error"
        except OidLookupError, err:
            print err

        r2 = bs.get(101)
        assert not r2.isOptionalItem(), r2
        assert r2.answer() == 1942

    finally:
        GhostItem = _GhostItem

def testGhostRestore():
    bs = openBoundaryStore()
    root = bs.get(100)
    assert root.isOptionalItem(), root
    assert root.answer() == 42
    assert root.state == (2,3,7)

    r2 = bs.get(101)
    assert not r2.isOptionalItem(), r2
    assert r2.answer() == 1942

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def excepthook(excType, exc, tb): 
    pass

def setup():
    excepthook.state = (sys.excepthook, sys.stderr)
    sys.stderr = sys.stdout
def teardown():
    sys.excepthook, sys.stderr = excepthook.state
    excepthook.state = None

