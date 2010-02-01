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

import contextlib
from . import ambit
from .reference import BoundaryReferenceRegistry

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~ Definitions 
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

BoundaryAmbitCodec = ambit.PickleAmbitCodec
BoundaryRefWalker = ambit.PickleRefWalker

_fastOutTypes_ = (
    type(None), type, 
    str, unicode, 
    bool, int, long, float, complex,
    tuple, list, set, dict, )

class BasicBoundaryStrategy(ambit.IBoundaryStrategy):
    """This strategy asks objects for a _boundary_(bndCtx) method.  If it
    exists, it is called.  It can return a new reference object, an oid, or
    True to generate a new oid"""

    def __init__(self, store, context):
        self.store = store
        self.oidForObj = store.reg.oidForObj
        self.context = context

    @contextlib.contextmanager
    def inBoundaryCtx(self, oid=None, obj=False):
        self.targetOid = oid
        self.bndCtx = self.context
        yield None

    def objForRef(self, ref):
        if isinstance(ref, (int,long)):
            return self.store.ref(ref)

        # use BoundaryReference.boundaryRef protocol
        return ref.boundaryRef(self, self.bndCtx)

    def refForObj(self, obj, _fastOutTypes_=_fastOutTypes_):
        # this is one long method to keep it fast
        if obj.__class__ in _fastOutTypes_:
            return
        if isinstance(obj, type): 
            return # We don't accept type based sentinals
        fnBoundary = getattr(obj, '_boundary_', False)
        if not fnBoundary: 
            return # sentinal not found, cannot be reference

        # common boundary protocol:
        oid = self.oidForObj(obj)
        if oid is not None:
            if oid == self.targetOid:
                # call boundary on the target for continuity
                fnBoundary(self.bndCtx)
                oid = None
            return oid
        else:
            ref = fnBoundary(self.bndCtx)
            if ref is True: 
                ref = self.store.set(None, obj, True)
            return ref or None

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~ Strategy with registerable custom types
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

class BoundaryStrategy(BasicBoundaryStrategy, BoundaryReferenceRegistry):
    def refForObj(self, obj, _fastOutTypes_=_fastOutTypes_):
        # this is one long method to keep it fast
        if obj.__class__ in _fastOutTypes_:
            return
        if isinstance(obj, type): 
            return # We don't accept type based sentinals

        # test for custom ref types that don't support _boundary_ protocol
        # using isinstance, and delegate responsibility to refForCustomType
        if isinstance(obj, self.customRefTypes):
            return self.refForCustomType(obj)

        # common boundary protocol:
        fnBoundary = getattr(obj, '_boundary_', False)
        if not fnBoundary: 
            return # sentinal not found, cannot be reference

        oid = self.oidForObj(obj)
        if oid is not None:
            if oid == self.targetOid:
                oid = None
            return oid
        else:
            ref = fnBoundary(self.bndCtx)
            if ref is True: 
                ref = self.store.set(None, obj, True)
            return ref or None

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

class BoundaryStrategyByType(BoundaryStrategy):
    def refForObj(self, obj, _fastOutTypes_=_fastOutTypes_):
        # this is one long method to keep it fast
        if obj.__class__ in _fastOutTypes_:
            return
        if isinstance(obj, type): 
            return # We don't accept type based sentinals

        # test for custom ref types that don't support _boundary_ protocol
        # using issubclass(type(obj)) to bypass proxy trickery, and delegate
        # responsibility to refForCustomType
        if issubclass(type(obj), self.customRefTypes):
            return self.refForCustomType(obj)

        # common boundary protocol:
        fnBoundary = getattr(obj, '_boundary_', False)
        if not fnBoundary: 
            return # sentinal not found, cannot be reference

        oid = self.oidForObj(obj)
        if oid is not None:
            if oid == self.targetOid:
                oid = None
            return oid
        else:
            ref = fnBoundary(self.bndCtx)
            if ref is True: 
                ref = self.store.set(None, obj, True)
            return ref or None

    def refForCustomType(self, refObj):
        try: objKlass = type(refObj)
        except Exception: 
            objKlass = refObj.__class__

        refFactory = self.factoryForCustomType(objKlass)
        return refFactory(self, refObj)

