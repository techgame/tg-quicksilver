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

import weakref

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~ Definitions 
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

class BoundaryReferenceRegistry(object):
    customRefTypes = set() # class global
    customRefMap = dict() # class global

    @classmethod
    def registerCustomRef(klass, refFactory, types=None):
        if types is None:
            types = refFactory.types

        if not isinstance(types, (list, tuple, set)):
            types = (types,)
        for sc in types:
            if not isinstance(sc, type):
                raise ValueError("Subclass %r must be an instance of type"%(sc,))

        klass.customRefMap.update(
            [(sc, refFactory) for sc in types])
        klass.customRefTypes = tuple(klass.customRefMap.keys())

    def factoryForCustomType(self, objKlass):
        findRefFactory = self.customRefMap.get

        for cls in objKlass.__mro__:
            refFactory = findRefFactory(cls)
            if refFactory is not None:
                return refFactory

        raise RuntimeError("Factory not found for registered custom type")

    def refForCustomType(self, refObj):
        refFactory = self.factoryForCustomType(refObj.__class__)
        return refFactory(self, refObj)

    @classmethod
    def classPromoteGlobals(klass):
        # class registry globals
        klass.customRefMap = klass.customRefMap.copy()
        return klass.registerCustomRef

registerCustomRef = BoundaryReferenceRegistry.registerCustomRef

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~ BoundaryReference API
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

class IBoundaryReference(object):
    types = []
    def __init__(self, strategy, refObj):
        raise NotImplementedError('Subclass Responsibility: %r' % (self,))
    def boundaryRef(self, strategy, bndCtx):
        raise NotImplementedError('Subclass Responsibility: %r' % (self,))

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

class BoundaryWeakref(object):
    types = [weakref.ref]
    def __init__(self, strategy, refObj):
        self.wr_obj = refObj()
    def boundaryRef(self, strategy, bndCtx):
        return weakref.ref(self.wr_obj)

registerCustomRef(BoundaryWeakref)

