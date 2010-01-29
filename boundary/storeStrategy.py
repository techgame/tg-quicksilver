#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~ Imports 
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

from .ambit import IBoundaryStrategy, PickleAmbitCodec
from .reference import BoundaryReferenceRegistry

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~ Definitions 
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

BoundaryAmbitCodec = PickleAmbitCodec

class BasicBoundaryStrategy(IBoundaryStrategy):
    """This strategy asks objects for a _boundary_(bndCtx) method.  If it
    exists, it is called.  It can return a new reference object, an oid, or
    True to generate a new oid"""

    def __init__(self, store, bndCtx):
        self.store = store
        self.oidForObj = store.reg.oidForObj
        self.bndCtx = bndCtx

    def setBoundaryRef(self, oid, obj=False):
        self.targetOid = oid

    def objForRef(self, ref):
        if isinstance(ref, (int,long)):
            return self.store.ref(ref)

        # use BoundaryReference.boundaryRef protocol
        return ref.boundaryRef(self)

    def refForObj(self, obj):
        # this is one long method to keep it fast
        if isinstance(obj, type): 
            return # We don't accept type based sentinals
        fnBoundary = getattr(obj, '_boundary_', False)
        if not fnBoundary: 
            return # sentinal not found, cannot be reference

        # common boundary protocol:
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
#~ Strategy with registerable custom types
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

class BoundaryStrategy(BasicBoundaryStrategy, BoundaryReferenceRegistry):
    def refForObj(self, obj):
        # this is one long method to keep it fast
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
    def refForObj(self, obj):
        # this is one long method to keep it fast
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

