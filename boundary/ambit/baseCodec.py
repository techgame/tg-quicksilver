#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~ Imports 
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

from .. import NotStorableMixin

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~ Definitions 
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

class BaseAmbitCodec(NotStorableMixin):
    def __init__(self, idForObj=None, objForId=None):
        self.init(idForObj, objForId)

    def init(self, idForObj=None, objForId=None):
        self._initEncoder(idForObj)
        self._initDecoder(objForId)

    def _initEncoder(self, idForObj=None):
        raise NotImplementedError('Subclass Responsibility: %r' % (self,))
    def _initDecoder(self, objForId=None):
        raise NotImplementedError('Subclass Responsibility: %r' % (self,))

    def dump(self, obj, hash=True):
        """Return serialized data from obj, and hash if hash is True"""
        raise NotImplementedError('Subclass Responsibility: %r' % (self,))
    def load(self, data, meta=None):
        """Return object unserialized from data"""
        raise NotImplementedError('Subclass Responsibility: %r' % (self,))

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

class BaseAmbitStrategy(NotStorableMixin):
    #Codec = some subclass of BaseAmbitCodec

    def __init__(self, host):
        self.host = host
        self._objCache = host._objCache
        self._codec = self.Codec(self._refForObj, self._objForRef)
        self._targetOid = None

    def dump(self, oid, obj):
        if self._targetOid is not None:
            raise RuntimeError("TargetOID is not None: unexpected recursion")

        self._targetOid = oid
        r = self._codec.dump(obj, True)
        self._targetOid = None
        return r # data, hash

    def load(self, oid, data, meta=None):
        if self._targetOid is not None:
            raise RuntimeError("TargetOID is not None: unexpected recursion")

        self._targetOid = oid
        obj = self._codec.load(data, meta)
        self._targetOid = None
        return obj

    def _objForRef(self, ref):
        return self.host.get(ref)

    def _refForObj(self, obj):
        if isinstance(obj, type): 
            return
        if getattr(obj, '__bounded__', False):
            oid = self._objCache.get(obj, None)
            if oid is None:
                return self.host.set(None, obj, True)
            elif oid != self._targetOid:
                return oid

