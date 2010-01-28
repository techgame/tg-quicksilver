#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~ Imports 
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

from ...mixins import NotStorableMixin

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
    def load(self, data):
        """Return object unserialized from data"""
        raise NotImplementedError('Subclass Responsibility: %r' % (self,))

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

class BaseAmbitStrategy(NotStorableMixin):
    Codec = None  # some subclass of BaseAmbitCodec

    def __init__(self, host):
        self.host = host
        self._objCache = host._objCache
        self._codec = self.Codec(self._refForObj, self._objForRef)
        self._targetOid = None

    def dump(self, oid, obj):
        if self._targetOid is not None:
            raise RuntimeError("TargetOID is not None: unexpected recursion")

        self._targetOid = oid
        data = self._codec.dump(obj)
        self._targetOid = None
        return data

    def load(self, oid, data):
        if self._targetOid is not None:
            raise RuntimeError("TargetOID is not None: unexpected recursion")

        self._targetOid = oid
        obj = self._codec.load(data)
        self._targetOid = None
        return obj

    def hashDigest(self, oid, data):
        return self._codec.hashDigest(data)

    def _objForRef(self, oid):
        return self.host.ref(oid)

    def _refForObj(self, obj):
        if isinstance(obj, type): 
            return
        if getattr(obj, '__bounded__', False):
            oid = self._objCache.get(obj, None)
            if oid is None:
                return self.host.set(None, obj, True)
            elif oid != self._targetOid:
                return oid

