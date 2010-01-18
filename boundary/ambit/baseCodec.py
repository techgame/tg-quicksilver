#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~ Imports 
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~ Definitions 
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

class BaseAmbitCodec(object):
    def __init__(self, idForObj=None, objForId=None):
        self.init(idForObj, objForId)

    def init(self, idForObj=None, objForId=None):
        self._initEncoder(idForObj)
        self._initDecoder(objForId)

    def _initEncoder(self, idForObj=None):
        raise NotImplementedError('Subclass Responsibility: %r' % (self,))
    def _initDecoder(self, objForId=None):
        raise NotImplementedError('Subclass Responsibility: %r' % (self,))

    def encode(self, obj, incMemo=False):
        raise NotImplementedError('Subclass Responsibility: %r' % (self,))
    def decode(self, data, meta=None):
        raise NotImplementedError('Subclass Responsibility: %r' % (self,))

