#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~ Imports 
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~ Definitions 
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

class MetadataViewBase(object):
    def get(self, name, default=None):
        return self.getMeta(name, None, default)
    def getAt(self, name, idx=None, default=None):
        raise NotImplementedError('Subclass Responsibility: %r' % (self,))

    def set(self, name, value):
        return self.setAt(name, None, value)
    def setAt(self, name, idx, value):
        raise NotImplementedError('Subclass Responsibility: %r' % (self,))

    def delete(self, name):
        return self.deleteAt(name, None, value)
    def deleteAt(self, name, idx):
        raise NotImplementedError('Subclass Responsibility: %r' % (self,))

    def default(self, name, value=None):
        return self.defaultAt(name, None, value)
    def defaultAt(self, name, idx, value=None):
        sentinal = object()
        r = self.getAt(name, idx, sentinal)
        if r is sentinal:
            self.setAt(name, idx, value)
            return value
        else: return r

    def iter(self, name):
        raise NotImplementedError('Subclass Responsibility: %r' % (self,))

    def keyView(self, metaKey):
        return KeyedMetadataView(self, metaKey)
    def __getitem__(self, metaKey):
        return self.keyView(metaKey)

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

class KeyedMetadataView(object):
    def __init__(self, view, metaKey):
        self._name = metaKey
        self._view = view

    def get(self, default=None):
        return self.getAt(None, default)
    def set(self, value):
        return self.setAt(None, value)
    def delete(self):
        return self.deleteAt(None, value)
    def default(self, value=None):
        return self.defaultAt(None, value)

    def getAt(self, key, default=None):
        return self._view.getAt(self._name, key, default)
    def setAt(self, key, value):
        return self._view.setAt(self._name, key, value)
    def deleteAt(self, key):
        return self._view.deleteAt(self._name, key)

    def defaultAt(self, key, value=None):
        sentinal = object()
        r = self.getAt(key, sentinal)
        if r is sentinal:
            r = value
            self.setAt(key, r)
        return r

    def __iter__(self):
        return self._view.iter(self._name)
    def __getitem__(self, key):
        if isinstance(key, slice):
            return list(self)[key]
        else: return self.getAt(key)
    def __setitem__(self, key, value):
        if isinstance(key, slice):
            raise ValueError("KeyedMetadataView does not support assignment by slice")
        return self.setAt(key, value)
    def __delitem__(self, key, value):
        if isinstance(key, slice):
            raise ValueError("KeyedMetadataView does not support delete by slice")
        return self.delAt(key, value)

