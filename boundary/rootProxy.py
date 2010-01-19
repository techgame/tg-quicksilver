#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~ Imports 
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~ Definitions 
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

class RootProxy(object):
    __bounded__ = True
    __slots__ = ['_obj_']

    def __getattribute__(self, name):
        obj = object.__getattribute__(self, '_obj_')
        return getattr(obj, name)
    def __setattr__(self, name, value):
        obj = object.__getattribute__(self, '_obj_')
        setattr(obj, name, value)
    def __delattr__(self, name, value):
        obj = object.__getattribute__(self, '_obj_')
        delattr(obj, name)

    def __getstate__(self):
        return object.__getattribute__(self, '_obj_')
    def __setstate__(self, obj):
        object.__setattribute__(self, '_obj_', obj)

