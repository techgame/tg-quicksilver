#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~ Imports 
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~ Definitions 
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

class NotStorableMixin(object):
    """Provides a sanity check to make sure unstorable objects are not stored"""
    def __getnewargs__(self):
        raise RuntimeError("%s.%s is not storable" % (self.__class__.__module__, self.__class__.__name__))
    def __getstate__(self):
        raise RuntimeError("%s.%s is not storable" % (self.__class__.__module__, self.__class__.__name__))
    def __setstate__(self, state):
        raise RuntimeError("%s.%s is not storable" % (self.__class__.__module__, self.__class__.__name__))

