#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~ Imports 
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

from ...mixins import NotStorableMixin
from .utils import Namespace

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~ Definitions 
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

class VersionsAbstract(NotStorableMixin):
    ns = Namespace()

    def workspace(self, *args, **kw):
        """Returns a bound Workspace"""
        raise NotImplementedError('Subclass Responsibility: %r' % (self,))

    def versions(self, *args, **kw):
        """Returns a bound ChangesView"""
        raise NotImplementedError('Subclass Responsibility: %r' % (self,))

    def metadata(self, *args, **kw):
        """Returns a bound MetadataView"""
        raise NotImplementedError('Subclass Responsibility: %r' % (self,))

    def head(self, *args, **kw): 
        return self.versions().head(*args, **kw)
    def heads(self, *args, **kw): 
        return self.versions().heads(*args, **kw)

