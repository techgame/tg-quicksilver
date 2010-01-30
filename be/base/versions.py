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

from ...mixins import NotStorableMixin
from .utils import Namespace

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~ Definitions 
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

class VersionsAbstract(NotStorableMixin):
    ns = Namespace()

    def workspace(self, wsid=None):
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

