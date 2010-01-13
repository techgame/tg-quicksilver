#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~ Imports 
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

import itertools
from functools import partial

from .. import base
from ..base.utils import Namespace, getnode, splitColumnData

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~ Definitions 
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

class HostDataView(object):
    def __init__(self, host):
        self.setHost(host)

    def setHost(self, host):
        self.cur = host.cur
        self.ns = host.ns

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

class OpBase(object):
    def setHost(self, host):
        self.cur = host.cur
        self.ns = host.ns

    def _bindExecute(self, stmt, ns=None, cur=None):
        if cur is None: cur = self.cur
        if ns is None:
            stmt %= self.ns
        elif ns: 
            stmt %= ns

        def executeArgs(ex, stmt, *args):
            return cur.execute(stmt, args)
        return partial(executeArgs, cur.execute, stmt)

