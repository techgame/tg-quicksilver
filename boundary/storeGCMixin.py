#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~ Imports 
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~ Definitions 
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

class BoundaryStoreGCMixin(object):
    def garbageCollect(self, greylist=None):
        white, black = self.gcWalk(greylist)
        print 'white:', len(white), 'black:', len(black)
        for oid in white:
            print '  gc oid:', oid, self.get(oid, None)
            self._deleteByOid(oid)
        return white
    gc = garbageCollect

    def gcWalk(self, greylist):
        black = self._gcWalkAll(greylist)
        white = set(self.allOids())
        white -= black
        return white, black
    def _gcWalkAll(self, greylist, black=None):
        if greylist is None:
            greylist = self.getGCRoots()

        if black is None: black = set()
        grey = set(self.reg.oidForObj(e) for e in greylist)
        while grey:
            oid = grey.pop()
            if oid not in black:
                black.add(oid)
                grey.update(self.findRefsFrom(oid))
        return black

    def getGCRoots(self):
        raise NotImplementedError('Either override getGCRoots() or reference them explicitly')

