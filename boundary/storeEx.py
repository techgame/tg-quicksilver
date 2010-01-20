#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~ Imports 
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

from .store import BoundaryStore

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~ Definitions 
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

class StatsBoundaryStore(BoundaryStore):
    def init(self, workspace):
        BoundaryStore.init(self, workspace)
        BoundaryStats(self)

class BoundaryStats(object):
    r_count = 0
    r_data = 0
    r_payload = 0

    w_count = 0
    w_data = 0
    w_payload = 0

    def __init__(self, bs, attr='stats'):
        setattr(bs, attr, self)
        bs._onRead = self._onRead
        bs._onWrite = self._onWrite

    def printStats(self):
        print 'Store Stats:'
        print '  @read [%5d] %8.1f%% = %6i/%6i' % (self.r_count,
                ((100.0 * self.r_data)/max(1, self.r_payload)),
                self.r_data, self.r_payload)

        print '  @write[%5d] %8.1f%% = %6i/%6i' % (self.w_count,
                ((100.0 * self.w_data)/max(1,self.w_payload)),
                self.w_data, self.w_payload)

    def _onRead(self, record, data=None):
        if data is None:
            return
        self.r_count += 1
        self.r_payload += len(record.payload)
        self.r_data += len(data)

    def _onWrite(self, payload, data):
        self.w_count += 1
        self.w_payload += len(payload)
        self.w_data += len(data)

