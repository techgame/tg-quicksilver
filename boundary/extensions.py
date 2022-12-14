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

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~ Definitions 
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

class BoundaryStats(object):
    r_count = 0
    r_data = 0
    r_payload = 0

    w_count = 0
    w_data = 0
    w_payload = 0

    def __init__(self, bs, attr='stats'):
        setattr(bs, attr, self)
        self._onReadPost = bs._onRead
        bs._onRead = self._onRead

        self._onWritePost = bs._onWrite
        bs._onWrite = self._onWrite

    def printStats(self, reset=True):
        print 'Store Stats:'
        print '  @read [%5d] %8.1f%% = %6i/%6i' % (self.r_count,
                ((100.0 * self.r_data)/max(1, self.r_payload)),
                self.r_data, self.r_payload)

        print '  @write[%5d] %8.1f%% = %6i/%6i' % (self.w_count,
                ((100.0 * self.w_data)/max(1,self.w_payload)),
                self.w_data, self.w_payload)

        if reset:
            self.reset()

    def reset(self):
        self.__dict__.update((k,0) for k in self.__dict__ if k[:2] in ('r_', 'w_'))

    def _onRead(self, rec, data, entry):
        if data is not None:
            self.r_count += 1
            self.r_payload += len(rec['payload'])
            self.r_data += len(data)
        return self._onReadPost(rec, data, entry)

    def _onWrite(self, payload, data, entry):
        self.w_count += 1
        self.w_payload += len(payload)
        self.w_data += len(data)
        return self._onWritePost(payload, data, entry)

