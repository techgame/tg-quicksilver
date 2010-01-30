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

import uuid
from .namespace import Namespace

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~ Definitions 
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

getnode = uuid.getnode

def joinKWDict(items, kwDict):
    if not kwDict: return items
    if items is None: return kwDict
    update = getattr(items, 'update', None)
    if update is None:
        items = dict(items)
        items.update(kwDict)
    else: update(kwDict)
    return items

def splitKeyValsEx(items, kwDict):
    items = joinKWDict(items, kwDict)
    iterkeys = getattr(items, 'iterkeys', None)
    if iterkeys is not None:
        cols = list(iterkeys())
        data = [items[k] for k in cols]
    else:
        cols, data = zip(*_items_)
    return cols, data

def splitColumnData(kwData, columns):
    cols = []; values = []
    for c in columns:
        v = kwData.pop(c, values)
        if v is not values:
            cols.append(c)
            values.append(v)
    return cols, values

