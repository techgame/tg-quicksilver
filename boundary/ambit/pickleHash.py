#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~ Imports 
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

import re
import pickle
from pickle import Unpickler as _Unpickler

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~ Definitions 
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

_Stop = pickle._Stop

def _fetchNames(module, regex, ns):
    match = re.compile(regex).match
    ns.update((n,getattr(module, n)) for n in dir(module) if match(n))

_fetchNames(pickle, '[A-Z][A-Z0-9_]+$', globals())

class HashUnpickler(_Unpickler):
    def load(self):
        """Read a pickled object representation from the open file.

        Return the reconstituted object hierarchy specified in the file.
        """
        self.mark = object() # any new unique object
        self.stack = stack = []
        def sigAppend(code, data):
            stack.append(data)
        self.sigAppend = sigAppend
        def sigFold(code, k):
            e = ''.join(stack[k:])
            stack[k:] = [e]
        self.sigFold = sigFold
        def sigReplace(code, k, e):
            stack[k:] = [e]
        self.sigReplace = sigReplace

        read = self.read
        dispatch = self.dispatch
        try:
            while 1:
                key = read(1)
                dispatch[key](self, key)
        except _Stop, stopinst:
            return stopinst.value

    dispatch = {}

    def load_eof(self, key):
        raise EOFError
    dispatch[''] = load_eof

    def load_proto(self, code):
        proto = ord(self.read(1))
        if not 0 <= proto <= 2:
            raise ValueError, "unsupported pickle protocol: %d" % proto
    dispatch[PROTO] = load_proto

    def load_persid(self, code):
        self.sigAppend(code, self.readline())
    dispatch[PERSID] = load_persid

    def load_binpersid(self, code):
        pid = self.stack.pop()
        self.sigAppend(code, pid)
    dispatch[BINPERSID] = load_binpersid

    def load_none(self, code):
        self.sigAppend(code, '')
    dispatch[NONE] = load_none

    def load_false(self, code):
        self.sigAppend(code, '')
    dispatch[NEWFALSE] = load_false

    def load_true(self, code):
        self.sigAppend(code, '')
    dispatch[NEWTRUE] = load_true

    def load_int(self, code):
        self.sigAppend(code, self.readline())
    dispatch[INT] = load_int

    def load_binint(self, code):
        self.sigAppend(code, self.read(4))
    dispatch[BININT] = load_binint

    def load_binint1(self, code):
        self.sigAppend(code, self.read(1))
    dispatch[BININT1] = load_binint1

    def load_binint2(self, code):
        self.sigAppend(code, self.read(2))
    dispatch[BININT2] = load_binint2

    def load_long(self, code):
        self.sigAppend(code, self.readline())
    dispatch[LONG] = load_long

    def load_long1(self, code):
        n = ord(self.read(1))
        bytes = self.read(n)
        self.sigAppend(code, bytes)
    dispatch[LONG1] = load_long1

    def load_long4(self, code):
        n = mloads('i' + self.read(4))
        bytes = self.read(n)
        self.sigAppend(code, bytes)
    dispatch[LONG4] = load_long4

    def load_float(self, code):
        self.sigAppend(code, self.readline())
    dispatch[FLOAT] = load_float

    def load_binfloat(self, code):
        self.sigAppend(code, self.read(8))
    dispatch[BINFLOAT] = load_binfloat

    def load_string(self, code):
        rep = self.readline()[:-1]
        self.sigAppend(code, rep)
    dispatch[STRING] = load_string

    def load_binstring(self, code):
        n = mloads('i' + self.read(4))
        self.sigAppend(code, self.read(n))
    dispatch[BINSTRING] = load_binstring

    def load_unicode(self, code):
        self.sigAppend(code, self.readline())
    dispatch[UNICODE] = load_unicode

    def load_binunicode(self, code):
        n = mloads('i' + self.read(4))
        self.sigAppend(code, self.read(n))
    dispatch[BINUNICODE] = load_binunicode

    def load_short_binstring(self, code):
        n = ord(self.read(1))
        self.sigAppend(code, self.read(n))
    dispatch[SHORT_BINSTRING] = load_short_binstring

    def load_tuple(self, code):
        self.sigFold(code, self.marker())
    dispatch[TUPLE] = load_tuple

    def load_empty_tuple(self, code):
        self.sigAppend(code, '')
    dispatch[EMPTY_TUPLE] = load_empty_tuple

    def load_tuple1(self, code):
        self.sigFold(code, -1)
    dispatch[TUPLE1] = load_tuple1

    def load_tuple2(self, code):
        self.sigFold(code, -2)
    dispatch[TUPLE2] = load_tuple2

    def load_tuple3(self, code):
        self.sigFold(code, -3)
    dispatch[TUPLE3] = load_tuple3

    def load_empty_list(self, code):
        self.sigAppend(code, '')
    dispatch[EMPTY_LIST] = load_empty_list

    def load_empty_dictionary(self, code):
        self.sigAppend(code, '')
    dispatch[EMPTY_DICT] = load_empty_dictionary

    def load_list(self, code):
        self.sigFold(code, self.marker())
    dispatch[LIST] = load_list

    def load_dict(self, code):
        stack = self.stack
        k = self.marker()
        items = stack[k+1:]
        items = zip(items[::2], items[1::2])
        items.sort()
        items = ''.join(k+v for k,v in items)
        ##stack[k:] = [code+items]
        self.sigReplace(code, k, items)
    dispatch[DICT] = load_dict

    def load_inst(self, code):
        v = [self.readline(),self.readline()]
        k = self.marker()
        v.extend(stack[k+1:])
        #stack[k:] = [code + ''.join(v)]
        self.sigReplace(code, k, ''.join(v))
    dispatch[INST] = load_inst

    def load_obj(self, code):
        # Stack is ... markobject classobject arg1 arg2 ...
        k = self.marker()
        v = ''.join(stack[k+1:])
        #stack[k:] = [code+v]
        self.sigReplace(code, k, v)
    dispatch[OBJ] = load_obj

    def load_newobj(self, code):
        self.sigFold(code, -2)
    dispatch[NEWOBJ] = load_newobj

    def load_global(self, code):
        module = self.readline()
        name = self.readline()
        self.sigAppend(code, module+name)
    dispatch[GLOBAL] = load_global

    def load_ext1(self, code):
        self.sigAppend(code, self.read(1))
    dispatch[EXT1] = load_ext1

    def load_ext2(self, code):
        self.sigAppend(code, self.read(2))
    dispatch[EXT2] = load_ext2

    def load_ext4(self, code):
        self.sigAppend(code, self.read(4))
    dispatch[EXT4] = load_ext4

    def load_reduce(self, code):
        self.sigFold(-2)
    dispatch[REDUCE] = load_reduce

    def load_pop(self, code):
        del self.stack[-1]
    dispatch[POP] = load_pop

    def load_pop_mark(self, code):
        k = self.marker()
        del self.stack[k:]
    dispatch[POP_MARK] = load_pop_mark

    def load_dup(self, code):
        self.sigAppend(code, self.stack[-1])
    dispatch[DUP] = load_dup

    def load_get(self, code):
        self.sigAppend(code, self.memo[self.readline()[:-1]])
    dispatch[GET] = load_get

    def load_binget(self, code):
        i = ord(self.read(1))
        self.sigAppend(code, self.memo[repr(i)])
    dispatch[BINGET] = load_binget

    def load_long_binget(self, code):
        i = mloads('i' + self.read(4))
        self.sigAppend(code, self.memo[repr(i)])
    dispatch[LONG_BINGET] = load_long_binget

    def load_put(self, code):
        self.memo[self.readline()[:-1]] = self.stack[-1]
    dispatch[PUT] = load_put

    def load_binput(self, code):
        i = ord(self.read(1))
        self.memo[repr(i)] = self.stack[-1]
    dispatch[BINPUT] = load_binput

    def load_long_binput(self, code):
        i = mloads('i' + self.read(4))
        self.memo[repr(i)] = self.stack[-1]
    dispatch[LONG_BINPUT] = load_long_binput

    def load_append(self, code):
        stack = self.stack
        v = stack.pop()
        #stack[-1] += code+v
        self.sigReplace(code, -1, v)
    dispatch[APPEND] = load_append

    def load_appends(self, code):
        stack = self.stack
        k = self.marker()
        v = ''.join(stack[k+1:])
        #del stack[k:]
        #stack[-1] += code+v
        self.sigReplace(code, k-1, v)
    dispatch[APPENDS] = load_appends

    def load_setitem(self, code):
        stack = self.stack
        value = stack.pop()
        key = stack.pop()
        #stack[-1] += code+key+value
        self.sigReplace(code, -1, key+value)
    dispatch[SETITEM] = load_setitem

    def load_setitems(self, code):
        stack = self.stack
        k = self.marker()
        items = zip(stack[k+1::2], stack[k+2::2])
        items.sort()
        items = ''.join(k+v for k,v in items)
        #del stack[k:]
        #stack[-1] += code+items
        self.sigReplace(code, k-1, items)
    dispatch[SETITEMS] = load_setitems

    def load_build(self, code):
        self.sigFold(code, -2)
    dispatch[BUILD] = load_build

    def load_mark(self, code):
        self.stack.append(self.mark)
    dispatch[MARK] = load_mark

    def load_stop(self, code):
        value = self.stack.pop()
        raise _Stop(value)
    dispatch[STOP] = load_stop
