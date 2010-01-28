#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~ Imports 
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

import types, functools, operator
from ..mixins import NotStorableMixin

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~ Definitions 
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

class ProxyMeta(type):
    _objKlass_ = object

    def __instancecheck__(klass, instance):
        """Override for isinstance(instance, klass)."""
        print 'meta __instancecheck__:'
        r = type.__instancecheck__(klass, instance)
        if not r:
            r = isinstance(klass._objKlass_, instance)
        return r
    def __subclasscheck__(klass, subclass):
        """Override for issubclass(subclass, klass)."""
        print 'meta __subclasscheck__:'
        r = type.__subclasscheck__(klass, subclass)
        if r: return r
        r = issubclass(subclass, klass._objKlass_)
        return r

    _proxyIgnored = '''
        __metaclass__ __class__ __dict__ __weakref__ 
        __subclasscheck__ __instancecheck__ __subclasshook__ 
        __getattribute__ __repr__ '''
    _proxyIgnored = set(_proxyIgnored.split())
    _proxyKinds = (
            types.UnboundMethodType, 
            type(operator.__contains__),
            type(object.__reduce__), 
            type(object.__delattr__),
            type(object.__new__),)

    def rebindNamespace(klass, objKlass, rebind=None):
        ignored = klass._proxyIgnored
        ns = dict()

        kinds = klass._proxyKinds
        for name in dir(objKlass):
            if not name.startswith('__') or not name.endswith('__'):
                continue
            if name in ignored:
                continue
            value = getattr(objKlass, name)
            if not isinstance(value, kinds):
                continue
            if value is getattr(object, name, None):
                continue
            ns[name] = value

        if not ns: 
            return ns

        update_wrapper = functools.update_wrapper
        if rebind is None:
            rebind = klass._rebindFunction
        elif rebind is True:
            rebind = klass._rebindProxyFunction

        for name,vfn in ns.items():
            ns[name] = update_wrapper(rebind(name), vfn)
        return ns

    def createProxyClass(klass, objKlass, rebind=None):
        ns = dict()
        if rebind is True:
            ns.update(klass.rebindNamespace(operator, rebind))
            ns.update(klass.rebindNamespace(RootProxy, rebind))
        else:
            ns.update(klass.rebindNamespace(objKlass, rebind))

        ns.update(_objKlass_=objKlass, __slots__=[], __module__=objKlass.__module__)
        name = klass.__name__
        if not isinstance(None, objKlass):
            name = '%s{%s}' % (objKlass.__name__, name)
        return type(klass)(name, (klass,), ns)

    def _rebindFunction(klass, name):
        def fn(self, *args):
            obj = object.__getattribute__(self, '_obj_')
            fn = getattr(obj, name)
            return fn(*args)
        return fn

    def _rebindProxyFunction(klass, fnName):
        """Uses entry.fetchProxyFn to retrive object from store, passing fnName
        that caused the proxy-load-fault"""
        def fn(self, *args):
            entry = object.__getattribute__(self, '_obj_')
            obj = entry.fetchProxyFn(fnName, args)
            return fn(*args)
        return fn

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

class RootProxy(object):
    __metaclass__ = ProxyMeta 
    __slots__ = ['_obj_', '__weakref__']

    def __repr__(self):
        obj = object.__getattribute__(self, '_obj_')
        return repr(obj)
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
    def __setstate__(self, state):
        object.__setattribute__(self, '_obj_', state)

    _adaptCache = {}
    @classmethod
    def adaptProxy(klass, pxy, obj):
        objKlass = obj.__class__
        pxyKlass = klass._adaptCache.get(objKlass)
        if pxyKlass is None:
            pxyKlass = klass.createProxyClass(objKlass)
            klass._adaptCache[objKlass] = pxyKlass

        object.__setattr__(pxy, '__class__', pxyKlass)
        object.__setattr__(pxy, '_obj_', obj)
        return pxy

    @classmethod
    def adaptDeferred(klass, pxy, entry):
        pxyKlass = klass._adaptCache[None]
        object.__setattr__(pxy, '__class__', pxyKlass)
        object.__setattr__(pxy, '_obj_', entry)
        return pxy

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

class RootProxyRef(RootProxy):
    __slots__ = []
    def __repr__(self):
        entry = object.__getattribute__(self, '_obj_')
        typeref = entry.typeref()
        return '<%s.%s {proxy oid:%s}>' % (
            typeref.__module__, typeref.__name__, entry.oid, )

    def __getattribute__(self, name):
        entry = object.__getattribute__(self, '_obj_')
        return entry.fetchProxyAttr(name)

    @classmethod
    def createDeferredProxy(klass):
        objKlass = type(None)
        pxyKlass = klass.createProxyClass(objKlass, True)
        klass._adaptCache[None] = pxyKlass
        klass._adaptCache[objKlass] = pxyKlass
        return pxyKlass

RootProxyRef = RootProxyRef.createDeferredProxy()

