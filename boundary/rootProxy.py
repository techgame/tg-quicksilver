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
        r = type.__instancecheck__(klass, instance)
        if not r:
            r = isinstance(klass._objKlass_, instance)
        return r
    def __subclasscheck__(klass, subclass):
        """Override for issubclass(subclass, klass)."""
        r = type.__subclasscheck__(klass, subclass)
        if r: return r
        r = issubclass(subclass, klass._objKlass_)
        return r

    _proxyIgnored = '''
        __metadata__ __class__ __subclasscheck__ __instancecheck__
        __weakref__ __subclasshook__ __dict__ __repr__
        '''
    _proxyIgnored = set(_proxyIgnored.split())
    _proxyKinds = (
            types.UnboundMethodType, 
            type(operator.__contains__),
            type(object.__reduce__), 
            type(object.__delattr__),
            type(object.__new__),)

    def createProxyClass(klass, objKlass):
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

        nsKeys = []
        if ns:
            update_wrapper = functools.update_wrapper
            rebind = klass._rebindFunction
            for name,vfn in ns.items():
                ns[name] = update_wrapper(rebind(name), vfn)
                nsKeys.append(name)

        ns.update(_objKlass_=objKlass, __slots__=[], __module__=objKlass.__module__)
        name = '%s{%s}' % (objKlass.__name__, klass.__name__)
        return klass.__class__(name, (klass,), ns)

    def _rebindFunction(klass, name):
        def fn(self, *args, **kw):
            obj = object.__getattribute__(self, '_obj_')
            return getattr(obj, name)
        return fn

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

class RootProxy(object):
    __metaclass__ = ProxyMeta 
    __bounded__ = True
    __slots__ = ['_obj_', '__weakref__']

    def __repr__(self):
        obj = object.__getattribute__(self, '_obj_')
        if obj is None:
            return repr(obj)
        return object.__repr__(self)
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
    def __setstate__(self, obj):
        object.__setattribute__(self, '_obj_', obj)

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

    def __subclasscheck__(self, test):
        return False
    def __instancecheck__(self, test):
        return False

