#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~ Imports 
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

from .baseCodec import BaseAmbitStrategy, BaseAmbitCodec
from .pickleCodec import PickleAmbitCodec

# XXX: Possible future implementations
##from .jsonCodec import JsonAmbitCodec 
##from .xmlCodec import XMLAmbitCodec

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~ Definitions 
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

class AmbitStrategy(BaseAmbitStrategy):
    Codec = PickleAmbitCodec

