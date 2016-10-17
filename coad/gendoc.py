import pydoc,sys
from pydoc import HTMLDoc
import COAD

def fixed_classlink(self, object, modname):
  """Make a link for a class.    
    TODO: This needs to be fixed up the chain to test for built-ins vs modules
    that require documentation created
  """
  #print('o:%s mod:%s'%(object.__name__,modname))
  name, module = object.__name__, sys.modules.get(object.__module__)
  if hasattr(module, name) and getattr(module, name) is object:
    if modname != module.__name__:
      retstr ='<a href="http://docs.python.org/library/%s.html#%s">%s</a>'
    else:
      retstr ='<a href="%s.html#%s">%s</a>'
    return retstr % (module.__name__, name, pydoc.classname(object, modname))
  return pydoc.classname(object, modname)

def fixed_modulelink(self, object):
  """Make a link for a module.
    TODO: This needs to be fixed up the chain to test for built-ins vs modules
    that require documentation created 
  """
  #print("%s = %s"%(object.__name__,object.__name__))
  return '<a href="http://docs.python.org/library/%s.html">%s</a>' % (object.__name__, object.__name__)

if __name__ == "__main__":
  HTMLDoc.modulelink = fixed_modulelink
  HTMLDoc.classlink = fixed_classlink
  pydoc.writedoc(COAD)
