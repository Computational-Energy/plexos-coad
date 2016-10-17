COAD Class-Object-Attribute-Data Utility

INTRODUCTION

This package consists of APIs for reading, manipulating and
writing data from an XML document that adheres to the PLEXOS schema.  
It loads all data into a Sqlite database and creates accessors to
data.


USE CASES

list - Print all objects of a class type
show - Print all attributes set for an object
dump - Print data associated with an object; Class, Attributes, Children
get - Retrieve the value of specific object attribute
set - Set the value of a specific object attribute, creating it if the attribute
      is valid for the class
get_children - Retrieve all children or a specific named class of children
set_children - Add or replace children
save - Write the state of the data to an xml file


HOW TO USE

First instantiate a COAD object that loads the data into a database.  The
default file 'master.xml' is used for this demonstration.  These examples are given
as part of an ordered example, execute them in the same order to get the results.

Load the file:
from COAD import COAD
coad=COAD('master.xml')

Output:
Loading master.xml into master.db
Loaded 3413 rows in 0 seconds
Memory usage: 27217920

To show all objects that belong to a given class:

coad.list('Model')

Output:
Base

To show all set attributes for an object with a name:

coad.show('Base')

Output:
Model.Base.Enabled=-1
Report.Base.Output Results by Day=-1

To show all information about an object, including its class, attributes and children:
coad['Model']['Base'].dump()

Output:
Object:  Base                                ID: 3
  Class: Model                               ID: 35
  Attributes set:
    Enabled = -1
  Children (3):
    Object:  Base                                ID: 2
      Class: Horizon                             ID: 37
      No attributes set
      No children
    Object:  Base                                ID: 4
      Class: Report                              ID: 38
      Attributes set:
        Output Results by Day = -1
      No children
    Object:  Base                                ID: 5
      Class: ST Schedule                         ID: 42
      No attributes set
      No children

MODIFYING OBJECTS

Each level of dictionary from the top COADSqlite object reflects the structure of the
data accessors [Class][Object][Attribute] to ClassDict,ObjectDict,String objects respectively.

Getting an attribute:
coad['Report']['Base']['Output Results by Day']

Output:
'-1'

Setting an attribute:
coad['Report']['Base']['Output Results by Day']=1
coad['Report']['Base']['Output Results by Day']

Output:
1

Note that they type changed from str to int.  All values are converted to string when
writing to the xml file.

Deleting an attribute:
del(coad['Report']['Base']['Output Results by Day'])
coad['Report']['Base']['Output Results by Day']
Traceback (most recent call last):
  File "<stdin>", line 1, in <module>
  File "./COADSqlite.py", line 463, in __getitem__
    return self.store[key]
KeyError: 'Output Results by Day'

This means the attribute is no longer set.  It can be added back with the same code as
setting an attribute.

Adding an attribute:
coad['Report']['Base']['Output Results by Day']='-1'
coad['Report']['Base']['Output Results by Day']

Output:
'-1'

MODIFYING MEMBERS OF MODELS:

Models have a number of child objects associated with them.  For now there are two ways to change
children - replace or add.  Replacing is the default scheme, with all classes that match the objects
passed in removed from the model's children list.  Adding children will not modify existing 
relationships.

Example: Set the Model performance to Gurobi
coad['Model']['Base'].set_children(coad['Performance']['Gurobi'])
coad['Model']['Base'].dump()

Output:
Object:  Base                                ID: 3
  Class: Model                               ID: 35
  Attributes set:
    Enabled = -1
  Children (4):
    Object:  Base                                ID: 2
      Class: Horizon                             ID: 37
      No attributes set
      No children
    Object:  Base                                ID: 4
      Class: Report                              ID: 38
      Attributes set:
        Output Results by Day = -1
      No children
    Object:  Base                                ID: 5
      Class: ST Schedule                         ID: 42
      No attributes set
      No children
    Object:  Gurobi                              ID: 9
      Class: Performance                         ID: 47
      Attributes set:
        SOLVER = 4
      No children



UNIT TESTS

The unit tests in the test/ directory have many examples of how to use the APIs
to manipulate the plexos data.



