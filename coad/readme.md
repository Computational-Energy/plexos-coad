# Plexos Tools

## Requirements
* h5py
* numpy
* pandas

## Utilities
* [COAD.py](#coad) - Manipulation of Plexos input files
* [plexos2hdf5.py](#plexos2hdf5) - Process Plexos output into sqlite database
* [output.py](#outputpy) - Read Plexos output as pandas dataframes
* [ModelUtil.py](#modelutil) - Utilities for modifying Plexos input
* [plexos_database.py](#plexos_databasepy) - Core conversion of Plexos XML files
to sqlite database

## COAD
Class-Object-Attribute Data Utility

### INTRODUCTION

This module consists of APIs for reading, manipulating and
writing data from an XML document that adheres to the PLEXOS schema.  
It loads all data into a Sqlite database and creates accessors to
data.


### USE CASES

* list - Print all objects of a class type
* show - Print all attributes set for an object
* dump - Print data associated with an object; Class, Attributes, Children
* get - Retrieve the value of specific object attribute
* set - Set the value of a specific object attribute, creating it if the attribute
is valid for the class
* get_children - Retrieve all children or a specific named class of children
* set_children - Add or replace children
* save - Write the state of the data to an xml file

### HOW TO USE

First instantiate a COAD object that loads the data into a database.  The
default file 'master.xml' is used for this demonstration.  These examples are given
as part of an ordered example, execute them in the same order to get the results.

#### Load the file:
```python
from COAD import COAD
coad = COAD('master.xml')
```
Output:
```
Loading master.xml into master.db
Loaded 3413 rows in 0 seconds
Memory usage: 27217920
```
#### To show all objects that belong to a given class:
```python
coad.list('Model')
```
Output:
```
Base
```

#### To show all set attributes for an object with a name:
```python
coad.show('Base')
```
Output:
```
Model.Base.Enabled=-1
Report.Base.Output Results by Day=-1
```
#### To show all information about an object, including its class, attributes and children:
```python
coad['Model']['Base'].dump()
```
Output:
```
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
```
### MODIFYING OBJECTS

Each level of dictionary from the top COADSqlite object reflects the structure of the
data accessors [Class][Object][Attribute] to ClassDict, ObjectDict, String objects
respectively.

#### Getting an attribute:
```python
coad['Report']['Base']['Output Results by Day']
```
Output:
```
'-1'
```
#### Setting an attribute:
```python
coad['Report']['Base']['Output Results by Day'] = 1
coad['Report']['Base']['Output Results by Day']
```
Output:
```
1
```
Note that the type changed from str to int.  All values are converted to string when
writing to the xml file.

#### Deleting an attribute:
```python
del(coad['Report']['Base']['Output Results by Day'])
coad['Report']['Base']['Output Results by Day']
Traceback (most recent call last):
  File "<stdin>", line 1, in <module>
  File "./COADSqlite.py", line 463, in __getitem__
    return self.store[key]
KeyError: 'Output Results by Day'
```
This means the attribute is no longer set.  It can be added back with the same code as
setting an attribute.

#### Adding an attribute:
```python
coad['Report']['Base']['Output Results by Day'] = '-1'
coad['Report']['Base']['Output Results by Day']
```
Output:
```
'-1'
```
### MODIFYING MEMBERS OF MODELS:

Models have a number of child objects associated with them.  For now there are two ways to change
children - replace or add.  Replacing is the default scheme, with all classes that match the objects
passed in removed from the model's children list.  Adding children will not modify existing
relationships.

Example: Set the Model performance to Gurobi
```python
coad['Model']['Base'].set_children(coad['Performance']['Gurobi'])
coad['Model']['Base'].dump()
```
Output:
```
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
```
### UNIT TESTS

The unit tests in the test/ directory have many examples of how to use the APIs
to manipulate the plexos data.

## Plexos2HDF5
Reads model xml, BIN files, and logs from a plexos solution zipfile to generate
a single hdf5 representation of the solution.
```
usage: plexos2hdf5.py [-h] [-d] [-o OUTPUT] zipfile

Process plexos output zipfile into hdf5 file

positional arguments:
  zipfile               plexos output zipfile

optional arguments:
  -h, --help            show this help message and exit
  -d, --debug           show detailed logs
  -o OUTPUT, --output OUTPUT
                        hdf5 filename to write
```
### solution.py
Conversion of an R solution processor to python
```
usage: solution.py [-h] [-d] zipfile

Process plexos output zipfile

positional arguments:
  zipfile      plexos output zipfile

optional arguments:
  -h, --help   show this help message and exit
  -d, --debug  show detailed logs
```

### output.py

#### Get list of classes
```python
from output import PlexosOutput
ps = PlexosOutput('test/mda_output.zip')
ps.keys()
```
Output:
```
[u'Node', u'Generator', u'Constraint', u'Region', u'System', u'Line']
```
#### Get list of objects
```python
ps['Line'].keys()
```
Output:
```
[u'B0_B2', u'B1_B2', u'B0_B1']
```
#### Get list of data attributes
```python
ps['Line'].get_property_names()
```
Output:
```
[u'Flow', u'Export Limit', u'Import Limit']
```
#### Get data values
```python
ps['Line']['B1_B2'].get_data_values('Flow')
```
Output:
```
[-0.935319116500001, -0.6970154267499986, -0.5217735017499989, -0.41615258650000153, -0.3980630747500005, -0.46516376499999984, -0.7597340485000006, -1.2800584555000007, -1.812169899250002, -2.0393797997500016, -2.1432084820000004, -2.20546277575, -2.2587450190000005, -2.15386336825, -2.0509797174999984, -1.98446034625, -1.9687104047500001, -2.1013393862500007, -2.4032077540000008, -2.3716624119999983, -2.0844381467499993, -1.7796791724999996, -1.4374390120000011, -1.1613561009999995]
```
#### Get data times
```python
ps['Line']['B1_B2'].get_data_times('Flow')
```
Output:
```
[datetime.datetime(2020, 4, 16, 0, 0), datetime.datetime(2020, 4, 16, 1, 0), datetime.datetime(2020, 4, 16, 2, 0), datetime.datetime(2020, 4, 16, 3, 0), datetime.datetime(2020, 4, 16, 4, 0), datetime.datetime(2020, 4, 16, 5, 0), datetime.datetime(2020, 4, 16, 6, 0), datetime.datetime(2020, 4, 16, 7, 0), datetime.datetime(2020, 4, 16, 8, 0), datetime.datetime(2020, 4, 16, 9, 0), datetime.datetime(2020, 4, 16, 10, 0), datetime.datetime(2020, 4, 16, 11, 0), datetime.datetime(2020, 4, 16, 12, 0), datetime.datetime(2020, 4, 16, 13, 0), datetime.datetime(2020, 4, 16, 14, 0), datetime.datetime(2020, 4, 16, 15, 0), datetime.datetime(2020, 4, 16, 16, 0), datetime.datetime(2020, 4, 16, 17, 0), datetime.datetime(2020, 4, 16, 18, 0), datetime.datetime(2020, 4, 16, 19, 0), datetime.datetime(2020, 4, 16, 20, 0), datetime.datetime(2020, 4, 16, 21, 0), datetime.datetime(2020, 4, 16, 22, 0), datetime.datetime(2020, 4, 16, 23, 0)]
```
#### Get data units
```python
ps['Line']['B1_B2'].get_data_unit('Flow')
```
Output:
```
u'MW'
```
#### Get dataframe for entire class
```python
ps['Line'].get_data('Flow')
```
Output:
```
                        B0_B1  B0_B2     B1_B2
2020-04-16 00:00:00  4.935319      4 -0.935319
2020-04-16 01:00:00  4.697015      4 -0.697015
2020-04-16 02:00:00  4.521774      4 -0.521774
2020-04-16 03:00:00  4.416153      4 -0.416153
2020-04-16 04:00:00  4.398063      4 -0.398063
2020-04-16 05:00:00  4.465164      4 -0.465164
2020-04-16 06:00:00  4.759734      4 -0.759734
2020-04-16 07:00:00  5.280058      4 -1.280058
2020-04-16 08:00:00  5.812170      4 -1.812170
2020-04-16 09:00:00  6.039380      4 -2.039380
2020-04-16 10:00:00  6.143208      4 -2.143208
2020-04-16 11:00:00  6.205463      4 -2.205463
2020-04-16 12:00:00  6.258745      4 -2.258745
2020-04-16 13:00:00  6.153863      4 -2.153863
2020-04-16 14:00:00  6.050980      4 -2.050980
2020-04-16 15:00:00  5.984460      4 -1.984460
2020-04-16 16:00:00  5.968710      4 -1.968710
2020-04-16 17:00:00  6.101339      4 -2.101339
2020-04-16 18:00:00  6.403208      4 -2.403208
2020-04-16 19:00:00  6.371662      4 -2.371662
2020-04-16 20:00:00  6.084438      4 -2.084438
2020-04-16 21:00:00  5.779679      4 -1.779679
2020-04-16 22:00:00  5.437439      4 -1.437439
2020-04-16 23:00:00  5.161356      4 -1.161356
```
#### Get dataframe for multiple objects
```python
ps['Line'].get_data('Flow', object_names=['B0_B1', 'B1_B2'])
```
Output:
```
                        B0_B1     B1_B2
2020-04-16 00:00:00  4.935319 -0.935319
2020-04-16 01:00:00  4.697015 -0.697015
2020-04-16 02:00:00  4.521774 -0.521774
2020-04-16 03:00:00  4.416153 -0.416153
2020-04-16 04:00:00  4.398063 -0.398063
2020-04-16 05:00:00  4.465164 -0.465164
2020-04-16 06:00:00  4.759734 -0.759734
2020-04-16 07:00:00  5.280058 -1.280058
2020-04-16 08:00:00  5.812170 -1.812170
2020-04-16 09:00:00  6.039380 -2.039380
2020-04-16 10:00:00  6.143208 -2.143208
2020-04-16 11:00:00  6.205463 -2.205463
2020-04-16 12:00:00  6.258745 -2.258745
2020-04-16 13:00:00  6.153863 -2.153863
2020-04-16 14:00:00  6.050980 -2.050980
2020-04-16 15:00:00  5.984460 -1.984460
2020-04-16 16:00:00  5.968710 -1.968710
2020-04-16 17:00:00  6.101339 -2.101339
2020-04-16 18:00:00  6.403208 -2.403208
2020-04-16 19:00:00  6.371662 -2.371662
2020-04-16 20:00:00  6.084438 -2.084438
2020-04-16 21:00:00  5.779679 -1.779679
2020-04-16 22:00:00  5.437439 -1.437439
2020-04-16 23:00:00  5.161356 -1.161356
```

### ModelUtil
Utilities for splitting horizons in a Plexos input file

Methods:
* split_horizon(coad, model_name, num_partitions, start_day_overlap=0,
write_rindex_file=False, rindex_file=sys.stdout):
```
Split the horizons associated with model by creating new models and horizons for every split

    coad - COAD object
    model_name - name of the model to split horizons
    num_partitions - number of partitions, can handle 1000 max
    start_day_overlap - start horizon this many days before partition
    write_rindex_file - Whether or not to write the index file of partition information
    rindex_file - The file-like object to write the index file of parition information
```

### plexos_database.py
Conversion of Plexos XML files to and from sqlite database

Methods:
* load(source, dbfilename=None, create_db_file=True, remove_invalid_chars=False):
```
Load the xml file into a sqlite database
    Trust nothing, assume the worst on input by placing
    all table and column names in single quotes
    and feeding the text via parameterized SQL whenever possible

    Args:   source - Plexos XML filename or file-like object to load
            create_db_file - False will create an in-memory database
                             Defaults to True
            dbfilename - Optional filename to save the database.  If set to None,
                         will take the prefix of filename and append .db
            remove_invalid_chars - True will remove invalid xml 1.0 chars by
                    creating a new tempfile and read/writing

    Returns: sqlite db
```
* save(dbcon, filename):
```
Write contents of plexos sqlite database to xml filename

    Args:   dbcon - sqlite database connection
            filename - Location to save plexos XML file.  The file will be
                       overwritten if it exists

    No Return
```
