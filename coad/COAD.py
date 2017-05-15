"""Class-Object-Attribute Dictionary Tools

This module contains tools for manipulating Plexos data files outside of the
Plexos UI.  It loads the xml file into a sqlite database for further inspection
and modification.

Example:
    from COAD import COAD
    coad = COAD("master.xml")
    print("Before set, solver is %s"%coad['Performance']['Gurobi']['SOLVER'])
    coad['Performance']['Gurobi']['SOLVER'] = 3
    coad.save("master_new.xml")
    coad_new = COAD("master_new.xml")
    print("After set, solver is %s"%coad['Performance']['Gurobi']['SOLVER'])
"""
import collections
import os
import sqlite3 as sql
import sys
import uuid

import plexos_database

class COAD(collections.MutableMapping):
    '''Edit models, horizons, memberships and object attributes of plexos data.
    Quickly modify the largest xml files for simulation.

    Instantiation will import xml data into a sqlite database or open an
    existing sqlite database of plexos data

    When import xml data, the new database will be saved as the same name as
    the file with a .db suffix instead of .xml

    When create_db_file is set to False, the new database will be created only
    in memory

    The class presents a map of class names to ClassDict objects
    '''

    def __init__(self, filename=None, create_db_file=True):
        if filename is None:
            filename = os.path.abspath(os.path.dirname(__file__)) + os.sep + "master.xml"
        try:
            with open(filename):
                pass
        except:
            raise Exception('Unable to open %s'%filename)
        if filename.endswith('.db'):
            self.dbcon = sql.connect(filename)
        elif not filename.endswith('.xml'):
            raise Exception('Invalid filename suffix')
        else:
            self.dbcon = plexos_database.load(filename, create_db_file=create_db_file)
        #self.dbcon.text_factory = str
        # TODO: Make COAD stateless
        #self.store = dict()
        #self.populate_store()
        # Test for uid in data table, important for ordering of properties.
        # Occasionally missing from input files
        self.has_data_uid = False
        cur = self.dbcon.cursor()
        cur.execute('PRAGMA table_info(data)')
        for row in cur.fetchall():
            if 'uid' == row[1]:
                self.has_data_uid = True
                break
        # Sometimes there is no tag table
        self.has_tag_table = True
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='tag'")
        if len(cur.fetchall()) == 0:
            self.has_tag_table = False

    def populate_store(self):
        ''' Populate this map with class names and pointers to their classDict
            objects
        '''
        cur = self.dbcon.cursor()
        cur.execute("SELECT * FROM class")
        for row in cur.fetchall():
            c_meta = dict(zip([d[0] for d in cur.description], row))
            self.store[c_meta['name']] = ClassDict(self, c_meta)

    def save(self, filename):
        ''' Write current contents of database to xml
        '''
        plexos_database.save(self.dbcon, filename)

    def list(self, classname):
        ''' Return a list of all objects in class classname'''
        #with sql.connect(self.dbfilename) as con:
        cur = self.dbcon.cursor()
        list_select = ('SELECT name FROM object WHERE class_id IN '
                       '(SELECT class_id FROM class WHERE name=?)')
        cur.execute(list_select, [classname])
        return [o[0] for o in cur.fetchall()]

    def show(self, objname):
        ''' Print a list of all attributes in an object
            class_name.objname.attribute_name=attribute value

            attribute_data table has object_id, attribute_id, value
            attribute has attribute_name,
            object has object_id, class_id, object_name
            class has class_id,class_name

        '''
        #with sql.connect(self.dbfilename) as con:
        cur = self.dbcon.cursor()
        sel = '''SELECT c.name as class_name, o.name as objname,
                 a.name as attribute_name, ad.value as attribute_value
                 FROM object o
                 INNER JOIN class c ON c.class_id=o.class_id
                 INNER JOIN attribute_data ad ON ad.object_id = o.object_id
                 INNER JOIN attribute a ON a.attribute_id=ad.attribute_id
                 WHERE o.name=?'''
        cur.execute(sel, [objname])

        attributes = cur.fetchall()
        for att in attributes:
            print('%s.%s.%s=%s'%tuple(att))

    def get_by_object_id(self, object_id):
        ''' Return an ObjectDict based on object_id
        '''
        cur = self.dbcon.cursor()
        sel = '''SELECT o.name AS oname, c.name AS cname FROM object o
                 INNER JOIN class c ON c.class_id=o.class_id
                 WHERE object_id=?'''
        cur.execute(sel, [object_id])
        (oname, cname) = cur.fetchone()
        return self[cname][oname]
        #objcls = ClassDict(self, clsmeta)
        #return ObjectDict(objcls, objmeta)

    def get_by_hierarchy(self, identifier, default=None):
        ''' Return the ClassDict, ObjectDict or attribute value for an object
            class_name.object_name.attribute_name = attribute value
            or
            class_name|object_name|attribute_name = attribute value
            if one of the names has a . in it
        '''
        hier = identifier.split('.')
        if hier[0] not in self:
            hier = identifier.split('|')
        if hier[0] not in self:
            raise Exception("No such class '%s'"%hier[0])
        retobj = self[hier[0]]
        if len(hier) > 1:
            if hier[1] not in retobj:
                raise Exception("No such object '%s' in %s"%(hier[1], hier[0]))
            retobj = retobj[hier[1]]
            if len(hier) > 2:
                if hier[2] not in retobj:
                    raise Exception("No such attribute '%s' in %s"%(hier[2], hier[1]))
                retobj = retobj[hier[2]]
        return retobj

    def set(self, identifier, value):
        ''' Sets the attribute value for an object
            class_name.object_name.attribute_name=attribute value
            Will create a new row in attribute_data if no existing value is found
            '''
        try:
            (class_name, object_name, attribute_name) = identifier.split('.')
        except:
            raise Exception('''Invalid identifier, must take the form of:
                class name.object name.attribute name''')
        self[class_name][object_name][attribute_name] = value

    def diff(self, other_coad):
        ''' Print a difference between two coad objects
                For each key in each coad:
                    Report differences in keys
                    Report differences in values for each key
        '''
        other_keys = set(other_coad.keys())
        self_keys = set(self.keys())
        missing_keys = self_keys - other_keys
        if missing_keys:
            print("Missing keys: %s"%missing_keys)
        extra_keys = other_keys - self_keys
        if extra_keys:
            print("Extra keys: %s"%extra_keys)
        for key in self_keys & other_keys:
            cls_diff = self[key].diff(other_coad[key])
            if len(cls_diff) > 0:
                print("Difference in ClassDict %s"%key)
                for diff_msg in cls_diff:
                    print("  %s"%diff_msg)

    def diff_db(self, otherfilename):
        ''' Print a difference between two sqlite database files
                For each table in each db:
                    Report differences in schema
                    Report row differences
        '''
        def diff_table(table_name, cur1, cur2):
            ''' Print a difference between two tables
                First list schema differences
                Then data differences

                Assumes cursors have been created using sql.Row row_factory
            '''
            cur1.execute("SELECT * FROM '%s' ORDER BY 1,2"%(table_name))
            schema1 = [k[0] for k in cur1.description]
            data1 = cur1.fetchall()
            # Test the table on two - make sure all cols in one are still available
            cur2.execute("SELECT * FROM '%s' LIMIT 1"%(table_name))
            schema2 = [k[0] for k in cur2.description]
            if len(set(schema1) - set(schema2)) > 0:
                print("Table %s has different schemas"%table_name)
                return
            sel = "SELECT %s FROM '%s' ORDER BY 1,2"
            cur2.execute(sel%(','.join(["["+k+"]" for k in schema1]), table_name))
            data2 = cur2.fetchall()
            # At this point both data sets should be in the same order
            # For now use set functions to display differences
            in1 = set(data1) - set(data2)
            in2 = set(data2) - set(data1)
            if len(in1) > 0 or len(in2) > 0:
                print("Differences in table %s"%table_name)
                row_format = "{:>15}"*(len(schema1))
                if len(in1) > 0:
                    print("Only in original file:")
                    print(row_format.format(*schema1))
                    print('-'*15*len(schema1))
                    for i in in1:
                        print(row_format.format(*i))
                if len(in2) > 0:
                    print("Only in new file:")
                    print(row_format.format(*schema1))
                    print('-' * 15 * len(schema1))
                    for i in in2:
                        print(row_format.format(*i))
        if not otherfilename.endswith('.db'):
            raise Exception('Invalid filename extention for ' + otherfilename)
        self.dbcon.row_factory = sql.Row
        cur = self.dbcon.cursor()
        cur.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
        tables = [s[0] for s in cur.fetchall()]
        with sql.connect(otherfilename) as other_con:
            other_con.row_factory = sql.Row
            other_cur = other_con.cursor()
            other_cur.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
            other_tables = [s[0] for s in other_cur.fetchall()]
            # Tables in both dbs
            for tbl in set(tables) | set(other_tables):
                diff_table(tbl, cur, other_cur)
            # Tables only in first db
            for tbl in set(tables) - set(other_tables):
                print('Tables removed from first file')
            # Tables only in second db
            for tbl in set(other_tables) - set(tables):
                print('Tables added to first file')

    def __setitem__(self, key, value):
        raise Exception('Operation not supported yet')

    def __getitem__(self, key):
        cur = self.dbcon.cursor()
        cur.execute("SELECT * FROM class WHERE name=?", [key])
        row = cur.fetchone()
        if row is None:
            raise Exception("No such class %s"%key)
        c_meta = dict(zip([d[0] for d in cur.description], row))
        return ClassDict(self, c_meta)
        #for row in cur.fetchall():
        #    c_meta = dict(zip([d[0] for d in cur.description], row))
        #    self.store[c_meta['name']] = ClassDict(self, c_meta)
        #return self.store[key]

    def __delitem__(self, key):
        raise Exception('Operation not supported yet')
        #del self.store[key]

    def __iter__(self):
        cur = self.dbcon.cursor()
        cur.execute("SELECT name FROM class ORDER BY class_id")
        return iter([n[0] for n in cur.fetchall()])
        #return iter(self.store)

    def __len__(self):
        cur = self.dbcon.cursor()
        cur.execute("SELECT count(*) FROM class")
        return cur.fetchone()[0]
        #return iter([n[0] for n in cur.fetchall()])
        return len(self.store)

class ClassDict(collections.MutableMapping):
    '''
        meta is a dictionary describing the class to match the
        database entry

        Uses Abstract Base Classes to extend a dictionary
    '''
    def __init__(self, coad, meta):
        # TODO Remove store and have it be a better ORM
        self.store = dict()
        self.coad = coad
        self.meta = meta
        self.valid_properties = dict()
        cur = self.coad.dbcon.cursor()
        cur.execute("SELECT collection_id, parent_class_id FROM collection WHERE child_class_id=?", [self.meta['class_id']])
        collections = list(cur.fetchall())
        #collections = self.coad.db['collection'].find({'child_class_id':self.meta['class_id']})
        for coll in collections:
            #parent = coll['parent_class_id']
            parent = coll[1]
            cur.execute("SELECT name FROM class WHERE class_id=?", [parent])
            #parent_meta = self.coad.db['class'].find_one({'class_id':parent})
            parent_name = cur.fetchone()[0]
            cur.execute("SELECT * FROM property WHERE collection_id=?", [coll[0]])
            #props = self.coad.db['property'].find({'collection_id': coll['collection_id']})
            #for prop in props:
            for row in cur.fetchall():
                # SQLite needed ints for _id columns for PK, joins.  Convert back
                # to str for compatibility with all other data
                prop = dict(zip([d[0] for d in cur.description], [str(v) if isinstance(v, int) else v for v in row]))
                if parent_name not in self.valid_properties:
                    self.valid_properties[parent_name] = {}
                #if parent_meta['name'] not in self.valid_properties:
                #    self.valid_properties[parent_meta['name']] = {}
                if prop['property_id'] in self.valid_properties:
                    raise Exception("Duplicate property %s in class %s"%(prop['name'], self.meta['name']))
                #self.valid_properties[parent_meta['name']][prop['property_id']] = prop
                self.valid_properties[parent_name][prop['property_id']] = prop
        self.valid_properties_by_name = {}
        for p, pv in self.valid_properties.iteritems():
            self.valid_properties_by_name[p] = {}
            for k, v in pv.iteritems():
                if v['name'] in  self.valid_properties_by_name:
                    raise Exception("Duplicate property %s in class %s"%(v['name'], self.meta['name']))
                self.valid_properties_by_name[p][v['name']] = k
        #cur = self.coad.dbcon.cursor()
        #cur.execute("SELECT * FROM object WHERE class_id=?", [self.meta['class_id']])
        #for row in cur.fetchall():
        #    obj = dict(zip([d[0] for d in cur.description], row))
        #    if obj['name'] in self.store:
        #        msg = 'Duplicate name of object %s in class %s'
        #        raise Exception(msg%(obj['name'], self.meta['name']))
        #    self.store[obj['name']] = ObjectDict(self.coad, obj)

    def __setitem__(self, key, value):
        ''' Allow setting keys to an objectdict '''
        raise Exception('Opertation not supported yet')
        if not isinstance(value, ObjectDict):
            raise Exception('Unable to set Class child to anything but Object')
        # TODO: Some kind of validation in databaseland
        self.store[key] = value

    def __getitem__(self, key):
        cur = self.coad.dbcon.cursor()
        cur.execute("SELECT * FROM object WHERE class_id=? AND name=?", [self.meta['class_id'], key])
        objrow = cur.fetchone()
        obj = dict(zip([d[0] for d in cur.description], objrow))
        return ObjectDict(self.coad, obj)
        #return self.store[key]

    def __delitem__(self, key):
        # To remove this object:
        #     Remove all attribute data associated with the object
        #     Remove all records from membership where this is the parent_id
        #     TODO: Should objects not associated with any other object that
        #                 were children of this object be deleted as well?
        #     Remove record from object
        raise Exception('Opertation not supported yet')
        # TODO: remove attribute data from db
        #del self.store[key]

    def __iter__(self):
        # TODO: Just keys or keys and values?
        cur = self.coad.dbcon.cursor()
        cur.execute("SELECT name FROM object WHERE class_id=?", [self.meta['class_id']])
        return iter([row[0] for row in cur.fetchall()])
        #return iter(self.store)

    def __len__(self):
        cur = self.coad.dbcon.cursor()
        cur.execute("SELECT count(*) FROM object WHERE class_id=?", [self.meta['class_id']])
        return cur.fetchone()[0]
        #return len(self.store)

    def diff(self, other_class):
        ''' Return a list of difference between two ClassDict objects

        For each key in each ClassDict:
            Report differences in keys
            Report differences in ObjectDicts for each key
        '''
        diff_msg = []
        other_keys = set(other_class.keys())
        self_keys = set(self.keys())
        missing_keys = self_keys - other_keys
        if missing_keys:
            diff_msg.append("Missing ClassDict keys: %s"%missing_keys)
        extra_keys = other_keys - self_keys
        if extra_keys:
            diff_msg.append("Extra ClassDict keys: %s"%extra_keys)
        for key in self_keys & other_keys:
            obj_diff = self[key].diff(other_class[key])
            if len(obj_diff) > 0:
                diff_msg.append("Difference in ObjectDict %s"%key)
                diff_msg += obj_diff
        return diff_msg

    def get_collection_id(self, child_class_id):
        ''' Return the collection id that represents the relationship between
        this object's class and a child's class
            Collections appear to be another view of membership, maybe a list of
        allowed memberships
        '''
        cur = self.coad.dbcon.cursor()
        cur.execute("SELECT collection_id FROM collection WHERE parent_class_id=? AND child_class_id=?", [self.meta['class_id'], child_class_id])
        #collection = self.coad.db['collection'].find_one({'parent_class_id':self.meta['class_id'], 'child_class_id':child_class_id}, {'collection_id':1})
        collection = cur.fetchone()
        if collection is None:
            msg = 'Unable to find collection for the parent %s and child %s'
            raise Exception(msg%(self.meta['class_id'], child_class_id))
        return str(collection[0])

    def get_categories(self):
        ''' Return a list of category dicts available for objects of this class, ordered
        by rank.
        '''
        cur = self.coad.dbcon.cursor()
        cur.execute("SELECT * FROM category WHERE class_id=?", [self.meta['class_id']])
        unsorted_cats = []
        for row in cur.fetchall():
            cat = dict(zip([d[0] for d in cur.description], row))
            unsorted_cats.append(cat)
        categories = sorted(unsorted_cats, key=lambda c: int(c['rank']))
        return categories

    def add_category(self, name):
        ''' Add a new category to this class, not allowing duplicated names in class
        '''
        # Get existing categories for class
        cur = self.coad.dbcon.cursor()
        cur.execute("SELECT * FROM category WHERE class_id=?", [self.meta['class_id']])
        lastrank = -1
        for row in cur.fetchall():
            cat = dict(zip([d[0] for d in cur.description], row))
            lastrank = max(int(cat['rank']), lastrank)
            if cat['name'] == 'name':
                raise Exception("Category %s already exists in %s"%(name, self.meta['name']))
        cmd = "INSERT INTO category (name,rank,class_id) VALUES (?,?,?)"
        vls = [name, str(lastrank+1), self.meta['class_id']]
        cur.execute(cmd, vls)
        self.coad.dbcon.commit()

    # TODO: Any need for remove category?  Would have to change objects that use
    # the deleted category to the default

class ObjectDict(collections.MutableMapping):
    ''' Overwrites the setitem method to allow updates to data and dict
        Works by using the list of attribute and attribute data dicts
        and manipulating the original database as needed

        meta is a dictionary describing the object as it is described in the
        database

        Uses Abstract Base Classes to extend a dictionary
    '''
    def __init__(self, coad, meta):
        self.store = dict()
        self.coad = coad
        self.meta = meta
        self.hierarchy = '%s.%s'%(self.get_class().meta['name'], self.meta['name'])
        cur = self.coad.dbcon.cursor()
        self._no_update = True
        # Check for attributes.  Output xml does not populate attribute data.
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='attribute_data'")
        if len(cur.fetchall()) == 1:
            # Populate current values
            cmd = '''SELECT a.name as attribute_name, ad.value as attribute_value
                     FROM attribute_data ad INNER JOIN attribute a
                     ON a.attribute_id=ad.attribute_id WHERE ad.object_id=?'''
            cur.execute(cmd, [self.meta['object_id']])
            for atr in cur.fetchall():
                self[atr[0]] = atr[1]
            # Populate allowed values
            self.valid_attributes = {}
            cmd = '''SELECT a.* FROM attribute a
            INNER JOIN object o ON o.class_id=c.class_id
            INNER JOIN class c ON o.class_id=a.class_id
            WHERE o.object_id=?'''
            cur.execute(cmd, [self.meta['object_id']])
            for row in cur.fetchall():
                atr = dict(zip([d[0] for d in cur.description], row))
                self.valid_attributes[atr['name']] = atr
        self._no_update = False

    def __setitem__(self, key, value):
        if self._no_update:
            self.store[key] = value
            return
        # TODO: Make sure value is valid
        # Make sure this attribute is allowed in this class
        if key not in self.valid_attributes:
            msg = '%s is not a valid attribute of object %s, valid attributes:%s'
            raise Exception(msg%(key, self.meta['name'], self.valid_attributes.keys()))
        cur = self.coad.dbcon.cursor()
        cmd = "UPDATE attribute_data SET value=? WHERE object_id=? and attribute_id=?"
        vls = [value, self.meta['object_id'], self.valid_attributes[key]['attribute_id']]
        cur.execute(cmd, vls)
        if cur.rowcount == 0:
            # Did not work, add a new row
            cmd = "INSERT INTO attribute_data (object_id,attribute_id,value) VALUES (?,?,?)"
            vls = [self.meta['object_id'], self.valid_attributes[key]['attribute_id'], value]
            cur.execute(cmd, vls)
        self.coad.dbcon.commit()
        self.store[key] = value

    def __getitem__(self, key):
        return self.store[key]

    def __delitem__(self, key):
        cur = self.coad.dbcon.cursor()
        cmd = "DELETE FROM attribute_data WHERE object_id=? AND attribute_id=?"
        vls = [self.meta['object_id'], self.valid_attributes[key]['attribute_id']]
        cur.execute(cmd, vls)
        self.coad.dbcon.commit()
        del self.store[key]

    def __iter__(self):
        return iter(self.store)

    def __len__(self):
        return len(self.store)

    def __str__(self):
        return repr(self.store)

    def copy(self, newname=None):
        ''' Create a new object entry in the database, duplicate all the
            attribute_data entries as well.
            # TODO: Enforce unique naming
        '''
        # Verify there is no existing object of this class with this name
        if newname in self.get_class().keys():
            raise Exception("Duplicate name '%s' for same class"%newname)
        cols = []
        vals = []
        for (k, val) in self.meta.items():
            # GUID is new in version 7, and must be unique across all objects
            if k == 'GUID':
                cols.append(k)
                vals.append(str(uuid.uuid4()))
            if k != 'object_id':
                cols.append(k)
                if k == 'name':
                    if newname is None:
                        newname = self.meta['name'] + '-' + str(uuid.uuid4())
                        #val = self.meta['name'] + '-' + str(uuid.uuid4())
                    #else:
                    val = newname
                vals.append(val)
        cur = self.coad.dbcon.cursor()
        fill = ','.join('?'*len(cols))
        cmd = "INSERT INTO object (%s) VALUES (%s)"%(','.join(["'%s'"%c for c in cols]), fill)
        cur.execute(cmd, vals)
        self.coad.dbcon.commit()
        #new_obj_meta = dict(zip(cols, vals))
        #new_obj_meta['object_id'] = cur.lastrowid
        #new_obj_dict = ObjectDict(self.coad, new_obj_meta)
        new_obj_dict = self.get_class()[newname]
        for (k, val) in self.store.items():
            new_obj_dict[k] = val
        # Add this objectdict to classdict
        #new_obj_dict.get_class()[new_obj_meta['name']] = new_obj_dict
        # Create new the membership information
        # TODO: Is it possible to have orphans by not checking child_object_id?
        cur.execute("SELECT * FROM membership WHERE parent_object_id=?",
                    [self.meta['object_id']])
        cols = [d[0] for d in cur.description]
        parent_object_id_idx = cols.index('parent_object_id')
        for row in cur.fetchall():
            newrow = list(row)
            newrow[parent_object_id_idx] = new_obj_dict.meta['object_id']
            cmd = "INSERT INTO membership (%s) VALUES (%s)"
            vls = (','.join(["'"+c+"'" for c in cols[1:]]), ','.join(['?' for d in newrow[1:]]))
            cur.execute(cmd%vls, newrow[1:])
        # Copy memberships where this is the child
        cur.execute("SELECT * FROM membership WHERE child_object_id=?",
                    [self.meta['object_id']])
        cols = [d[0] for d in cur.description]
        child_object_id_idx = cols.index('child_object_id')
        for row in cur.fetchall():
            newrow = list(row)
            newrow[child_object_id_idx] = new_obj_dict.meta['object_id']
            cmd = "INSERT INTO membership (%s) VALUES (%s)"
            vls = (','.join(["'"+c+"'" for c in cols[1:]]), ','.join(['?' for d in newrow[1:]]))
            cur.execute(cmd%vls, newrow[1:])
        self.coad.dbcon.commit()
        return new_obj_dict

    def set_children(self, children, replace=True):
        ''' Set the children of this object.    If replace is true, it will
        remove any existing children matching the classes passed in otherwise it
        will append the data.
        Can handle either a single ObjectDict or list of ObjectDicts
        TODO: Validate that object is allowed to have the children passed in
        '''
        children_by_class = {}
        if isinstance(children, ObjectDict):
            class_id = children.get_class().meta['class_id']
            children_by_class[class_id] = [children]
        else:
            for obj in children:
                if not isinstance(obj, ObjectDict):
                    msg = "Children must be of type ObjectDict, passed item was %s"
                    raise Exception(msg%(type(obj)))
                class_id = obj.get_class().meta['class_id']
                if class_id not in children_by_class.keys():
                    children_by_class[class_id] = [obj]
                else:
                    children_by_class[class_id].append(obj)
        cur = self.coad.dbcon.cursor()
        for (class_id, objectdicts) in children_by_class.items():
            if replace:
                cmd = "DELETE FROM membership WHERE parent_object_id=? AND child_class_id=?"
                cur.execute(cmd, [self.meta['object_id'], class_id])
            collection_id = self.get_collection_id(class_id)
            for obj in objectdicts:
                cmd = '''INSERT INTO membership (parent_class_id, parent_object_id,
                         collection_id, child_class_id, child_object_id)
                         VALUES (?,?,?,?,?)'''
                vls = [self.meta['class_id'], self.meta['object_id'], collection_id,
                       class_id, obj.meta['object_id']]
                cur.execute(cmd, vls)
        self.coad.dbcon.commit()

    def get_parents(self, class_name=None):
        ''' Return a list of all parents that match the class name.  If class
        name is None, return all parents
        '''
        parents = []
        cur = self.coad.dbcon.cursor()
        cmd = "SELECT parent_object_id FROM membership"

        s_params = [self.meta['object_id']]
        if class_name:
            cmd += " INNER JOIN class c ON c.class_id=parent_class_id"
        cmd += " WHERE child_object_id=?"
        if class_name:
            cmd += " AND c.name=?"
            s_params.append(class_name)
        cur.execute(cmd, s_params)
        for row in cur.fetchall():
            parents.append(self.coad.get_by_object_id(row[0]))
        return parents

    def get_children(self, class_name=None):
        ''' Return a list of all children that match the class name
        '''
        children = []
        cur = self.coad.dbcon.cursor()
        select = '''SELECT c.name AS class_name,o.name AS object_name
                 FROM membership
                 INNER JOIN class c ON c.class_id=child_class_id
                 INNER JOIN object o ON o.object_id=child_object_id
                 WHERE parent_object_id=?'''
        s_params = [self.meta['object_id']]
        if class_name is not None:
            select = select + " and c.name=?"
            s_params.append(class_name)
        cur.execute(select, s_params)
        for row in cur.fetchall():
            children.append(self.coad[row[0]][row[1]])
        return children

    def get_category(self):
        ''' Return the name of this object's category
        '''
        cur = self.coad.dbcon.cursor()
        cmd = 'SELECT name FROM category WHERE category_id=?'
        cur.execute(cmd, [self.meta['category_id']])
        return cur.fetchone()[0]

    def set_category(self, name):
        ''' Set this object's category to name
        '''
        available_cats = self.get_class().get_categories()
        for cat in available_cats:
            if cat['name'] == name:
                cur = self.coad.dbcon.cursor()
                cur.execute("""UPDATE object SET category_id=? WHERE object_id=?""", [cat['category_id'], self.meta['object_id']])
                self.coad.dbcon.commit()
                return
        raise Exception("No such category %s for class %s"%(name, self.get_class().meta['name']))

    def get_class(self):
        ''' Return the ClassDict that contains this object
        '''
        for class_dict in self.coad.values():
            if class_dict.meta['class_id'] == self.meta['class_id']:
                return class_dict
        raise Exception('Unable to find class associated with object')

    def get_collection_id(self, child_class_id):
        ''' Return the collection id that represents the relationship between
        this object's class and a child's class
            Collections appear to be another view of membership, maybe a list of
        allowed memberships
        '''
        cur = self.coad.dbcon.cursor()
        cmd = '''SELECT collection_id FROM collection WHERE parent_class_id=?
                 AND child_class_id=?'''
        cur.execute(cmd, [self.meta['class_id'], child_class_id])
        rows = cur.fetchall()
        if len(rows) != 1:
            msg = 'Unable to find collection for the parent %s and child %s'
            raise Exception(msg%(self.meta['class_id'], child_class_id))
        return rows[0][0]

    def get_properties(self):
        '''Return a dict of all properties set for this object along with any
        properties tagged to another object.

        Tagged properties apply only to tag object

        Returns:
            dict of class/object_hierarchy=dict of property_name=value
        '''
        cur = self.coad.dbcon.cursor()
        props = {}
        # Sometimes there is no data table
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='data'")
        if len(cur.fetchall()) == 0:
            return props
        cmd = "SELECT parent_object_id, membership_id FROM membership WHERE child_object_id=?"
        cur.execute(cmd, [self.meta['object_id']])
        memberships = list(cur.fetchall())
        #memberships = self.clsdict.coad.db['membership'].find({'child_object_id':self.meta['object_id']})
        for member in memberships:
            parent = self.coad.get_by_object_id(member[0])
            cmd = "SELECT * FROM data WHERE membership_id=?"
            if self.coad.has_data_uid:
                cmd += " ORDER BY uid"
            cur.execute(cmd, [member[1]])
            data = []
            for row in cur.fetchall():
                data.append(dict(zip([d[0] for d in cur.description], row)))
            #data = self.clsdict.coad.db['data'].find({'membership_id':member['membership_id']}).sort('uid', 1)
            for d in data:
                tag_hier = parent.hierarchy
                if self.coad.has_tag_table:
                    # Can parents and tags coexist? Yes!  It appears the data_id
                    # shown in the tag is the overwritten value of the default.
                    # Check for tag, which is modified data for a specific data_id
                    cmd = "SELECT object_id FROM tag WHERE data_id=?"
                    #tag = self.clsdict.coad.db['tag'].find_one({'data_id':d['data_id']})
                    cur.execute(cmd, [d['data_id']])
                    tag = cur.fetchone()
                    if tag is not None:
                        # TODO: Ever multiple tags for the same data_id?
                        tag_obj = self.coad.get_by_object_id(tag[0])
                        #raise Exception("Found a tag! parent %s, tag %s"%(parent.hierarchy, tag_obj.hierarchy))
                        tag_hier = tag_obj.hierarchy
                name = self.get_class().valid_properties[parent.get_class().meta['name']][str(d['property_id'])]['name']
                # Test for input mask, substituting if needed
                cmd = "SELECT input_mask FROM property WHERE property_id=?"
                #tag = self.clsdict.coad.db['tag'].find_one({'data_id':d['data_id']})
                cur.execute(cmd, [d['property_id']])
                prop = cur.fetchone()
                #prop = self.clsdict.coad.db['property'].find_one({'property_id':d['property_id']})
                valdict = {}
                if prop[0] is not None:
                    mask = prop[0].split(";")
                    it = iter(mask)
                    for k in it:
                        valdict[str(k)] = next(it).strip("\"")
                if d['value'] in valdict:
                    value = valdict[d['value']]
                else:
                    value = d['value']
                if tag_hier not in props:
                    props[tag_hier] = {}
                if name not in props[tag_hier]:
                    props[tag_hier][name] = value
                else:
                    if not isinstance(props[tag_hier][name], list):
                        props[tag_hier][name] = [props[tag_hier][name], value]
                    else:
                        props[tag_hier][name].append(value)
        return props
        '''Return a dict of all properties set for this object
        '''
        cur = self.coad.dbcon.cursor()
        props = {}
        # Check for table "data"
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='data'")
        if len(cur.fetchall()) == 1:
            cmd = """SELECT p.name, d.value FROM data d
                INNER JOIN property p ON p.property_id = d.property_id
                WHERE membership_id IN
                (SELECT membership_id FROM membership WHERE child_object_id=?)"""
            if self.coad.has_data_uid:
                cmd += " ORDER BY d.uid"
            cur.execute(cmd, [self.meta['object_id']])
            for (name, value) in cur.fetchall():
                if name not in props:
                    props[name] = value
                else:
                    if not isinstance(props[name], list):
                        props[name] = [props[name], value]
                    else:
                        props[name].append(value)
        return props

    def get_property(self, name, tag='System.System'):
        '''Return the value of a property by name
        '''
        if isinstance(tag, ObjectDict):
            tag_obj = tag
        else:
            tag_obj = self.coad.get_by_hierarchy(tag)
        tag_clsname = tag_obj.get_class().meta['name']
        # Reverse lookup of class.valid_properties to get property_id
        if name not in self.get_class().valid_properties_by_name[tag_clsname]:
            raise Exception('"%s" is not a valid property for class %s'%(name, tag_clsname))
        prop_id = self.get_class().valid_properties_by_name[tag_clsname][name]
        # Tag object should always be ObjectDict
        tag_obj_id = tag_obj.meta['object_id']
        cmd = "SELECT membership_id FROM membership WHERE child_object_id=? AND parent_object_id=?"
        cur = self.coad.dbcon.cursor()
        cur.execute(cmd, [self.meta['object_id'], tag_obj_id])
        member = cur.fetchone()
        #member = self.clsdict.coad.db['membership'].find_one({'child_object_id':self.meta['object_id'], 'parent_object_id':tag_obj_id}, {'membership_id':1})
        if member is None:
            raise Exception("Unable to find membership for %s in %s"%(tag.hierarchy, self.hierarchy))
        # Test for input mask, substituting if needed
        cmd = "SELECT input_mask FROM property WHERE property_id=?"
        cur.execute(cmd, [prop_id])
        prop = cur.fetchone()
        #prop = self.clsdict.coad.db['property'].find_one({'property_id':prop_id})
        valdict = {}
        if prop[0] is not None:
            mask = prop[0].split(";")
            it = iter(mask)
            for k in it:
                valdict[str(k)] = next(it).strip("\"")
        def valmap(val):
            if val in valdict:
                return valdict[val]
            else:
                return val
        cmd = "SELECT value FROM data WHERE membership_id=? AND property_id=?"
        if self.coad.has_data_uid:
            cmd += " ORDER BY uid"
        cur.execute(cmd, [member[0], prop_id])
        all_data = cur.fetchall()
        mapped_data = [valmap(d[0]) for d in all_data]
        data_count = len(mapped_data)
        if data_count == 0:
            raise Exception("No exisiting data found for membership %s"%member['membership_id'])
        elif data_count == 1:
            return mapped_data[0]
        else:
            return mapped_data

    def set_property(self, name, value, tag='System.System'):
        '''Set the value of a property by name
        Limited to modifying existing values.  Will not add new data.
        '''
        if isinstance(value, list):
            raise Exception("Overwriting list of data not supported yet")
        cur = self.coad.dbcon.cursor()
        tag_obj = self.coad.get_by_hierarchy(tag)
        tag_clsname = tag_obj.get_class().meta['name']
        # Commonly used method for converting human value to stored value
        def get_mask_value(value, mask=None):
            '''Using the property input_mask attribute, map value to a valid
            value and return it'''
            valdict = {}
            if mask:
                vv = []
                mask_s = mask.split(";")
                it = iter(mask_s)
                for k in it:
                    mval = next(it).strip("\"")
                    if mval == value:
                        return k
                    vv.append(mval)
                raise Exception("Value '%s' not in property's input_mask.  Valid values are:\n%s\n"%(value,'\n'.join(vv)))
            else:
                return value
        # If the tagged class doesn't have the property as valid, it's set as a tag
        if tag_clsname not in self.get_class().valid_properties_by_name:
            # Modify if value is already set
            cmd = "SELECT data_id FROM tag WHERE object_id=?"
            cur.execute(cmd, [tag_obj.meta['object_id']])
            possible_tags = list(cur.fetchall())
            #possible_tags = self.clsdict.coad.db['tag'].find({'object_id':tag_obj.meta['object_id']})
            for ptag in possible_tags:
                # Get property name, see if it matches name
                cmd = "SELECT property_id, membership_id FROM data WHERE data_id=?"
                cur.execute(cmd, [ptag[0]])
                #ptag_data = self.clsdict.coad.db['data'].find_one({'data_id':ptag['data_id']})
                ptag_data = cur.fetchone()
                cmd = "SELECT name, is_dynamic, input_mask FROM property WHERE property_id=?"
                cur.execute(cmd, [ptag_data[0]])
                ptag_prop = cur.fetchone()
                #ptag_prop = self.clsdict.coad.db['property'].find_one({'property_id':ptag_data['property_id']})
                if ptag_prop[0] == name:
                    # If it does, see if the membership matches this object
                    cmd = "SELECT child_object_id FROM membership WHERE membership_id=?"
                    cur.execute(cmd, [ptag_data[1]])
                    ptag_member = cur.fetchone()
                    #ptag_member = self.clsdict.coad.db['membership'].find_one({'membership_id':ptag_data['membership_id']})
                    # If it matches, set the value
                    if ptag_member[0] == self.meta['object_id']:
                        # Get the masked value before is_dynamic is updated
                        m_value = get_mask_value(value, ptag_prop[2])
                        # Make sure property has dynamic set to true
                        if ptag_prop[1] != 'true':
                            cmd = "UPDATE property SET is_dynamic=true WHERE property_id=?"
                            cur.execute(cmd, [ptag_data[0]])
                            #self.clsdict.coad.db['property'].update(ptag_prop, {'$set': {'is_dynamic': 'true'}})
                        cmd = "UPDATE data SET value=? WHERE data_id=?"
                        cur.execute(cmd, [m_value, ptag[0]])
                        #self.clsdict.coad.db['data'].update(ptag_data, {'$set': {'value': m_value}})
                        self.coad.dbcon.commit()
                        return
            # Add new tag and data here
            prop_id = self.get_class().valid_properties_by_name['System'][name]
            cmd = "SELECT input_mask, is_dynamic FROM property WHERE property_id=?"
            cur.execute(cmd, [prop_id])
            prop = cur.fetchone()
            #prop = self.clsdict.coad.db['property'].find_one({'property_id':prop_id})
            # Get the masked value before is_dynamic is updated
            m_value = get_mask_value(value, prop[0])
            # Make sure is_dynamic is set to true
            if prop[1] != 'true':
                cmd = "UPDATE property SET is_dynamic='true' WHERE property_id=?"
                cur.execute(cmd, [prop_id])
                self.coad.dbcon.commit()
                #self.clsdict.coad.db['property'].update(prop, {'$set': {'is_dynamic': 'true'}})
            # Add new data
            cmd = "SELECT data_id, uid FROM data"
            cur.execute(cmd)
            data_id_list = list(cur.fetchall())
            #data_id_list = list(self.clsdict.coad.db['data'].find( {}, { '_id': 0, 'data_id': 1, 'uid': 1 } ))
            last_data_id = max(map(int, [x[0] for x in data_id_list]))
            last_uid = max(map(int, [x[1] for x in data_id_list]))
            sys_obj = self.coad.get_by_hierarchy('System.System')
            cmd = "SELECT membership_id FROM membership WHERE child_object_id=? AND parent_object_id=?"
            cur.execute(cmd, [self.meta['object_id'], sys_obj.meta['object_id']])
            member = cur.fetchone()
            #member = self.clsdict.coad.db['membership'].find_one({'child_object_id':self.meta['object_id'], 'parent_object_id':sys_obj.meta['object_id']}, {'membership_id':1})
            cmd = "INSERT INTO data (data_id,uid,membership_id,value,property_id) VALUES (?,?,?,?,?)"
            cur.execute(cmd, [last_data_id+1, str(last_uid+1), member[0], m_value, prop_id])
            #self.clsdict.coad.db['data'].insert({'data_id':str(last_data_id+1),
            #                             'uid':str(last_uid+1),
            #                             'membership_id':member['membership_id'],
            #                             'value':m_value,
            #                             'property_id':prop_id})
            # Add new tag
            cmd = "INSERT INTO tag (data_id, object_id) VALUES (?,?)"
            cur.execute(cmd, [last_data_id+1, tag_obj.meta['object_id']])
            self.coad.dbcon.commit()
            #self.clsdict.coad.db['tag'].insert({'data_id':str(last_data_id+1),
            #                            'object_id':tag_obj.meta['object_id']})
        else:
            # Reverse lookup of class.valid_properties to get property_id
            if name not in self.get_class().valid_properties_by_name[tag_clsname]:
                raise Exception('"%s" is not a valid property for class %s'%(name, tag_clsname))
            prop_id = self.get_class().valid_properties_by_name[tag_clsname][name]
            cmd = "SELECT input_mask FROM property WHERE property_id=?"
            cur.execute(cmd, [prop_id])
            prop = cur.fetchone()
            #prop = self.clsdict.coad.db['property'].find_one({'property_id':prop_id})
            # Tag object should always be ObjectDict
            tag_obj_id = tag_obj.meta['object_id']
            cmd = "SELECT membership_id FROM membership WHERE child_object_id=? AND parent_object_id=?"
            cur.execute(cmd, [self.meta['object_id'], tag_obj_id])
            member = cur.fetchone()
            #member = self.clsdict.coad.db['membership'].find_one({'child_object_id':self.meta['object_id'], 'parent_object_id':tag_obj_id}, {'membership_id':1})
            if member is None:
                raise Exception("Unable to find membership for %s in %s"%(tag, self.meta['name']))
            cmd = "SELECT data_id FROM data WHERE membership_id=? AND property_id=?"
            cur.execute(cmd, [member[0], prop_id])
            all_data = list(cur.fetchall())
            #all_data = self.clsdict.coad.db['data'].find({'membership_id':member['membership_id'], 'property_id':prop_id}).sort('uid', 1)
            data_count = len(all_data)
            if data_count == 0:
                raise Exception("No exisiting data found for membership %s"%member[0])
            elif data_count == 1:
                # Can replace this data
                data = all_data[0]
                cmd = "UPDATE data SET value=? WHERE data_id=?"
                cur.execute(cmd, [get_mask_value(value, prop[0]), data[0]])
                self.coad.dbcon.commit()
                #self.clsdict.coad.db['data'].update(data, {'$set': {'value': get_mask_value(prop, value)}})
            else:
                raise Exception('Overwriting list of data not supported yet')
        return



        #TODO: Handle arrays of values
        cur = self.coad.dbcon.cursor()
        cmd = """SELECT d.data_id FROM data d
            INNER JOIN property p ON p.property_id = d.property_id
            WHERE p.name=?
            AND membership_id IN
            (SELECT membership_id FROM membership WHERE child_object_id=?)"""
        if self.coad.has_data_uid:
            cmd += " ORDER BY d.uid"
        cur.execute(cmd, [name, self.meta['object_id']])
        match_data = cur.fetchall()
        if isinstance(value, list):
            if len(value) != len(match_data):
                msg = 'Property "%s" expects %s values, %s provided'
                raise Exception(msg%(name, len(match_data), len(value)))
            cur.executemany('UPDATE data SET value=? WHERE data_id=?',
                            zip(value, [x[0] for x in match_data]))
        elif len(match_data) != 1:
            raise Exception('Unable to find single property to modify for %s'%name)
        else:
            data_id = match_data[0][0]
            cur.execute("""UPDATE data SET value=? WHERE data_id=?""", [value, data_id])
            if cur.rowcount != 1:
                raise Exception('Unable to set property %s, %s rows affected'%(name, cur.rowcount))
        self.coad.dbcon.commit()

    def set_properties(self, new_dict):
        '''Set all the propery values present in dict

            NOTE: This is not transactional.  A failure may leave some values set,
            others not set.
        '''
        for name, value in new_dict.iteritems():
            self.set_property(name, value)

    def dump(self, recursion_level=0):
        ''' Print to stdout as much information as possible about object to facilitate debugging
        '''
        spacing = '        '*recursion_level
        msg = 'Object:    {:<30}            ID: {}'.format(self.meta['name'],
                                                             self.meta['object_id'])
        print(spacing + msg)
        msg = '    Class: {:<30}            ID: {}'.format(self.get_class().meta['name'],
                                                             self.meta['class_id'])
        print(spacing + msg)
        if self.keys():
            print(spacing+'    Attributes set:')
            for atr in self.items():
                print(spacing+'        %s = %s'%atr)
        else:
            print(spacing+'    No attributes set')
        all_children = set([o.hierarchy for o in self.get_children()])
        all_parents = set([o.hierarchy for o in self.get_parents()])
        children = all_children - all_parents
        parents = all_parents - all_children
        peers = all_children & all_parents
        if len(parents):
            print(spacing+'    Parents (%s):'%len(parents))
            for p in parents:
                msg = '        '+p
                print(spacing + msg)
        else:
            print(spacing+'    No parents')
        if len(peers):
            print(spacing+'    Peers (%s):'%len(peers))
            for p in peers:
                msg = '        '+p
                print(spacing + msg)
        else:
            print(spacing+'    No peers')
        if len(children):
            print(spacing+'    Children (%s):'%len(children))
            for k in children:
                self.get_class().coad.get_by_hierarchy(k).dump(recursion_level+1)
        else:
            print(spacing+'    No children')

    def print_object_attrs(self):
        ''' Prints the object's attributes in Class.Object.Attribute=Value format
        '''
        c_name = self.get_class().meta['name']
        for (key, val) in self.items():
            print('%s.%s.%s=%s'%(c_name, self.meta['name'], key, val))

    def diff(self, other_obj):
        ''' Return a list of differences between two ObjectDicts

        For each key in each ObjectDict:
            Report differences in keys
            Report differences in values for each key

        Compare Attribute Data
        Compare Properties
        Compare Children
        '''
        diff_msg = []
        other_keys = set(other_obj.keys())
        self_keys = set(self.keys())
        missing_keys = self_keys - other_keys
        if missing_keys:
            diff_msg.append("  Missing ObjectDict keys: %s"%missing_keys)
        extra_keys = other_keys - self_keys
        if extra_keys:
            diff_msg.append("  Extra ObjectDict keys: %s"%extra_keys)
        for key in self_keys & other_keys:
            if self[key] != other_obj[key]:
                diff_msg.append("  Different Object Value for %s"%key)
                diff_msg.append("    Orig: %s Comp: %s"%(self[key], other_obj[key]))
        other_props = other_obj.get_properties()
        self_props = self.get_properties()
        other_props_keys = set(other_props.keys())
        self_props_keys = set(self_props.keys())
        missing_props = self_props_keys - other_props_keys
        if missing_props:
            diff_msg.append("  Missing ObjectDict props: %s"%missing_props)
        extra_props = other_props_keys - self_props_keys
        if extra_props:
            diff_msg.append("  Extra ObjectDict props: %s"%extra_props)
        for key in self_props_keys & other_props_keys:
            if self_props[key] != other_props[key]:
                diff_msg.append("  Different Property Value for %s"%key)
                diff_msg.append("    Orig: %s Comp: %s"%(self_props[key], other_props[key]))
        # Children diff
        other_kids = other_obj.get_children()
        self_kids = self.get_children()
        # TODO: How to describe differences in children, name, id?
        return diff_msg
