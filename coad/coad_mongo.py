"""Class-Object-Attribute Dictionary Tools Using MongoDB

This module contains tools for manipulating Plexos data files outside of the
Plexos UI.  It loads the xml file into a mongo database for further inspection
and modification.

Example:
    from coad_mongo import COAD
    coad = COAD("master.xml")
    print("Before set, solver is %s"%coad['Performance']['Gurobi']['SOLVER'])
    coad['Performance']['Gurobi']['SOLVER'] = 3
    coad.save("master_new.xml")
    coad_new = COAD("master_new.xml")
    print("After set, solver is %s"%coad['Performance']['Gurobi']['SOLVER'])
"""
import collections
import os
import pymongo
#import sqlite3 as sql
import uuid

import plexos_mongo

class COAD(collections.MutableMapping):
    '''Edit models, horizons, memberships and object attributes of plexos data.
    Quickly modify the largest xml files for simulation.

    Instantiation will import xml data into a mongo database or open an
    existing mongo database of plexos data

    The class presents a map of class names to ClassDict objects
    '''

    def __init__(self, filename=None, reload=True, host='localhost', port=27017):
        '''Initialize the COAD object, populating Classes, Objects and Attributes

        Args:
            filename - Name of plexos input file to use, must end in xml.  If
                not provided, the master.xml is used
            reload - Reload the mongo database from file, defaults to True
            host - Hostname of MongoDB
            port - Port of MongoDB
        '''
        if filename is None:
            filename = os.path.join(os.path.abspath(os.path.dirname(__file__)), "master.xml")
        # Check for database in mongo.
        dbname = os.path.basename(filename).translate(None, '.$')
        if not reload:
            client = pymongo.MongoClient(host, port)
            self.db = client[dbname]
        else:
            try:
                with open(filename):
                    pass
            except:
                raise Exception('Unable to open %s'%filename)
            if not filename.endswith('.xml'):
                raise Exception('Invalid filename suffix')
            self.db = plexos_mongo.load(filename, host=host, port=port)
        # Add map of property ids to property names
        all_properties = self.db['property'].find({}, {'name':1, 'property_id':1, '_id':0})
        self.valid_properties = dict()
        for p in all_properties:
            self.valid_properties[p['property_id']] = p['name']

    def save(self, filename):
        ''' Write current contents of database to xml
        '''
        plexos_mongo.save(self.db, filename)

    def list(self, classname):
        ''' Return a list of all objects in class classname'''
        # Get class_id for classname
        cls_id = self.db['class'].find_one({'name':classname}, {'class_id':1, '_id':0})
        objs = self.db['object'].find({'class_id':cls_id['class_id']}, {'name':1, '_id':0})
        return [o['name'] for o in objs]

    def show(self, objname):
        ''' Print a list of all attributes in an object
            class_name.objname.attribute_name=attribute value

            attribute_data table has object_id, attribute_id, value
            attribute has attribute_name,
            object has object_id, class_id, object_name
            class has class_id,class_name
        '''
        objects = self.db['object'].find({'name':objname}, {'class_id':1, 'object_id':1, '_id':0})
        for obj in objects:
            clsdict =  self.db['object'].find_one({'class_id': obj['class_id']}, {'name':1, '_id':0})
            attributes = self.db['attribute'].find({'class_id':obj['class_id']}, {'name':1, 'attribute_id':1, '_id':0})
            for att in attributes:
                att_data = self.db['attribute_data'].find_one({'attribute_id':att['attribute_id'], 'object_id':obj['object_id']}, {'value':1, '_id':0})
                if att_data is not None:
                    print('%s.%s.%s=%s'%(clsdict['name'], objname, att['name'], att_data['value']))

    def get(self, identifier, default=None):
        ''' Return the attribute value for an object
            class_name.object_name.attribute_name=attribute value

            attribute_data table has object_id, attribute_id, value
            attribute has attribute_name,
            object has object_id, class_id, object_name
            class has class_id,class_name

            TODO: Use default as inherited from MutableMapping
        '''
        try:
            (class_name, object_name, attribute_name) = identifier.split('.')
        except:
            raise Exception('''Invalid identifier, must take the form of:
                class name.object name.attribute name''')
        return self[class_name][object_name][attribute_name]

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

    def __setitem__(self, key, value):
        raise Exception('Operation not supported yet')

    def __getitem__(self, key):
        meta = self.db['class'].find_one({'name':key})
        return ClassDict(self, meta)

    def __delitem__(self, key):
        raise Exception('Operation not supported yet')

    def __iter__(self):
        cls_dicts = self.db['class'].find({},{'name':1, '_id':0})
        return iter([c['name'] for c in cls_dicts])

    def __len__(self):
        return self.coad.db['class'].count()

class ClassDict(collections.MutableMapping):
    '''
        meta is a dictionary describing the class to match the
        database entry

        Uses Abstract Base Classes to extend a dictionary
    '''
    def __init__(self, coad, meta):
        self.store = dict()
        self.coad = coad
        self.meta = meta
        # TODO: Add more info to valid attributes
        attributes = self.coad.db['attribute'].find({'class_id':self.meta['class_id']})
        self.valid_attributes = dict()
        for att in attributes:
            self.valid_attributes[att['attribute_id']] = att['name']
        # TODO: Duplicate names ever happen?
        self.named_valid_attributes = {v: k for k, v in self.valid_attributes.items()}
        objects = self.coad.db['object'].find({'class_id':self.meta['class_id']})
        for objdoc in objects:
            if objdoc['name'] in self.store:
                msg = 'Duplicate name of object %s in class %s'
                raise Exception(msg%(objdoc['name'], self.meta['name']))
            self.store[objdoc['name']] = ObjectDict(self, objdoc)

    def __setitem__(self, key, value):
        ''' Allow setting keys to an objectdict '''
        raise Exception('Opertation not supported yet')
        if not isinstance(value, ObjectDict):
            raise Exception('Unable to set Class child to anything but Object')
        # TODO: Some kind of validation in databaseland
        self.store[key] = value

    def __getitem__(self, key):
        meta = self.coad.db['object'].find_one({'name':key, 'class_id':self.meta['class_id']})
        return ObjectDict(self, meta)

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
        obj_dicts = self.coad.db['object'].find({'class_id':self.meta['class_id']},{'name':1, '_id':0})
        return iter([c['name'] for c in obj_dicts])
        class ObjIterable:
            def __init__(self, cls):
                self.cls = cls
                self.all_objects = self.cls.coad.db['object'].find({'class_id':self.cls.meta['class_id']})
            def __iter__(self):
                return self
            def next(self):
                return ObjectDict(self.cls, self.all_objects.next())
        return ObjIterable(self)

    def __len__(self):
        return self.coad.db['object'].find({'class_id':self.cls.meta['class_id']}).count()

    def diff(self, other_class):
        ''' Return a list of difference between two ClassDict objects

        For each key in each ClassDict:
            Report differences in keys
            Report differences in ObjectDicts for each key
        '''
        # TODO: Update for mongo
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
        collection = self.coad.db['collection'].find_one({'parent_class_id':self.meta['class_id'], 'child_class_id':child_class_id}, {'collection_id':1})
        if collection is None:
            msg = 'Unable to find collection for the parent %s and child %s'
            raise Exception(msg%(self.meta['class_id'], child_class_id))
        return collection['collection_id']


class ObjectDict(collections.MutableMapping):
    ''' Overwrites the setitem method to allow updates to data and dict
        Works by using the list of attribute and attribute data dicts
        and manipulating the original database as needed

        meta is a dictionary describing the object as it is described in the
        database

        Uses Abstract Base Classes to extend a dictionary
    '''
    def __init__(self, clsdict, meta):
        self.store = dict()
        self.clsdict = clsdict
        self.meta = meta

    def __setitem__(self, key, value):
        if key not in self.clsdict.named_valid_attributes:
            msg = '%s is not a valid attribute of object %s, valid attributes:%s'
            raise Exception(msg%(key, self.meta['name'], self.clsdict.named_valid_attributes.keys()))
        att_id = self.clsdict.named_valid_attributes[key]
        self.clsdict.coad.db['attribute_data'].update({'object_id':self.meta['object_id'], 'attribute_id':att_id},
                                                      {'$set': {'value': value}}, upsert=True)

    def __getitem__(self, key):
        att_id = self.clsdict.named_valid_attributes[key]
        attdata = self.clsdict.coad.db['attribute_data'].find_one({'object_id':self.meta['object_id'], 'attribute_id':att_id})
        return attdata['value']

    def __delitem__(self, key):
        # TODO: Code for mongo
        if key not in self.clsdict.named_valid_attributes:
            msg = '%s is not a valid attribute of object %s, valid attributes:%s'
            raise Exception(msg%(key, self.meta['name'], self.clsdict.named_valid_attributes.keys()))
        att_id = self.clsdict.named_valid_attributes[key]
        self.clsdict.coad.db['attribute_data'].remove({'object_id':self.meta['object_id'], 'attribute_id':att_id}, True)
        return
        cur = self.coad.dbcon.cursor()
        cmd = "DELETE FROM attribute_data WHERE object_id=? AND attribute_id=?"
        vls = [self.meta['object_id'], self.valid_attributes[key]['attribute_id']]
        cur.execute(cmd, vls)
        self.coad.dbcon.commit()
        del self.store[key]

    def __iter__(self):
        att_ids = self.clsdict.coad.db['attribute_data'].distinct('attribute_id', {'object_id':self.meta['object_id']})
        return iter([self.clsdict.valid_attributes[aid] for aid in att_ids])
        #return iter(self.store)

    def __len__(self):
        return self.clsdict.coad.db['attribute_data'].distinct('attribute_id', {'object_id':self.meta['object_id']}).count()
        #return len(self.store)

    def __str__(self):
        attdata = self.clsdict.coad.db['attribute_data'].find({'object_id':self.meta['object_id']})
        att_dict = dict()
        for ad in attdata:
            att_dict[self.clsdict.valid_attributes[ad['attribute_id']]] = ad['value']
        return repr(att_dict)
        #return repr(self.store)

    def copy(self, newname=None):
        ''' Create a new object entry in the database, duplicate all the
            attribute_data entries as well.
            # TODO: Enforce unique naming
        '''
        # TODO: update for mongo
        obj_id_list = self.clsdict.coad.db['object'].find( {}, { '_id': 0, 'object_id':1 } )
        last_obj_id = max(map(int, [x['object_id'] for x in obj_id_list]))
        new_object_id = last_obj_id + 1;
        new_obj = self.clsdict.coad.db['object'].find({'object_id':self.meta['object_id']}, {'_id':0})[0]
        new_obj['object_id'] = str(new_object_id)
        if newname is None:
            new_obj['name'] = new_obj['name'] + '-' + str(uuid.uuid4())
        else:
            new_obj['name'] = newname
        self.clsdict.coad.db['object'].insert_one(new_obj)
        # Add obj to class list
        self.clsdict.store[new_obj['name']] = ObjectDict(self.clsdict, new_obj)
        # Copy attributes
        new_atts = []
        att_list = self.clsdict.coad.db['attribute_data'].find( {'object_id':self.meta['object_id']}, { '_id': 0 } )
        for att in att_list:
            att['object_id'] = str(new_object_id)
            new_atts.append(att)
        self.clsdict.coad.db['attribute_data'].insert_many(new_atts)
        # Get highest membership_id
        # TODO: Something bad may happen if object has no memberships
        mship_id_list = self.clsdict.coad.db['membership'].find( {}, { '_id': 0, 'membership_id':1 } )
        last_mship_id = max(map(int, [x['membership_id'] for x in mship_id_list]))
        new_mship_id = last_mship_id + 1;
        # Copy memberships where this is the parent
        new_mships = []
        mships = self.clsdict.coad.db['membership'].find({'parent_object_id': self.meta['object_id']}, { '_id': 0 })
        for mship in mships:
            mship['parent_object_id'] = str(new_object_id)
            mship['membership_id'] = str(new_mship_id)
            new_mship_id += 1
            new_mships.append(mship)
        # Copy data from parent memberships
        # Copy memberships where this is the child
        # Copy data from child memberships
        if len(new_mships) > 0:
            self.clsdict.coad.db['membership'].insert_many(new_mships)
        return self.clsdict.store[new_obj['name']]

    def set_children(self, children, replace=True):
        ''' Set the children of this object.    If replace is true, it will
        remove any existing children matching the classes passed in otherwise it
        will append the data.
        Can handle either a single ObjectDict or list of ObjectDicts
        TODO: Validate that object is allowed to have the children passed in
        '''
        # TODO: update for mongo
        # Convert objdict to list of objdict
        if isinstance(children, ObjectDict):
            children = [children]
        # Get last membership id
        mship_id_list = self.clsdict.coad.db['membership'].find( {}, { '_id': 0, 'membership_id':1 } )
        last_mship_id = max(map(int, [x['membership_id'] for x in mship_id_list]))
        new_mship_id = last_mship_id + 1;
        # Add all memberships
        new_mships = []
        for child in children:
            # Remove all memberships that match child_class_id and parent_object_id
            child_class_id = child.clsdict.meta['class_id']
            if replace:
                self.clsdict.coad.db['membership'].remove({'child_class_id':child_class_id,
                                                           'parent_object_id':self.meta['object_id']})
            new_mship = {'membership_id':str(new_mship_id),
                         'child_class_id':child_class_id,
                         'child_object_id':child.meta['object_id'],
                         'parent_class_id':self.clsdict.meta['class_id'],
                         'parent_object_id':self.meta['object_id'],
                         'collection_id':self.clsdict.get_collection_id(child_class_id)}
            new_mships.append(new_mship)
            new_mship_id += 1
        if len(new_mships) > 0:
            self.clsdict.coad.db['membership'].insert_many(new_mships)
        return
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
            collection_id = self.clsdict.get_collection_id(class_id)
            for obj in objectdicts:
                cmd = '''INSERT INTO membership (parent_class_id, parent_object_id,
                         collection_id, child_class_id, child_object_id)
                         VALUES (?,?,?,?,?)'''
                vls = [self.meta['class_id'], self.meta['object_id'], collection_id,
                       class_id, obj.meta['object_id']]
                cur.execute(cmd, vls)
        self.coad.dbcon.commit()

    def get_children(self, class_name=None):
        ''' Return a list of all children that match the class name.  If class
        name is None, return all children
        '''
        children = []
        memberships = self.clsdict.coad.db['membership'].find({'parent_object_id':self.meta['object_id']})
        for member in memberships:
            m_class = self.clsdict.coad.db['class'].find_one({'class_id':member['child_class_id']},{'name':1})
            if class_name is None or m_class['name'] == class_name:
                m_obj = self.clsdict.coad.db['object'].find_one({'object_id':member['child_object_id']},{'name':1})
                children.append(self.clsdict.coad[m_class['name']][m_obj['name']])
        return children

    def get_class(self):
        ''' Return the ClassDict that contains this object
        '''
        return self.clsdict

    def get_properties(self):
        '''Return a dict of all properties set for this object with respect to the System parent.
        '''
        props = {}
        memberships = self.clsdict.coad.db['membership'].find({'child_object_id':self.meta['object_id']}, {'membership_id':1})
        for member in memberships:
            data = self.clsdict.coad.db['data'].find({'membership_id':member['membership_id']}).sort('uid', 1)
            for d in data:
                name = self.clsdict.coad.valid_properties[d['property_id']]
                value = d['value']
                if name not in props:
                    props[name] = value
                else:
                    if not isinstance(props[name], list):
                        props[name] = [props[name], value]
                    else:
                        props[name].append(value)
        return props

    def get_property(self, name):
        '''Return the value of a property by name
        '''
        props = self.get_properties()
        if name not in props:
            raise Exception('Object has no property "%s" set'%name)
        return props[name]

    def set_property(self, name, value):
        '''Set the value of a property by name
        '''
        raise Exception('Opertation not supported yet')

        # If list do something else
        if isinstance(value, list):
            raise Exception('Opertation not supported yet')
        # Find data id(s), has to be done by membership + child_object_id
        memberships = self.clsdict.coad.db['membership'].find({'child_object_id':self.meta['object_id']}, {'membership_id':1})
        for member in memberships:
            data = self.clsdict.coad.db['data'].find({'membership_id':member['membership_id']}).sort('uid', 1)
            for d in data:
                if name == self.clsdict.coad.valid_properties[d['property_id']]:
                    # Update here
                    # TODO: Multiple values
                    pass
        prop_id = None
        for (pi, pn) in self.clsdict.coad.valid_properties.iteritems():
            if pn == name:
                prop_id = pi
                break
        if prop_id is None:
            raise Exception('Object has no valid property "%s"'%name)
        # If list do something else
        if isinstance(value, list):
            raise Exception('Opertation not supported yet')
        # Update value
        self.clsdict.coad.db['data'].update()
        #TODO: Handle arrays of values
        cur = self.coad.dbcon.cursor()
        cur.execute("""SELECT d.data_id FROM data d
            INNER JOIN property p ON p.property_id = d.property_id
            WHERE p.name=?
            AND membership_id IN
            (SELECT membership_id FROM membership WHERE child_object_id=?)""",
                    [name, self.meta['object_id']])
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
        kids = self.get_children()
        if len(kids):
            print(spacing+'    Children (%s):'%len(kids))
            for k in kids:
                k.dump(recursion_level+1)
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
                diff_msg.append("  Different Property Value for %s, object_id %s"%(key, self.meta['object_id']))
                diff_msg.append("    Orig: %s"%self_props[key])
                diff_msg.append("    Comp: %s"%other_props[key])
        # Children diff
        #other_kids = other_obj.get_children()
        #self_kids = self.get_children()
        # TODO: How to describe differences in children, name, id?
        return diff_msg
