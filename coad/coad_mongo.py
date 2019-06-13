"""Class-Object-Attribute Dictionary Tools Using MongoDB

This module contains tools for manipulating Plexos data files outside of the
Plexos UI.  It loads the xml file into a mongo database for further inspection
and modification.  Mongdb is started on first class instantiation and stopped on
python exit.

Example:
    from coad_mongo import COAD
    coad = COAD("master.xml")
    print("Before set, solver is %s"%coad['Performance']['Gurobi']['SOLVER'])
    coad['Performance']['Gurobi']['SOLVER'] = 3
    coad.save("master_new.xml")
    coad_new = COAD("master_new.xml")
    print("After set, solver is %s"%coad['Performance']['Gurobi']['SOLVER'])
"""
import atexit
import collections
import logging
import os
import pymongo
import subprocess
import sys
import uuid

from . import plexos_mongo

MONGODB_PROC = None
_logger = logging.getLogger(__name__)

class COAD(collections.MutableMapping):
    '''Edit models, horizons, memberships and object attributes of plexos data.
    Quickly modify the largest xml files for simulation.

    Instantiation will import xml data into a mongo database or open an
    existing mongo database of plexos data

    The class presents a map of class names to ClassDict objects
    '''

    def __init__(self, filename=None, reload=True, host='localhost', port=27017,
                 start_mongodb=True):
        '''Initialize the COAD object, populating Classes, Objects and Attributes

        Args:
            filename - Name of plexos input file to use, must end in xml.  If
                not provided, the master.xml is used
            reload - Reload the mongo database from file, defaults to True
            host - Hostname of MongoDB
            port - Port of MongoDB
            start_mongodb - Attempt to start mongodb, ignoring host and port options
        '''
        if filename is None:
            filename = os.path.join(os.path.abspath(os.path.dirname(__file__)), "master.xml")
        # Attempt to start mongodb if not already started
        if start_mongodb and sys.modules[__name__].MONGODB_PROC is None:
            #sys.modules[__name__].MONGODB_PROC = subprocess.Popen('mongod', stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            try:
                os.mkdir('mongo_data')
            except OSError:
                pass
            with open('mongod_log.txt', 'w') as mlog:
                sys.modules[__name__].MONGODB_PROC = subprocess.Popen(['mongod', '--dbpath', 'mongo_data'], stdout=mlog, stderr=mlog)
            host = 'localhost'
            port = 27017
            atexit.register(sys.modules[__name__].MONGODB_PROC.terminate)
        # Check for database in mongo.
        dbname = os.path.basename(filename).replace(".", "").replace("$", "")
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
        # TODO: This is incorrect for properties that have duplicate names, move into class/collection
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

            TODO: Don't overwrite the MutableMapping set
            '''
        try:
            (class_name, object_name, attribute_name) = identifier.split('.')
        except:
            raise Exception('''Invalid identifier, must take the form of:
                class name.object name.attribute name''')
        self[class_name][object_name][attribute_name] = value

    def get_hierarchy_for_object_id(self, object_id):
        ''' Return a hierarchy based on object_id.  Added to remove instantiation
        of various objects during property lookups
        '''
        objmeta = self.db['object'].find_one({'object_id':object_id})
        clsmeta = self.db['class'].find_one({'class_id':objmeta['class_id']})
        return "%s.%s"%(clsmeta['name'], objmeta['name'])

    def get_by_object_id(self, object_id):
        ''' Return an ObjectDict based on object_id
        '''
        objmeta = self.db['object'].find_one({'object_id':object_id})
        clsmeta = self.db['class'].find_one({'class_id':objmeta['class_id']})
        objcls = ClassDict(self, clsmeta)
        return ObjectDict(objcls, objmeta)

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
        #self.store = dict()
        self.coad = coad
        self.meta = meta
        # TODO: Add more info to valid attributes
        attributes = self.coad.db['attribute'].find({'class_id':self.meta['class_id']})
        self.valid_attributes = dict()
        for att in attributes:
            self.valid_attributes[att['attribute_id']] = att['name']
        # TODO: Duplicate names ever happen?
        self.named_valid_attributes = {v: k for k, v in self.valid_attributes.items()}
        #objects = self.coad.db['object'].find({'class_id':self.meta['class_id']})
        #for objdoc in objects:
        #    if objdoc['name'] in self.store:
        #        msg = 'Duplicate name of object %s in class %s'
        #        raise Exception(msg%(objdoc['name'], self.meta['name']))
        #    self.store[objdoc['name']] = ObjectDict(self, objdoc)
        # Collections have the property id allowed for class
        self.valid_properties = dict()
        collections = self.coad.db['collection'].find({'child_class_id':self.meta['class_id']})
        for coll in collections:
            parent = coll['parent_class_id']
            parent_meta = self.coad.db['class'].find_one({'class_id':parent})
            props = self.coad.db['property'].find({'collection_id': coll['collection_id']})
            for prop in props:
                if parent_meta['name'] not in self.valid_properties:
                    self.valid_properties[parent_meta['name']] = {}
                if prop['property_id'] in self.valid_properties:
                    raise Exception("Duplicate property %s in class %s"%(prop['name'], self.meta['name']))
                self.valid_properties[parent_meta['name']][prop['property_id']] = prop
        #all_properties = self.db['property'].find({}, {'name':1, 'property_id':1, '_id':0})
        #self.valid_properties = dict()
        #for p in all_properties:
        #    self.valid_properties[p['property_id']] = p['name']
        self.valid_properties_by_name = {}
        for p, pv in self.valid_properties.items():
            self.valid_properties_by_name[p] = {}
            for k, v in pv.items():
                if v['name'] in  self.valid_properties_by_name:
                    raise Exception("Duplicate property %s in class %s"%(v['name'], self.meta['name']))
                self.valid_properties_by_name[p][v['name']] = k

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
        class ObjIterable(object):
            def __init__(self, cls):
                self.cls = cls
                self.all_objects = self.cls.coad.db['object'].find({'class_id':self.cls.meta['class_id']})
            def __iter__(self):
                return self
            def next(self):
                return ObjectDict(self.cls, self.all_objects.next())
        return ObjIterable(self)

    def __len__(self):
        return self.coad.db['object'].find({'class_id': self.meta['class_id']}).count()

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

    def get_category_id(self, category):
        ''' Return the category id for objects of this class based on category name
        '''
        categories_cur = self.coad.db['category'].find_one({'class_id':self.meta['class_id'], 'name':category})
        return categories_cur['category_id']

    def get_categories(self):
        ''' Return a list of category dicts available for objects of this class, ordered
        by rank.
        '''
        categories_cur = self.coad.db['category'].find({'class_id':self.meta['class_id']})
        # Everything is a string in mongo, so need to sort by function
        categories = sorted(categories_cur, key=lambda c: int(c['rank']))
        return categories

    def add_category(self, name):
        ''' Add a new category to this class, not allowing duplicated names in class
        '''
        # TODO: Implement counters for all important _id attributes
        cat_id_list = self.coad.db['category'].find( {}, { '_id': 0, 'category_id':1 } )
        last_cat_id = max(map(int, [x['category_id'] for x in cat_id_list]))
        categories_cur = self.coad.db['category'].find({'class_id':self.meta['class_id']})
        lastrank = 0
        for cat in categories_cur:
            if name == cat['name']:
                raise Exception("Category %s already exists in %s"%(name, self.meta['name']))
            lastrank = max(lastrank, int(cat['rank']))
        newcat = {'name':name, 'rank':str(lastrank+1), 'category_id':str(last_cat_id+1), 'class_id':self.meta['class_id']}
        self.coad.db['category'].insert(newcat)
        return self.get_category_id(name)
    # TODO: Any need for remove category?  Would have to change objects that use
    # the deleted category to the default

    def new(self, name, category="-"):
        ''' Create a new object entry in the database.
        '''
        # Verify there is no existing object of this class with this name
        if name in list(self.keys()):
            raise Exception("Duplicate name '%s' for same class"%name)
        # Get category id or create a new one
        try:
            catid = self.get_category_id(category)
        except:
            catid = self.add_category(category)
        # Pull an object to find the attributes needed to fill
        sample_obj = self.coad.db['object'].find_one({}, {'_id':0})
        # Create new object_id
        obj_id_list = self.coad.db['object'].find( {}, { '_id': 0, 'object_id':1 } )
        last_obj_id = max(map(int, [x['object_id'] for x in obj_id_list]))
        new_object_id = last_obj_id + 1;
        new_obj = {}
        for k in sample_obj.keys():
            # Create new GUID if needed
            # GUID has been put in some versions of Plexos
            if 'GUID' == k:
                new_obj[k] = str(uuid.uuid4())
            elif 'object_id' == k:
                new_obj[k] = str(new_object_id)
            elif 'class_id' == k:
                new_obj[k] = self.meta['class_id']
            elif 'category_id' == k:
                new_obj[k] = catid
            elif 'name' == k:
                if name is None:
                    name = "New %s %s"%(self.meta['name'], str(uuid.uuid4()))
                new_obj[k] = name
            else:
                new_obj[k] = ""
        self.coad.db['object'].insert_one(new_obj)
        newobj = self[name]
        self.coad["System"]["System"].set_children(newobj, replace=False)
        return newobj


class ObjectDict(collections.MutableMapping):
    ''' Overwrites the setitem method to allow updates to data and dict
        Works by using the list of attribute and attribute data dicts
        and manipulating the original database as needed

        meta is a dictionary describing the object as it is described in the
        database

        Uses Abstract Base Classes to extend a dictionary
    '''
    def __init__(self, clsdict, meta):
        #self.store = dict()
        self.clsdict = clsdict
        self.meta = meta
        self.hierarchy = '%s.%s'%(self.clsdict.meta['name'], self.meta['name'])

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
        obj_id_list = self.clsdict.coad.db['object'].find( {}, { '_id': 0, 'object_id':1 } )
        last_obj_id = max(map(int, [x['object_id'] for x in obj_id_list]))
        new_object_id = last_obj_id + 1;
        new_obj = self.clsdict.coad.db['object'].find_one({'object_id':self.meta['object_id']}, {'_id':0})
        new_obj['object_id'] = str(new_object_id)
        if newname is None:
            new_obj['name'] = new_obj['name'] + '-' + str(uuid.uuid4())
        else:
            new_obj['name'] = newname
        # Verify there is no existing object of this class with this name
        exist_obj = self.clsdict.coad.db['object'].find_one({'class_id':self.meta['class_id'], 'name':new_obj['name']}, {'_id':0})
        if exist_obj:
            raise Exception("Duplicate name '%s' for same class"%new_obj['name'])
        # GUID has been put in some versions of Plexos
        if 'GUID' in self.meta:
            new_obj['GUID'] = str(uuid.uuid4())
        self.clsdict.coad.db['object'].insert_one(new_obj)
        # Copy attributes
        new_atts = []
        att_list = self.clsdict.coad.db['attribute_data'].find( {'object_id':self.meta['object_id']}, { '_id': 0 } )
        for att in att_list:
            att['object_id'] = str(new_object_id)
            new_atts.append(att)
        if len(new_atts) > 0:
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
        # Copy memberships where this is the child
        mships = self.clsdict.coad.db['membership'].find({'child_object_id': self.meta['object_id']}, { '_id': 0 })
        for mship in mships:
            mship['child_object_id'] = str(new_object_id)
            mship['membership_id'] = str(new_mship_id)
            new_mship_id += 1
            new_mships.append(mship)
        # TODO: Copy data from child memberships
        #
        if len(new_mships) > 0:
            self.clsdict.coad.db['membership'].insert_many(new_mships)
        return self.clsdict[new_obj['name']]

    def set_children(self, children, replace=True):
        ''' Set the children of this object.    If replace is true, it will
        remove any existing children matching the classes passed in otherwise it
        will append the data.
        Can handle either a single ObjectDict or list of ObjectDicts
        TODO: Validate that object is allowed to have the children passed in
        '''
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

    def get_parents(self, class_name=None):
        ''' Return a list of all parents that match the class name.  If class
        name is None, return all parents
        '''
        parents = []
        memberships = self.clsdict.coad.db['membership'].find({'child_object_id':self.meta['object_id']})
        for member in memberships:
            m_class = self.clsdict.coad.db['class'].find_one({'class_id':member['parent_class_id']},{'name':1})
            if class_name is None or m_class['name'] == class_name:
                m_obj = self.clsdict.coad.db['object'].find_one({'object_id':member['parent_object_id']},{'name':1})
                parents.append(self.clsdict.coad[m_class['name']][m_obj['name']])
        return parents

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

    def get_category(self):
        ''' Return the name of this object's category
        '''
        category = self.clsdict.coad.db['category'].find_one({'category_id':self.meta['category_id']})
        return category['name']

    def set_category(self, name):
        ''' Set this object's category to name
        '''
        available_cats = self.clsdict.get_categories()
        for cat in available_cats:
            if cat['name'] == name:
                self.clsdict.coad.db['object'].update({"object_id":self.meta['object_id']}, {'$set': {'category_id': cat['category_id']}})
                return
        raise Exception("No such category %s for class %s"%(name, self.clsdict.meta['name']))

    def get_class(self):
        ''' Return the ClassDict that contains this object
        '''
        return self.clsdict

    def get_properties(self):
        '''Return a dict of all properties set for this object along with any
        properties tagged to another object.

        Tagged properties apply only to tag object

        Returns:
            dict of class/object_hierarchy=dict of property_name=value
        '''
        props = {}
        memberships = self.clsdict.coad.db['membership'].find({'child_object_id':self.meta['object_id']})
        for member in memberships:
            parent = self.clsdict.coad.get_by_object_id(member['parent_object_id'])
            # TODO: sort by band
            #data = self.clsdict.coad.db['data'].find({'membership_id':member['membership_id']}).sort('uid', 1)
            data = self.clsdict.coad.db['data'].find({'membership_id':member['membership_id']}).sort('uid', 1)
            for d in data:
                # Can parents and tags coexist? Yes!  It appears the data_id
                # shown in the tag is the overwritten value of the default.
                # Check for tag, which is modified data for a specific data_id
                tag = self.clsdict.coad.db['tag'].find_one({'data_id':d['data_id']})
                if tag is not None:
                    # TODO: Ever multiple tags for the same data_id?
                    tag_obj = self.clsdict.coad.get_by_object_id(tag['object_id'])
                    #raise Exception("Found a tag! parent %s, tag %s"%(parent.hierarchy, tag_obj.hierarchy))
                    tag_hier = tag_obj.hierarchy
                else:
                    tag_hier = parent.hierarchy
                name = self.clsdict.valid_properties[parent.clsdict.meta['name']][d['property_id']]['name']
                # Test for input mask, substituting if needed
                prop = self.clsdict.coad.db['property'].find_one({'property_id':d['property_id']})
                band = self.clsdict.coad.db['band'].find_one({'data_id':d['data_id']})
                valdict = {}
                if 'input_mask' in prop:
                    mask = prop['input_mask'].split(";")
                    it = iter(mask)
                    for k in it:
                        valdict[str(k)] = next(it).strip("\"")
                if d['value'] in valdict:
                    value = valdict[d['value']]
                else:
                    value = d['value']
                if tag_hier not in props:
                    props[tag_hier] = {}
                max_band_id = int(prop['max_band_id'])
                #if name not in props[tag_hier]:
                if max_band_id == 1:
                    props[tag_hier][name] = value
                else:
                    if name not in props[tag_hier]:
                        props[tag_hier][name] = [None] * max_band_id
                    if band:
                        props[tag_hier][name][int(band['band_id']) - 1] = value
                    else:
                        props[tag_hier][name][0] = value
                    #if not isinstance(props[tag_hier][name], list):
                    #    props[tag_hier][name] = [props[tag_hier][name], value]
                    #else:
                    #    props[tag_hier][name].append(value)
        return props

    def get_property(self, name, tag='System.System'):
        '''Return the value of a property by name

        Args:
            tag - ObjectDict or hierarchy string of data tag

        Returns: string for single value, list for multiple values
        '''
        if isinstance(tag, ObjectDict):
            tag_obj = tag
        else:
            tag_obj = self.clsdict.coad.get_by_hierarchy(tag)
        # Tag object should always be ObjectDict
        tag_obj_id = tag_obj.meta['object_id']
        member = self.clsdict.coad.db['membership'].find_one({'child_object_id':self.meta['object_id'], 'parent_object_id':tag_obj_id}, {'membership_id':1, 'collection_id':1})
        if member is None:
            all_members = self.clsdict.coad.db['membership'].find({'child_object_id':self.meta['object_id']}, {'membership_id':1, 'collection_id':1})
            for member in all_members:
                prop = self.clsdict.coad.db['property'].find_one({'collection_id':member['collection_id'], 'name':name})
                if prop is not None:
                    break
            if prop is None:
                raise Exception("Unable to find membership for %s in %s"%(tag_obj.hierarchy, self.hierarchy))
        else:
            prop = self.clsdict.coad.db['property'].find_one({'collection_id':member['collection_id'], 'name':name})
        prop_id = prop['property_id']
        # Test for input mask, substituting if needed
        valdict = {}
        if 'input_mask' in prop:
            mask = prop['input_mask'].split(";")
            it = iter(mask)
            for k in it:
                valdict[str(k)] = next(it).strip("\"")
        def valmap(val):
            if val in valdict:
                return valdict[val]
            else:
                return val
        # Only use data that is tagged
        if tag_obj_id == '1':
            # TODO: This won't work, need to reject all data that isn't tagged at all
            all_data = self.clsdict.coad.db['data'].find({'membership_id':member['membership_id'], 'property_id':prop_id}).sort('uid', 1)
        else:
            tagged_data = self.clsdict.coad.db['tag'].find({'object_id':tag_obj_id},{'data_id':1})
            tagged_data_ids = [x['data_id'] for x in tagged_data]
            all_data = self.clsdict.coad.db['data'].find({'membership_id':member['membership_id'], 'property_id':prop_id, 'data_id': {'$in':tagged_data_ids}}).sort('uid', 1)
        data_count = all_data.count()
        mapped_data = []
        all_data_ids = []
        for d in all_data:
            mapped_data.append(valmap(d['value']))
            all_data_ids.append(d['data_id'])
        if data_count == 0:
            raise Exception("No exisiting data found for membership %s"%member['membership_id'])
        elif data_count == 1:
            return mapped_data[0]
        else:
            # Must order data based on band_id
            bands = self.clsdict.coad.db['band'].find({'data_id':{'$in':all_data_ids}}).sort('band_id', 1)
            all_banded_data = [x['data_id'] for x in bands]
            _logger.info("Data_ids %s Bands %s", all_data_ids, all_banded_data)
            missing_band = set(all_data_ids) - set(all_banded_data)
            ordered_data = [mapped_data[all_data_ids.index(missing_band.pop())]]
            for band_did in all_banded_data:
                ordered_data.append(mapped_data[all_data_ids.index(band_did)])
            return ordered_data

    def set_property(self, name, value, tag='System.System', data_tag=None):
        '''Set the value of a property by name.  Inserts new data if needed.
        If data_tag is set, create additional tag for datafile.
        '''
        tag_obj = self.clsdict.coad.get_by_hierarchy(tag)
        tag_clsname = tag_obj.clsdict.meta['name']
        # Commonly used method for converting human value to stored value
        def get_mask_value(prop, value):
            '''Using the property input_mask attribute, map value to a valid
            value and return it'''
            valdict = {}
            if 'input_mask' in prop:
                vv = []
                mask = prop['input_mask'].split(";")
                it = iter(mask)
                for k in it:
                    mval = next(it).strip("\"")
                    if mval == value:
                        return k
                    vv.append(mval)
                raise Exception("Value '%s' not in property's input_mask.  Valid values are:\n%s\n"%(value,'\n'.join(vv)))
            else:
                return value
        # If the tagged class doesn't have the property as valid, it's set as a
        # tag
        if tag_clsname not in self.clsdict.valid_properties_by_name:
            # Modify if value is already set
            possible_tags = self.clsdict.coad.db['tag'].find({'object_id':tag_obj.meta['object_id']})
            for ptag in possible_tags:
                # Get property name, see if it matches name
                ptag_data = self.clsdict.coad.db['data'].find_one({'data_id':ptag['data_id']})
                ptag_prop = self.clsdict.coad.db['property'].find_one({'property_id':ptag_data['property_id']})
                if ptag_prop['name'] == name:
                    # If it does, see if the membership matches this object
                    ptag_member = self.clsdict.coad.db['membership'].find_one({'membership_id':ptag_data['membership_id']})
                    # If it matches, set the value
                    if ptag_member['child_object_id'] == self.meta['object_id']:
                        if isinstance(value, list):
                            raise Exception("Overwriting list of tagged data is not supported yet")
                        # Get the masked value before is_dynamic is updated
                        m_value = get_mask_value(ptag_prop, value)
                        # Make sure property has dynamic set to true
                        if ptag_prop['is_dynamic'] != 'true' or ptag_prop['is_enabled'] != 'true':
                            self.clsdict.coad.db['property'].update(ptag_prop, {'$set': {'is_dynamic': 'true', 'is_enabled': 'true'}})
                        self.clsdict.coad.db['data'].update(ptag_data, {'$set': {'value': m_value}})
                        return
            # Add new tag and data here
            prop_id = self.clsdict.valid_properties_by_name['System'][name]
            prop = self.clsdict.coad.db['property'].find_one({'property_id':prop_id})
            # Get the masked value before is_dynamic is updated
            if not isinstance(value, list):
                value = [value]
            m_values = [get_mask_value(prop, x) for x in value]
            #m_value = get_mask_value(prop, value)
            # data tag involved?
            if data_tag is not None:
                data_obj = self.coad.get_by_hierarchy(data_tag)
            # Make sure is_dynamic is set to true
            if prop['is_dynamic'] != 'true' or prop['is_enabled'] != 'true':
                self.clsdict.coad.db['property'].update(prop, {'$set': {'is_dynamic': 'true', 'is_enabled': 'true'}})
            # Add new data
            #last_data_id = self.clsdict.coad.db['data'].find_one(sort=[("data_id", -1)])["data_id"]
            last_data_id = self.clsdict.coad.db['data'].aggregate([{'$project':{'int_data_id': {'$toInt': '$data_id'}}}, {'$sort':{"int_data_id": -1}}]).next()["int_data_id"]
            #last_uid = self.clsdict.coad.db['data'].find_one(sort=[({'int_uid': {'$toInt': '$uid'}}, -1)])["int_uid"]
            #last_uid = self.clsdict.coad.db['data'].find_one(sort=[({'$project':{'int_uid': {'$toInt': '$uid'}}}, -1)])["int_uid"]
            last_uid = self.clsdict.coad.db['data'].aggregate([{'$project':{'int_uid': {'$toInt': '$uid'}}}, {'$sort':{"int_uid": -1}}]).next()["int_uid"]
            #last_uid = last_data_id
            _logger.info("Max data id=%s max uid=%s", last_data_id, last_uid)
            #agg_result = self.clsdict.coad.db['data'].aggregate([{'$project':{'last_data_id': {'$max': '$data_id'},
            #                                        'last_uid': {'$max': {'$toInt': '$uid'}}}}])
            #_logger.info(agg_result)
            #_logger.info(list(agg_result))
            #_logger.info("Max data id=%s max uid=%s", agg_result['last_data_id'], agg_result['last_uid'])
            #data_id_list = list(self.clsdict.coad.db['data'].find( {}, { '_id': 0, 'data_id': 1, 'uid': 1 } ))
            #last_data_id = max(map(int, [x['data_id'] for x in data_id_list]))
            #last_uid = max(map(int, [x['uid'] for x in data_id_list]))
            #_logger.info("Max data id=%s max uid=%s", last_data_id, last_uid)
            sys_obj = self.clsdict.coad.get_by_hierarchy('System.System')
            member = self.clsdict.coad.db['membership'].find_one({'child_object_id':self.meta['object_id'], 'parent_object_id':sys_obj.meta['object_id']}, {'membership_id':1})
            band = 0
            for m_value in m_values:
                last_data_id += 1
                last_uid += 1
                band += 1
                self.clsdict.coad.db['data'].insert({'data_id':str(last_data_id),
                                         'uid':str(last_uid),
                                         'membership_id':member['membership_id'],
                                         'value':m_value,
                                         'property_id':prop_id})
                # Add new band
                if band > 1:
                    self.clsdict.coad.db['band'].insert({'data_id':str(last_data_id),
                        'band_id':str(band)})
                # Add new tag
                self.clsdict.coad.db['tag'].insert({'data_id':str(last_data_id),
                                        'object_id':tag_obj.meta['object_id']})
                if data_tag is not None:
                    self.clsdict.coad.db['tag'].insert({'data_id':str(last_data_id),
                                            'object_id':data_obj.meta['object_id']})
        else:
            # Reverse lookup of class.valid_properties to get property_id
            if name not in self.clsdict.valid_properties_by_name[tag_clsname]:
                raise Exception('"%s" is not a valid property for class %s'%(name, tag_clsname))
            prop_id = self.clsdict.valid_properties_by_name[tag_clsname][name]
            prop = self.clsdict.coad.db['property'].find_one({'property_id':prop_id})
            # Tag object should always be ObjectDict
            tag_obj_id = tag_obj.meta['object_id']
            member = self.clsdict.coad.db['membership'].find_one({'child_object_id':self.meta['object_id'], 'parent_object_id':tag_obj_id}, {'membership_id':1})
            if member is None:
                raise Exception("Unable to find membership for %s in %s"%(tag, self.meta['name']))
            all_data = self.clsdict.coad.db['data'].find({'membership_id':member['membership_id'], 'property_id':prop_id}).sort('uid', 1)
            data_count = all_data.count()
            if data_count == 0:
                raise Exception("No exisiting data found for membership %s"%member['membership_id'])
            elif data_count == 1:
                # Can replace this data
                data = all_data.next()
                self.clsdict.coad.db['data'].update(data, {'$set': {'value': get_mask_value(prop, value)}})
            else:
                raise Exception('Overwriting list of data not supported yet')

    def set_properties(self, new_dict):
        '''NOT IMPLEMENTED WITH NEW PROPERTY INFO
        Set all the propery values present in dict

            NOTE: This is not transactional.  A failure may leave some values set,
            others not set.
        '''
        raise Exception('Operation not implemented')
        for name, value in new_dict.items():
            self.set_property(name, value)

    def tag_property(self, name, tag):
        '''Tag a property with a object.  System.System throws an exception.
            Use untag_property to fill back system properties.
        '''
        tag_obj = self.clsdict.coad.get_by_hierarchy(tag)
        if tag_obj.meta['object_id'] == '1':
            raise Exception("Cannot tag with System object")
        # Need to find data_ids where they don't have tag_object_id matching tag_obj
        #members = self.clsdict.coad.db['property_view'].find({'child_object_id':self.meta['object_id']})
        membership = self.clsdict.coad.db['membership'].find_one({'child_object_id':self.meta['object_id']}, {'collection_id':1, 'membership_id':1})
        if membership is None:
            raise Exception("Unable to find membership for %s"%self.hierarchy)
        prop = self.clsdict.coad.db['property'].find_one({'name':name, 'collection_id':membership['collection_id']})
        # Data should exist for this property, if not raise an error
        data_ids = self.clsdict.coad.db['data'].find({'property_id':prop["property_id"], 'membership_id': membership['membership_id']}, {'_id':0, 'data_id':1})
        if data_ids.count() == 0:
            raise Exception("No data matching '%s' for this object", name)
        for data_id in data_ids:
            # Make sure tag for this data_id and tag object_id does not already exist
            tagtest = self.clsdict.coad.db['tag'].find_one({"data_id":data_id['data_id'], "object_id":tag_obj.meta['object_id']})
            if tagtest is not None:
                raise Exception("Duplicatd tag for data_id %s and object_id %s", data_id['data_id'], tag_obj.meta['object_id'])
            self.clsdict.coad.db['tag'].insert({"data_id":data_id['data_id'], "object_id":tag_obj.meta['object_id']})

    def untag_property(self, name, tag="System.System"):
        '''Remove tag for a given property and tag.  TBD if tag is System.System
            Need some way to remove all tags?
        '''
        tag_obj = self.clsdict.coad.get_by_hierarchy(tag)
        if tag_obj.meta['object_id'] == '1':
            raise Exception("Cannot untag System object")
        membership = self.clsdict.coad.db['membership'].find_one({'child_object_id':self.meta['object_id']}, {'collection_id':1, 'membership_id':1})
        if membership is None:
            raise Exception("Unable to find membership for %s"%self.hierarchy)
        prop = self.clsdict.coad.db['property'].find_one({'name':name, 'collection_id':membership['collection_id']})
        # Data should exist for this property, if not raise an error
        data_ids = self.clsdict.coad.db['data'].find({'property_id':prop["property_id"], 'membership_id': membership['membership_id']}, {'_id':0, 'data_id':1})
        if data_ids.count() == 0:
            _logger.warning("No properties untagged")
        else:
            for data_id_obj in data_ids:
                data_id = data_id_obj['data_id']
                _logger.info("Deleting data_id:%s object_id:%s from tag"%(data_id, tag_obj.meta['object_id']))
                self.clsdict.coad.db['tag'].remove({"data_id":data_id, "object_id":tag_obj.meta['object_id']}, True)

        return

    def get_text(self):
        '''Return a dict of all text set for this object along with any
        text tagged to another object.

        Returns:
            dict of class/object_hierarchy=dict of text property name=value
        '''
        #cur = self.coad.dbcon.cursor()
        text = {}
        # Sometimes there is no data or text table
        if 'data' not in self.clsdict.coad.db.collection_names() or 'text' not in self.clsdict.coad.db.collection_names():
            return text
        members = self.clsdict.coad.db['membership'].find({'child_object_id':self.meta['object_id']})
        for mem in members:
            #print("Member is %s"% mem)
            parent = self.clsdict.coad.get_by_object_id(mem['parent_object_id'])
            all_data = self.clsdict.coad.db['data'].find({'membership_id':mem['membership_id']})
            for dat in all_data:
                all_text = self.clsdict.coad.db['text'].find({'data_id':dat['data_id']})
                for txt in all_text:
                    property_id = dat['property_id']
                    name = self.clsdict.valid_properties[parent.get_class().meta['name']][str(property_id)]['name']
                    tag_set = False
                    if 'tag' in self.clsdict.coad.db.collection_names():
                        all_tags = self.clsdict.coad.db['tag'].find({'data_id':dat['data_id']})
                        for tag in all_tags:
                            tag_obj_hier = self.clsdict.coad.get_hierarchy_for_object_id(tag['object_id'])
                            if tag_obj_hier not in text:
                                text[tag_obj_hier] = {}
                            text[tag_obj_hier][name] = txt['value']
                            tag_set = True
                    if not tag_set:
                        #print("p:%s n:%s v:%s"%(parent.hierarchy, name, value))
                        if parent.hierarchy not in text:
                            text[parent.hierarchy] = {}
                        text[parent.hierarchy][name] = txt['value']
        return text

    def set_text(self, name, value, tag='System.System', class_id='Data File'):
        '''Set the value of a text item by name
            Will add new data if no existing text matches the tag.
            Will NOT add new membership if one doesn't exist.
            Assumes System.System requires a property set with the default value.
            Assumes it will use the "Data File" class for its class_id

            Allows setting filenames for certain properties such as Data File
        '''
        matches = []
        properties = self.clsdict.coad.db['property'].find({'name':name})
        members = self.clsdict.coad.db['membership'].find({'child_object_id':self.meta['object_id']})
        for prop in properties:
            for mem in members:
                if prop['collection_id'] == mem['collection_id']:
                    matches.append((mem['parent_object_id'], mem['membership_id'], prop['property_id']))
                    break
        for (parent_object_id, membership_id, property_id) in matches:
            parent_obj = self.clsdict.coad.get_by_object_id(parent_object_id)
            # Check if there is already a data for this property
            existing_data = self.clsdict.coad.db['data'].find_one({'membership_id':membership_id, 'property_id':property_id})
            if existing_data:
                data_id = existing_data['data_id']
            else:
                default_value = self.clsdict.valid_properties[parent_obj.meta['name']][str(property_id)]['default_value']
                # Add new data
                data_id_list = list(self.clsdict.coad.db['data'].find( {}, { '_id': 0, 'data_id': 1, 'uid': 1 } ))
                last_data_id = max(map(int, [x['data_id'] for x in data_id_list]))
                last_uid = max(map(int, [x['uid'] for x in data_id_list]))
                self.clsdict.coad.db['data'].insert({'data_id':str(last_data_id+1),
                                         'uid':str(last_uid+1),
                                         'membership_id':membership_id,
                                         'value':default_value,
                                         'property_id':property_id})
                data_id = last_data_id+1
            # Check for existing text
            existing_text = self.clsdict.coad.db['text'].find({'data_id':str(data_id)})
            if existing_text.count() > 0:
                self.clsdict.coad.db['text'].update({'data_id':str(data_id)},
                                                    {'$set': {'value': value}},
                                                    multi=True)
            else:
                # Get class_id
                text_cls = self.clsdict.coad.db['class'].find_one( {'$or': [{'class_id':class_id},{'name':class_id}]})
                self.clsdict.coad.db['text'].insert({'data_id':str(data_id),
                                                     'class_id':text_cls['class_id'],
                                                     'value':value})
            # Check if tag != parent_object_id and it's not System.System
            if tag != 'System.System' and tag != parent_obj.hierarchy:
                # Check if tag already set for tag's object_id
                tag_obj = self.clsdict.coad.get_by_hierarchy(tag)
                tags = self.clsdict.coad.db['tag'].find({'data_id':str(data_id), 'object_id':tag_obj.meta['object_id']})
                _logger.info("Looking for tag %s, found %s",tag, tags.count())
                if tags.count() == 0:
                    self.clsdict.coad.db['tag'].insert({'data_id':str(data_id), 'object_id':tag_obj.meta['object_id']})
        return

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
                msg = '        '+k
                print(spacing + msg)
                #self.get_class().coad.get_by_hierarchy(k).dump(recursion_level+1)
        else:
            print(spacing+'    No children')
        # Properties
        props = self.get_properties()
        prop_keys = sorted(props)
        if len(prop_keys):
            print(spacing+'    Properties:')
            for pkey in prop_keys:
                print(spacing+'        '+pkey)
                for vkey in sorted(props[pkey]):
                    print(spacing+'            %s=%s'%(vkey, props[pkey][vkey]))
        else:
            print(spacing+'    No properties')
        # Text
        props = self.get_text()
        prop_keys = sorted(props)
        if len(prop_keys):
            print(spacing+'    Text values:')
            for pkey in prop_keys:
                print(spacing+'        '+pkey)
                for vkey in sorted(props[pkey]):
                    print(spacing+'            %s=%s'%(vkey, props[pkey][vkey]))
        else:
            print(spacing+'    No text values')

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
