import argparse
import csv
import numpy as np
import os
import pandas as pd
import sys

#silly type checking thin
try:
    basestring
except NameError:
    basestring = str

#####
# export the data associated with a plexos input model and write as accessable csv files
#
# USAGE:
#  from coad.COAD import COAD
#  from coad import export_plexos_model
#  c = COAD(<filename for xml or db here>)
#  export_plexos_model.write_object_report(c['Model']['modelname'])


# TODO: add scenario read order (default=0, if == 0: read_order='alphabetical')
#   so that conflicting property/attribute definitions use the data defined by the higher read order

def get_model_items(coad,models, filter_val = '',filter_cls = 'Region'):
    #right now this only works for a single model in the models list. It runs with multiple,
    # but the resulting data isn't easily mapped back to the original model.
    # ... could replace [models] with model and remove for loop, or
    # enable model specfication in export_data...

    if filter_val != '':
        try:
            regions = [coad[filter_cls][filter_val]]
        except:
            if filter_cls=='Region':
                filter_cls = 'Zone'
            else:
                filter_cls = 'Region'

        try:
            regions = [coad[filter_cls][filter_val]]
        except:
            regions = ['']
            print('Cannot find filter in Regions or Zones')
    else:
        regions = ['']


    objects = []
    scenarios = []

    if models == ['']:
        sys.exit('Please specify a model to export')

    for mod in models:
        #all possible objects in a PLEXOS model
        all_fields = set(coad.keys())

        #objects with explicitly defined memberships to the model
        all_children = set([o.hierarchy for o in coad['Model'][mod].get_children()])

        #scenarios to gather
        scen = set([o.hierarchy for o in coad['Model'][mod].get_children('Scenario')])
        scenarios.extend([i.split('.',1)[1] for i in scen])

        #objects with model membership (without scenarios)
        settings = all_children-scen

        #parameters = [i.split('.',1)[0] for i in settings]
        #settings = [i.split('.',1)[1] for i in settings]

        for s in settings:
            objects.append({'cls': s.split('.',1)[0],
                                'name': s.split('.',1)[1]})


    #get all the data files associated with the scenarios
    data_files = []
    for data_file in coad['Data File']: #loop through every data file and only grab the ones that are active
        for text in coad['Data File'][data_file].get_text():
            if text in ['Scenario.' + s for s in scenarios] + ['System.System'] :
                data_files.append({'cls': coad['Data File'][data_file].get_class().meta['name'],'name': coad['Data File'][data_file].meta['name']})



    #get all relatives of filter region/zone
    if regions == ['']:
        regions = coad[filter_cls].values() # + coad['Zone'].values()

        for cls in coad.keys():
            for n in coad[cls].keys():
                objects.append({'cls':cls,'name':n})


    else:
        for r in regions:
            #print(filter_cls + ':  ' + r.meta['name'])
            for p in r.get_parents():
                #print('    parent: ' + p.meta['name'])
                objects.append({#'relationship': 'parent',
                                    'cls': p.get_class().meta['name'],
                                    'name': p.meta['name']})
                                    #,'obj': p})
                if p.meta['name'] != 'System':
                    for pp in p.get_children():
                        objects.append({#'relationship': 'child',
                                    'cls': pp.get_class().meta['name'],
                                    'name': pp.meta['name']})
                                    #,'obj': pp})
                for pp in p.get_parents():
                    objects.append({#'relationship': 'parent',
                                'cls': pp.get_class().meta['name'],
                                'name': pp.meta['name']})
                                #,'obj': pp})


    print('Done Collecting Objects, removing duplicates...')
    # drop the duplicates
    objects = pd.DataFrame(objects).drop_duplicates().reset_index(drop=True) #.T.to_dict().values()
    if len(data_files):
        objects.drop(objects.index[(objects['cls']=='Data File') & (~objects['name'].isin([x['name'] for x in data_files]))],inplace=True)
    else:
        objects.drop(objects.index[objects['cls']=='Data File'],inplace=True)

    print('Done')

    export_objects = {'objects':objects,'scenarios':scenarios,'data_files':list(data_files),'models':models}
    return(export_objects)


def export_data(coad, export_objects):
    # this is really slow, I think due to the get_properties, get_children, and get_parents calls
    d = []

    nobj = len(export_objects['objects'])
    for index, row in export_objects['objects'].iterrows():
        #clear_output()
        #print('{0}/{1}'.format(index,nobj))
        obj_class = row['cls']
        obj_name = row['name']
        if not(obj_class=='System' and obj_name=='System'):
            #print(obj_class,obj_name)
            obj = coad[obj_class][obj_name]
            if obj.keys():
                for atr, val in obj.items():
                    d.append({'cls': obj_class,
                                         'object': obj_name,
                                         'property': atr,
                                         'value': val})
            #relationships
            all_children = set([o.hierarchy for o in obj.get_children()])
            all_parents = set([o.hierarchy for o in obj.get_parents()])
            children = all_children - all_parents
            parents = all_parents - all_children
            peers = all_children & all_parents

            #parents
            if len(parents):
                for p in parents:
                    p_cls, p_val = p.split('.',1)
                    if (p_cls == 'Model' and p_val in export_objects['models']) or p_cls != 'Model':
                        d.append({'cls': obj_class,
                                         'object': obj_name,
                                         'property': p_cls,
                                         'value': p_val})
            #peers
            if len(peers):
                for p in peers:
                    p_cls, p_val = p.split('.',1)
                    d.append({'cls': obj_class,
                                     'object': obj_name,
                                     'property': p_cls,
                                     'value': p_val})
            #children
            if len(children):
                for p in children:
                    p_cls, p_val = p.split('.',1)
                    d.append({'cls': obj_class,
                                     'object': obj_name,
                                     'property': p_cls,
                                     'value': p_val})

            # Properties
            props = obj.get_properties()
            if len(props):
                props = pd.DataFrame(props).stack().reset_index()
                props.columns = ['property','scenario','value']
                props['cls'] = obj_class
                props['object'] = obj_name
                for item in props.to_dict('records'):
                    d.append(item)

            # Text
            props = obj.get_text()
            if len(props):
                props = pd.DataFrame(props).stack().reset_index()
                props.columns = ['property','scenario','value']
                props['cls'] = obj_class
                props['object'] = obj_name
                for item in props.to_dict('records'):
                    d.append(item)



    d = pd.DataFrame(d)
    d['scenario'].loc[(d['scenario']=='') | d['scenario'].isnull()] = 'System.System'
    d.drop(d.index[d['value']=='System'],inplace=True)
    d.drop(d.index[~d['scenario'].isin(['Data File.' + s['name'] for s in export_objects['data_files']] + ['Scenario.' + s for s in export_objects['scenarios']] + ['System.System'])],inplace=True)
    d.value = d.value.apply(lambda x: tuple(x) if type(x) is list else x)
    return(d)

def write_tables(data,folder=''):
    #write readable csv files for each data class

    def f(x):
        if any(x.columns.str.contains('scenario')):
            if len(x.scenario.unique())>1:
                y = dict(x[['scenario','value']].to_dict('split')['data'])
            else:
                y=tuple(x.value)
        else:
            y=tuple(x.value)
        return(y)


    for cls in data.cls.unique():
        #df = data.loc[data['cls']==cls].groupby(['object', 'property'])['value'].apply(lambda x: tuple(x)).reset_index()\
        #        .pivot(index='object', columns='property', values='value').fillna('')
        df = data.loc[data['cls']==cls].groupby(['object', 'property']).apply(f).reset_index()\
                .pivot(index='object', columns='property').fillna('')
        df = df.applymap(lambda x: x[0] if (len(x) == 1) else x)
        df.columns = df.columns.droplevel(0)
        df.to_csv(os.path.join(folder,cls+'.csv'))

def get_related_objects(coad_obj, obj_id, obj_set=None):
    """Recursively get all object related to passed object
        Searches:
            - children in membership
            - tagged data

        The system object (obj_id = 1) is ignored in all cases

        Return set of obj_ids with duplicates removed
    """
    #if obj_id == '1':
        # Ignore System object
        # return obj_set
    if obj_set is None:
        obj_set = set([1])
    cur = coad_obj.dbcon.cursor()
    cur.execute("SELECT child_object_id FROM membership WHERE parent_object_id=?", (obj_id,))
    ret_list = []
    for row in cur.fetchall():
        ret_list.append(row[0])
    cur.execute("""SELECT m.child_object_id FROM tag t
    INNER JOIN property p ON p.property_id=d.property_id
    INNER JOIN data d ON t.data_id=d.data_id
    INNER JOIN membership m ON m.membership_id=d.membership_id
    WHERE t.object_id=?""", (obj_id,))
    for row in cur.fetchall():
        ret_list.append(row[0])
    ret_set = set(ret_list)
    new_obj_ids = ret_set - obj_set
    total_set = ret_set | obj_set
    for o_id in new_obj_ids:
        new_obj_set = get_related_objects(coad_obj, o_id, total_set)
        total_set = new_obj_set | total_set
    return total_set

def write_csv_dict(csv_dict,folder,cls_name):
            # Write file for this class
        if len(csv_dict.keys()) > 0:
            filename = os.path.join(folder, "%s.csv"%cls_name)
            print("Writing %s"%filename)
            # Get all columns
            colnames = []
            for (oid, dat) in csv_dict.items():
                colnames = list(set(colnames) | set(dat.keys()))
            #print ("Columns:", colnames)
            with open(filename, 'w') as csvfile:
                csvwriter = csv.writer(csvfile)
                # Write header
                csvwriter.writerow(['object'] + colnames)
                for (oid, dat) in csv_dict.items():
                    # Get object name
                    cur.execute("SELECT name FROM object o WHERE object_id=?", (oid,))
                    row = [cur.fetchone()[0]]
                    # Write row
                    for x in colnames:
                        if x in dat:
                            row.append(dat[x])
                        else:
                            row.append("")
                    csvwriter.writerow(row)
        else:
            print("Class %s has no object data"%cls_name)

def create_csv_dict(coad_obj,cls_name,cur,obj_list_super,tagset):
    csv_dict = {}
    # Get attributes
    istart = 0
    delta = 999 #max number of sql variables
    while istart < len(obj_list_super): # this breaks on large datasets, so limit query sizes...
        obj_list = obj_list_super[istart:istart+delta]
        #start_time = time.time()
        cur.execute("""SELECT ad.object_id, a.name, ad.value FROM attribute_data ad
            INNER JOIN attribute a ON a.attribute_id=ad.attribute_id
            WHERE ad.object_id IN (%s)"""%",".join(["?"]*len(obj_list)),obj_list)
        #print("--- %s seconds ---" % (time.time() - start_time))
        for row in cur.fetchall():
            if row[0] not in csv_dict:
                csv_dict[row[0]] = {}
            obj_dict = csv_dict[row[0]]
            if row[1] in obj_dict:
                if isinstance(obj_dict[row[1]], str):
                    obj_dict[row[1]] = [obj_dict[row[1]]]
                obj_dict[row[1]].append(row[2])
            else:
                obj_dict[row[1]]=row[2]
        istart += delta
    # New way to get properties and text
    for obj_id in obj_list_super:
        obj = coad_obj.coad.get_by_object_id(obj_id)
        #for o_props in (obj.get_properties(), obj.get_text()):
        o_props = obj.get_properties()
        # By object, get_properties and get_text returns a dict of tag:propname:value(s)
        # Needs to be transformed to propname:tag:values
        if obj_id not in csv_dict:
            csv_dict[obj_id] = {}
        props_dict = dict([[s.split('.',1)[1],s] for s in list(o_props.keys())])
        cur.execute("""SELECT o.object_id, o.name FROM object o
            WHERE o.name IN (%s)"""%",".join(["?"]*len(props_dict)),list(props_dict.keys()))
        props_dict = dict([[i[0],props_dict[i[1]]] for i in cur.fetchall()])
        filtered_props = [props_dict[i] for i in list(set(props_dict.keys()).intersection(all_interesting_objs))]

        for (tagname, pdict) in o_props.items():
            if tagname in filtered_props:
                for (propname, values) in pdict.items():
                    if propname not in csv_dict[obj_id]:
                        csv_dict[obj_id][propname] = {}
                    if tagname in csv_dict[obj_id][propname]:
                        print("Duplicate name: %s Object: %s Tag: %s Oldval: %s Newval: %s "%(propname, obj_id, tagname, csv_dict[obj_id][propname][tagname], values))
                    csv_dict[obj_id][propname][tagname] = values
        o_props = obj.get_text()
        # Text objects overwrite properties, so rename property to propname(text)
        if obj_id not in csv_dict:
            csv_dict[obj_id] = {}
        for (tagname, pdict) in o_props.items():
            [tagclass,tagval] = tagname.split('.',1)
            value = coad_obj.coad[tagclass][tagval].meta['object_id']
            if value not in all_interesting_objs: #add tags that dont exist in classmap to a new map, then append existing csv's or write new ones
                tagset = tagset | set([value])
            for (propname, values) in pdict.items():
                propname += "(text)"
                if propname not in csv_dict[obj_id]:
                    csv_dict[obj_id][propname] = {}
                if tagname in csv_dict[obj_id][propname]:
                    print("Duplicate name: %s Object: %s Tag: %s Oldval: %s Newval: %s "%(propname, obj_id, tagname, csv_dict[obj_id][propname][tagname], values))
                csv_dict[obj_id][propname][tagname] = values

    # Get tags
    # Get children listed under class name
    istart = 0
    delta = 999 #max number of sql variables
    while istart < len(obj_list_super): # this breaks on large datasets, so limit query sizes...
        obj_list = obj_list_super[istart:istart+delta]
        #start_time = time.time()
        cur.execute("""SELECT m.parent_object_id, c.name, o.name FROM membership m
            INNER JOIN class c ON c.class_id=m.child_class_id
            INNER JOIN object o ON o.object_id=m.child_object_id
            WHERE m.parent_object_id IN (%s)"""%",".join(["?"]*len(obj_list)),obj_list)
        for row in cur.fetchall():
            (obj_id, name, value) = row
            if obj_id not in csv_dict:
                csv_dict[obj_id] = {}
            obj_dict = csv_dict[obj_id]
            if name in obj_dict:
                if isinstance(obj_dict[name],basestring):
                    obj_dict[name] = [obj_dict[name]]
                obj_dict[name].append(value)
            else:
                obj_dict[name] = value
        istart += delta
    return csv_dict, tagset

def create_class_map(cur,interesting_objs):
    class_map = {}
    for c_obj in interesting_objs:
        cur.execute("""SELECT c.name FROM object o
            INNER JOIN class c ON c.class_id=o.class_id
            WHERE o.object_id=?""",(c_obj,))
        t_cls = cur.fetchone()[0]
        if t_cls not in class_map:
            class_map[t_cls] = []
        class_map[t_cls].append(c_obj)
    return class_map

def write_object_report(coad_obj, folder=None):
    """Retrieve all associated objects to coad_obj, pull in attributes, properties,
    and texts.  Write as a series of CSV files in folder.
    """
    interesting_objs = get_related_objects(coad_obj.coad, coad_obj.meta['object_id'])
    cur = coad_obj.coad.dbcon.cursor()
    if folder is None:
        folder = coad_obj.meta['name']
    print("Writing report on %s objects to %s"%(len(interesting_objs), folder))
    if not os.path.isdir(folder):
        print("Creating report folder %s"%folder)
        os.makedirs(folder)
    # Create class mapping dict
    class_map = create_class_map(cur,interesting_objs)
    all_interesting_objs = set([item for sublist in class_map.values() for item in sublist])
    tagset = set()
    for cls_name, obj_list_super in class_map.items():
        #for obj_id in class_map[cls_name]:
        csv_dict, tagset = create_csv_dict(coad_obj,cls_name,cur,obj_list_super,tagset)
        write_csv_dict(csv_dict,folder,cls_name)
    #print class_map
    
    #do it again for the objects identified with tags
    tag_class_map = create_class_map(cur,tagset)
    tag_all_interesting_objs = set([item for sublist in tag_class_map.values() for item in sublist])
    tag_tagset = set()
    for tag_cls_name, tag_obj_list_super in tag_class_map.items():
        #for obj_id in class_map[cls_name]:
        tag_csv_dict, tag_tagset = create_csv_dict(coad_obj,tag_cls_name,cur,tag_obj_list_super,tag_tagset)
        write_csv_dict(tag_csv_dict,folder,tag_cls_name)


def main():
    parser = argparse.ArgumentParser(description="Export csv files for a specific PLEXOS input model")

    parser.add_argument('-f', '--filepath', help='path to PLEXOS input .xml or .db')
    parser.add_argument('-m', '--models', help='list of models to export',default='')
    parser.add_argument('-c', '--filter_cls', help='optional-class of filter string, e.g. Region, Zone',default='')
    parser.add_argument('-n', '--filter_val', help='optional-name of region or zone to extract',default='')
    parser.add_argument('-o', '--output_folder', help='folder to output csv files')

    args = parser.parse_args()


    coad = COAD(args.filepath)
    export_objects= get_model_items(coad,models=args.models,filter_cls=args.filter_cls,filter_val=args.filter_val)
    data = export_data(coad,export_objects)
    write_tables(data,args.output_folder)


if __name__ == "__main__":
    main()
