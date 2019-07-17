from coad.plexos_mongo import load, save
from coad.coad_mongo import COAD
import logging
import unittest

from coad._compat import is_py2

# Only load the master once
master_coad = COAD('coad/master.xml')
logger = logging.getLogger(__name__)

class TestDB(unittest.TestCase):

    _multiprocess_can_split_=False
    '''
    def setUp(self):
        # TODO: Move into setupclass for big files
        # May have to copy data in the write tests to avoid poisoning the other tests
        self.filename='coad/master.xml'
        #filename='test/118-Bus.xml'
        #filename='test/Solar33P.xml'
        #filename='test/WFIP-MISO.xml'
        #filename='test/WWSIS.xml'
        master_coad=COAD(self.filename)
    '''

    def test_load(self):
        db = load('coad/master.xml')
        self.assertEqual(db['object'].find_one({'name':'Gurobi'}, {'_id':0}),
            {u'category_id': u'47', u'description': u'Use the Gurobi solver', u'class_id': u'47', u'object_id': u'9', u'name': u'Gurobi'})

    def test_save(self):
        db = load('coad/master.xml')
        save(db, 'coad/test/master_save_mongo.xml')
        newcoad = load('coad/test/master_save_mongo.xml')
        self.assertEqual(newcoad['object'].find_one({'name':'Gurobi'}, {'_id':0}),
            {u'category_id': u'47', u'description': u'Use the Gurobi solver', u'class_id': u'47', u'object_id': u'9', u'name': u'Gurobi'})

    def test_list(self):
        self.assertEqual(['MOSEK', 'CPLEX', 'Xpress-MP', 'Gurobi'], master_coad.list('Performance'))

    def test_classes(self):
        expected = [ "System", "Generator", "Fuel", "Fuel Contract", "Emission",
                     "Abatement", "Storage", "Waterway", "Power Station",
                     "Physical Contract", "Purchaser", "Reserve", "Market",
                     "Region", "Zone", "Node", "Line", "MLF", "Transformer",
                     "Phase Shifter", "Interface", "Contingency", "Company",
                     "Financial Contract", "Transmission Right", "Cournot",
                     "RSI", "Constraint", "Condition", "Data File", "Escalator",
                     "Variable", "Timeslice", "Scenario", "Model", "Project",
                     "Horizon", "Report", "LT Plan", "PASA", "MT Schedule",
                     "ST Schedule", "Transmission", "Production", "Competition",
                     "Stochastic", "Performance", "Diagnostic"]
        self.assertEqual(expected, [x for x in master_coad.keys()])
        # self.assertEqual(expected, list(master_coad.keys()))

    def test_objects(self):
        expected = [u'MOSEK', u'CPLEX', u'Xpress-MP', u'Gurobi']
        # Tests pass for the following
        self.assertEqual(expected, [x for x in master_coad['Performance'].keys()])
        # but not for the following!
        # TODO: The iterable returned by master_coad["Performance"].keys() does not have the .cls attribute
        # self.assertEqual(expected, list(master_coad['Performance'].keys()))

    def test_valid_attributes(self):
        expected = [u'Small MIP Max Time', u'Small MIP Integer Count', u'Cache Text Data in Memory',
                    u'MIP Max Time', u'MIP Maximum Threads', u'MIP Relative Gap',
                    u'Small MIP Relative Gap', u'MIP Node Optimizer',
                    u'MIP Improve Start Gap', u'Maximum Threads', u'MIP Root Optimizer',
                    u'SOLVER', u'Small MIP Improve Start Gap', u'Cold Start Optimizer 3',
                    u'Hot Start Optimizer 1', u'Hot Start Optimizer 2', u'Hot Start Optimizer 3',
                    u'Small LP Optimizer', u'Small LP Nonzero Count', u'Cold Start Optimizer 1',
                    u'Cold Start Optimizer 2']
        self.assertEqual(sorted(expected), sorted(list(master_coad['Performance'].valid_attributes.values())))

    def test_attribute_data(self):
        self.assertEqual(master_coad['Performance']['Gurobi']['SOLVER'],'4')

    def test_get_children(self):
        should_contain = [master_coad['Horizon']['Base'],master_coad['Report']['Base'],master_coad['ST Schedule']['Base']]
        if is_py2:
            self.assertItemsEqual(should_contain,master_coad['Model']['Base'].get_children())
            self.assertItemsEqual([master_coad['Horizon']['Base']],master_coad['Model']['Base'].get_children('Horizon'))
        else:
            self.assertCountEqual(should_contain,master_coad['Model']['Base'].get_children())
            self.assertCountEqual([master_coad['Horizon']['Base']],master_coad['Model']['Base'].get_children('Horizon'))


    def test_get_parents(self):
        should_contain = [master_coad['System']['System'],master_coad['Model']['Base']]
        if is_py2:
            self.assertItemsEqual(should_contain,master_coad['Horizon']['Base'].get_parents())
            self.assertItemsEqual([master_coad['Model']['Base']],master_coad['Horizon']['Base'].get_parents('Model'))
        else:
            self.assertCountEqual(should_contain,master_coad['Horizon']['Base'].get_parents())
            self.assertCountEqual([master_coad['Model']['Base']],master_coad['Horizon']['Base'].get_parents('Model'))

    def test_get_class(self):
        g_class = master_coad['Performance']['Gurobi'].get_class()
        assert master_coad["Performance"] == g_class
        # TODO: figure out why the following does not pass
        # self.assertItemsEqual(master_coad['Performance'], g_class)

    def test_get_collection_id(self):
        '''Test get collection id
        '''
        self.assertEqual('196', master_coad['Model'].get_collection_id(master_coad['Horizon'].meta['class_id']))

    def test_get_properties(self):
        '''Test get properties
        '''
        filename = 'coad/test/118-Bus.xml'
        coad = COAD(filename)
        # Get properties
        line = coad['Line']['126']
        props = {'System.System':{'Reactance': '0.0202', 'Max Flow': '9900', 'Min Flow': '-9900', 'Resistance': '0.00175'}}
        self.assertEqual(line.get_properties(), props)
        # Get property
        self.assertEqual(line.get_property('Max Flow', 'System.System'), '9900')
        # Get properties
        filename = 'coad/test/RTS-96.xml'
        coad = COAD(filename)
        g = coad['Generator']['101-1']
        props = {'System.System':{'Mean Time to Repair': '50',
                     'Load Point': ['15.8', '16', '19.8', '20'],
                     'Heat Rate': ['15063', '15000', '14500', '14499'],
                     'Min Up Time': '1',
                     'Max Ramp Up': '3',
                     'Min Down Time': '1',
                     'Min Stable Level': '15.8',
                     'Units': '1',
                     'Start Cost Time': ['0', '1'],
                     'Maintenance Frequency': '2',
                     'Maintenance Rate': '3.84',
                     'Max Capacity': '20',
                     'Forced Outage Rate': '10'}}
        self.assertEqual(g.get_properties(), props)
        # Get property
        self.assertEqual(g.get_property('Load Point'), ['15.8', '16', '19.8', '20'])
        # Tagged properties
        self.assertEqual(coad['Generator']['118-1'].get_properties()['Scenario.RT_UC'],{'Commit':'0'})

    def test_get_property_with_input_mask(self):
        '''Test property value input mask
        '''
        filename = 'coad/test/RTS-96.xml'
        coad = COAD(filename)
        self.assertEqual(coad['Node']['101'].get_property("Is Slack Bus"), "Yes")

    def test_get_by_hierarchy(self):
        identifier='Performance.Gurobi.SOLVER'
        self.assertEqual(master_coad.get_by_hierarchy(identifier),'4')
        self.assertEqual(master_coad['Performance']['Gurobi']['SOLVER'],'4')

    def test_get_by_object_id(self):
        o = master_coad.get_by_object_id('9')
        self.assertEqual('Performance', o.clsdict.meta['name'])
        self.assertEqual('Gurobi', o.meta['name'])

    def test_class_valid_properties(self):
        '''Test valid properties for a class
        '''
        expected = {"Production Rate":"816",
            "Removal Rate":"817",
            "Removal Cost":"818",
            "Production at Start":"819",
            "Shadow Price Scalar":"820",
            "Price Scalar":"821",
            "Allocation":"822",
            "Allocation Day":"823",
            "Allocation Week":"824",
            "Allocation Month":"825",
            "Allocation Year":"826"
            }
        self.assertEqual(expected, master_coad['Generator'].valid_properties_by_name['Emission'])

    def test_get_categories(self):
        '''Test class and object category retrieval
        '''
        self.assertEqual("-", master_coad['Performance'].get_categories()[0]['name'])
        self.assertEqual("-", master_coad['Performance']['Gurobi'].get_category())

    def test_get_text(self):
        '''Get text values for Data File objects
        '''
        filename = 'coad/test/RTS-96.xml'
        coad = COAD(filename)
        expected = {u'Scenario.4HA_UC': {u'Filename': u'\\Model a_DA_Base Solution\\interval\\ST Generator(*).Units Generating.csv'}}
        result = coad['Data File']['4HA_UC'].get_text()
        self.assertEqual(result, expected)

    def test_blank_elements(self):
        '''Make sure blank elements are saved properly
        '''
        filename = 'coad/test/118-Bus_with_blanks.xml'
        pre = COAD(filename)
        cat = pre.db['category'].find_one({'category_id':'49'})
        val = cat['name']
        self.assertEqual(val, "")
        pre.save('coad/test/118-Bus_with_blanks_test.xml')
        post = COAD('coad/test/118-Bus_with_blanks_test.xml')
        cat = post.db['category'].find_one({'category_id':'49'})
        val = cat['name']
        self.assertEqual(val, "")


class TestModifications(unittest.TestCase):

    def test_set(self):
        copy_coad = COAD('coad/master.xml')
        # Existing attribute
        identifier='Performance.Gurobi.SOLVER'
        self.assertEqual(copy_coad.get_by_hierarchy(identifier),'4')
        copy_coad.set(identifier,'3')
        self.assertEqual(copy_coad.get_by_hierarchy(identifier),'3')
        # New attribute
        identifier='Performance.CPLEX.MIP Relative Gap'
        copy_coad.set(identifier,'.002')
        self.assertEqual('.002',copy_coad.get_by_hierarchy(identifier))
        copy_coad.set(identifier,'.001')
        self.assertEqual('.001',copy_coad.get_by_hierarchy(identifier))
        copy_coad.save('coad/test/master_save_mongo_mod.xml')

    def test_copy(self):
        copy_coad = COAD('coad/master.xml')
        oldobj = copy_coad['Performance']['Gurobi']
        newobj = oldobj.copy()
        self.assertNotEqual(oldobj.meta['name'],newobj.meta['name'])
        for (k,v) in oldobj.items():
            self.assertEqual(v,newobj[k])
        copy_coad.list('Performance')
        newobj = oldobj.copy()
        self.assertNotEqual(oldobj.meta['name'],newobj.meta['name'])
        oldobj = copy_coad['Model']['Base']
        newobj = oldobj.copy('Test Base Model')
        self.assertIn('Test Base Model',copy_coad['Model'])
        should_contain = [copy_coad['Horizon']['Base'],copy_coad['Report']['Base'],copy_coad['ST Schedule']['Base']]
        if is_py2:
            self.assertItemsEqual(should_contain,copy_coad['Model']['Test Base Model'].get_children())
            self.assertItemsEqual([copy_coad['System']['System']], copy_coad['Model']['Test Base Model'].get_parents())
        else:
            self.assertCountEqual(should_contain,copy_coad['Model']['Test Base Model'].get_children())
            self.assertCountEqual([copy_coad['System']['System']], copy_coad['Model']['Test Base Model'].get_parents())

        self.assertRaises(Exception, copy_coad['Model']['Base'].copy, 'Test Base Model')

    def test_set_children(self):
        # Single new child
        copy_coad = COAD('coad/master.xml')
        copy_coad['Model']['Base'].set_children(copy_coad['Performance']['Gurobi'])
        should_contain = [copy_coad['Horizon']['Base'],copy_coad['Report']['Base'],copy_coad['ST Schedule']['Base'],copy_coad['Performance']['Gurobi']]
        self.assertEqual(should_contain, copy_coad['Model']['Base'].get_children())
        # Replace added model
        copy_coad['Model']['Base'].set_children(copy_coad['Performance']['CPLEX'])
        should_contain = [copy_coad['Horizon']['Base'],copy_coad['Report']['Base'],copy_coad['ST Schedule']['Base'],copy_coad['Performance']['CPLEX']]
        self.assertEqual(should_contain, copy_coad['Model']['Base'].get_children())
        # TODO: Test multiple new children of different classes that overwrites existing
        # TODO: Test adding new child once collection functionality is understood
        # TODO: Add mix of new child classes once collection functionality is understood
        # Children with multiple collections available
        filename = 'coad/test/RTS-96.xml'
        coad = COAD(filename)
        self.assertRaises(Exception, coad['Node']['101'].set_children, coad['Zone'].meta['class_id'])
        self.assertEqual('178', coad['Node'].get_collection_id(coad['Zone'].meta['class_id'], name="Zone"))
        dzone = coad['Zone'].new("Dummy Zone")
        coad['Node']['101'].set_children(dzone, name="Zone")

    def test_modify_single_properties(self):
        '''Tests related to modifying properties with a single value
        '''
        filename='coad/test/118-Bus.xml'
        coad=COAD(filename)
        # Get properties
        line = coad['Line']['126']
        props = {'System.System':{'Reactance': '0.0202', 'Max Flow': '9900', 'Min Flow': '-9900', 'Resistance': '0.00175'}}
        self.assertEqual(line.get_properties(), props)
        # Get property
        self.assertEqual(line.get_property('Max Flow'), '9900')
        # Set property
        line.set_property('Min Flow', '123456')
        self.assertEqual(line.get_property('Min Flow'), '123456')
        # Set properties
        #new_props = {'Reactance': 'aaaa', 'Max Flow': 'bbbb', 'Min Flow': 'cccc', 'Resistance': 'dddd'}
        #line_a = coad['Line']['027']
        #line_a.set_properties(new_props)
        # Test save and load
        coad.save('coad/test/118-Bus_props_test.xml')
        solar = COAD('coad/test/118-Bus_props_test.xml')
        props['System.System']['Min Flow']='123456'
        line = solar['Line']['126']
        self.assertEqual(line.get_properties(), props)
        #line_a = coad['Line']['027']
        #self.assertEqual(line_a.get_properties(), new_props)

    def test_modify_single_tagged_properties(self):
        '''Tests related to modifying tagged properties with a single value
        '''
        filename = 'coad/test/RTS-96.xml'
        coad = COAD(filename)
        self.assertEqual(coad['Generator']['118-1'].get_properties()['Scenario.RT_UC'],{'Commit':'0'})
        coad['Generator']['118-1'].set_property('Commit', 'totally_committed', 'Scenario.RT_UC')
        # Test that dynamic flag is not set for Unit Commitment Optimality
        self.assertEqual(coad['Generator'].valid_properties['System']['12']['is_dynamic'],'false')
        self.assertEqual(coad['Generator'].valid_properties['System']['12']['is_enabled'],'false')
        self.assertRaises(Exception, coad['Generator']['118-1'].set_property, 'Unit Commitment Optimality', 'I hate input masks', 'Scenario.RT_UC')
        # Make sure invalid assignment does not trigger is_dynamic=true
        self.assertEqual(coad['Generator'].valid_properties['System']['12']['is_dynamic'],'false')
        self.assertEqual(coad['Generator'].valid_properties['System']['12']['is_enabled'],'false')
        # This tag isn't set at all
        coad['Generator']['118-1'].set_property('Unit Commitment Optimality', 'Rounded Relaxation', 'Scenario.RT_UC')
        self.assertEqual(coad['Generator'].valid_properties['System']['12']['is_dynamic'],'true')
        self.assertEqual(coad['Generator'].valid_properties['System']['12']['is_enabled'],'true')
        coad.save('coad/test/RTS-96_props_test.xml')
        saved_coad = COAD('coad/test/RTS-96_props_test.xml')
        expected = {'Commit':'totally_committed', 'Unit Commitment Optimality':'Rounded Relaxation'}
        self.assertEqual(saved_coad['Generator']['118-1'].get_properties()['Scenario.RT_UC'], expected)

    def test_modify_multiband_tagged_properties(self):
        '''Tests related to modifying tagged properties with a list of values
        '''
        filename = 'coad/test/RTS-96.xml'
        coad = COAD(filename)
        test_list = ['1','2','3','4']
        coad['Generator']['118-1'].set_property("Load Point", test_list, tag="Scenario.4HA_UC")
        logger.info(coad['Generator']['118-1'].get_properties())
        self.assertEqual(coad['Generator']['118-1'].get_property("Load Point", "Scenario.4HA_UC"), test_list)

    def test_set_text(self):
        '''Set text values for Data File objects
        '''
        filename = 'coad/test/RTS-96.xml'
        coad = COAD(filename)
        # Change an existing data file
        expected = {'Scenario.4HA_UC': {'Filename': 'test_filename'}}
        coad['Data File']['4HA_UC'].set_text('Filename', 'test_filename')
        result = coad['Data File']['4HA_UC'].get_text()
        self.assertEqual(result, expected)
        # Create a new data file text
        coad['Data File']['RT_UC'].copy('test_data_file')
        coad['Data File']['test_data_file'].set_text('Filename', 'another_test_filename', tag='Scenario.4HA_UC')
        result = coad['Data File']['test_data_file'].get_text()
        expected = {'Scenario.4HA_UC': {'Filename': 'another_test_filename'}}
        self.assertEqual(result, expected)
        last_text = coad.db['text'].find().sort([('$natural', -1)]).limit(1)
        self.assertEqual('41', last_text[0]['class_id'])
        #cur = coad.dbcon.cursor()
        #cur.execute("SELECT class_id FROM text WHERE data_id=(SELECT MAX(data_id) FROM text)")
        #self.assertEqual(41, cur.fetchone()[0])
        # Dup settings caused prop list to grow
        coad['Data File']['test_data_file'].set_text('Filename', 'another_test_filename', tag='Scenario.4HA_UC')
        result = coad['Data File']['test_data_file'].get_properties()
        self.assertEqual('0', result['Scenario.4HA_UC']['Filename'])

    def test_add_set_category(self):
        '''Test category creation for class and set for object
        '''
        copy_coad = COAD('coad/master.xml')
        new_cat_id = copy_coad['Performance'].add_category("Test Category")
        self.assertEqual(new_cat_id, copy_coad['Performance'].get_category_id("Test Category"))
        new_cat = copy_coad['Performance'].get_categories()[1]
        self.assertEqual("Test Category", new_cat['name'])
        self.assertEqual("1", new_cat['rank'])
        copy_coad['Performance']['Gurobi'].set_category("Test Category")
        self.assertEqual("Test Category", copy_coad['Performance']['Gurobi'].get_category())

    def test_new_object(self):
        '''Test creation of objects
        '''
        copy_coad = COAD('coad/master.xml')
        new_obj = copy_coad['Model'].new("Test Model")
        self.assertEqual("Test Model", new_obj.meta['name'])
        new_obj2 = copy_coad['Model'].new("Test Model Custom", category="Custom Category")
        self.assertEqual("Test Model Custom", new_obj2.meta['name'])
        self.assertEqual("Custom Category", new_obj2.get_category())
        filename = 'coad/test/RTS-96.xml'
        coad = COAD(filename)
        new_obj = coad['Node'].new("Test Node")
        new_obj.set_property("Allow Dump Energy", "No")
        self.assertEqual("No", new_obj.get_property("Allow Dump Energy"))

    def test_tag_property(self):
        '''Test tagging and untagging of existing property
        '''
        filename = 'coad/test/RTS-96.xml'
        coad = COAD(filename)
        g = coad['Generator']['101-1']
        props = {'Load Point': ['15.8', '16', '19.8', '20'],
                }
        g.tag_property("Load Point", tag="Scenario.DA")
        self.assertIn("Scenario.DA", g.get_properties())
        self.assertEqual(props, g.get_properties()['Scenario.DA'])
        g.untag_property("Load Point", tag="Scenario.DA")
        self.assertNotIn("Scenario.DA", g.get_properties().keys())

    '''



class TestObjectDict(unittest.TestCase):
    def setUp(self):
        # TODO: Move into setupclass for big files
        # May have to copy data in the write tests to avoid poisoning the other tests
        filename='coad/master.xml'
        #filename='test/118-Bus.xml'
        #filename='test/Solar33P.xml'
        #filename='test/WFIP-MISO.xml'
        #filename='test/WWSIS.xml'
        master_coad=COAD(filename)
        # Fix bad api for python 3.2
        if not hasattr(self,'assertItemsEqual'):
            self.assertItemsEqual=self.assertCountEqual

    def test_del(self):
        # Existing attribute
        del(master_coad['Model']['Base']['Enabled'])
        # Make sure the db has been modified
        fresh_obj=ObjectDict(master_coad,master_coad['Model']['Base'].meta)
        #print(fresh_obj)
        self.assertNotIn('Enabled',fresh_obj.keys())
        #master_coad.save('master_noenable.xml')
'''

"""
class TestObjectDictProperties(unittest.TestCase):
    '''Test properties using multiple input files
    '''
    def test_single_properties(self):
        '''Tests related to properties with a single value
        '''
        filename='coad/test/118-Bus.xml'
        coad=COAD(filename)
        # Get properties
        line = coad['Line']['126']
        props = {'Reactance': '0.0202', 'Max Flow': '9900', 'Min Flow': '-9900', 'Resistance': '0.00175'}
        self.assertEqual(line.get_properties(), props)
        # Get property
        self.assertEqual(line.get_property('Max Flow'), '9900')
        # Set property
        line.set_property('Min Flow', '123456')
        self.assertEqual(line.get_property('Min Flow'), '123456')
        # Set properties
        new_props = {'Reactance': 'aaaa', 'Max Flow': 'bbbb', 'Min Flow': 'cccc', 'Resistance': 'dddd'}
        line_a = coad['Line']['027']
        line_a.set_properties(new_props)
        # Test save and load
        coad.save('coad/test/118-Bus_props_test.xml')
        solar = COAD('coad/test/118-Bus_props_test.xml')
        props['Min Flow']='123456'
        line = solar['Line']['126']
        self.assertEqual(line.get_properties(), props)
        line_a = coad['Line']['027']
        self.assertEqual(line_a.get_properties(), new_props)

    def test_multi_properties(self):
        '''Tests related to properties with a list of values
        '''
        filename='coad/test/RTS-96.xml'
        coad=COAD(filename)
        # Get properties
        g = coad['Generator']['101-1']
        props = {'Mean Time to Repair': '50', 'Load Point': ['15.8', '16', '19.8', '20'], 'Heat Rate': ['14499', '14500', '15000', '15063'], 'Min Up Time': '1', 'Max Ramp Up': '3', 'Min Down Time': '1', 'Min Stable Level': '15.8', 'Units': '1', 'Start Cost Time': ['0', '1'], 'Maintenance Frequency': '2', 'Maintenance Rate': '3.84', 'Max Capacity': '20', 'Forced Outage Rate': '10'}
        self.assertEqual(g.get_properties(), props)
        # Get property
        self.assertEqual(g.get_property('Load Point'), ['15.8', '16', '19.8', '20'])
        # Set property
        g.set_property('Load Point', ['a', 'b', 'c', 'd'])
        self.assertEqual(g.get_property('Load Point'), ['a', 'b', 'c', 'd'])
        # Set property with wrong length
        with self.assertRaises(Exception):
            g.set_property('Load Point', ['a', 'b', 'c'])
        # Set properties
        new_props = {'Maintenance Rate': 'aaaa', 'Heat Rate': ['bbbb', 'cccc', 'dddd', 'eeee']}
        g2 = coad['Generator']['123-3']
        g2.set_properties(new_props)
        # Test save and load
        coad.save('coad/test/RTS-96_props_test.xml')
        saved_coad = COAD('coad/test/RTS-96_props_test.xml')
        props['Load Point'] = ['a', 'b', 'c', 'd']
        g = saved_coad['Generator']['101-1']
        self.assertEqual(g.get_properties(), props)
        g2 = saved_coad['Generator']['123-3']
        self.assertEqual(g2.get_property('Maintenance Rate'), new_props['Maintenance Rate'])
        self.assertEqual(g2.get_property('Heat Rate'), new_props['Heat Rate'])
"""
