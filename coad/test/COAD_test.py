from coad.COAD import COAD, ObjectDict
import unittest

# Only load the master once
master_coad = COAD('coad/master.xml')

class TestCOAD(unittest.TestCase):
    _multiprocess_can_split_=False

    def test_load(self):
        self.assertEqual(master_coad['Performance']['Gurobi']['SOLVER'],'4')

    def test_list(self):
        self.assertEqual(set(['MOSEK', 'CPLEX', 'Xpress-MP', 'Gurobi']), set(master_coad.list('Performance')))

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
        self.assertEqual(expected, master_coad.keys())

    def test_objects(self):
        expected = [u'MOSEK', u'CPLEX', u'Xpress-MP', u'Gurobi']
        self.assertEqual(set(expected), set(master_coad['Performance'].keys()))

    def test_get(self):
        identifier='Performance.Gurobi.SOLVER'
        self.assertEqual(master_coad.get_by_hierarchy(identifier),'4')
        self.assertEqual(master_coad['Performance']['Gurobi']['SOLVER'],'4')

    def test_set(self):
        # Existing attribute
        identifier='Performance.Gurobi.SOLVER'
        self.assertEqual(master_coad.get_by_hierarchy(identifier),'4')
        master_coad.set(identifier,'3')
        self.assertEqual(master_coad.get_by_hierarchy(identifier),'3')
        # New attribute
        identifier='Performance.CPLEX.MIP Relative Gap'
        master_coad.set(identifier,'.002')
        self.assertEqual('.002',master_coad.get_by_hierarchy(identifier))
        master_coad.set(identifier,'.001')
        self.assertEqual('.001',master_coad.get_by_hierarchy(identifier))
        master_coad.save('coad/test/master_save_sqlite_mod.xml')

    def test_save(self):
        master_coad.save('coad/test/master_save_sqlite.xml')
        newcoad = COAD('coad/test/master_save_sqlite.xml')
        self.assertEqual(newcoad['Performance']['Gurobi']['SOLVER'],'4')

    def test_get_by_hierarchy(self):
        identifier='Performance.Gurobi.SOLVER'
        self.assertEqual(master_coad.get_by_hierarchy(identifier),'4')
        self.assertEqual(master_coad['Performance']['Gurobi']['SOLVER'],'4')

    def test_get_by_object_id(self):
        o = master_coad.get_by_object_id('9')
        self.assertEqual('Performance', o.get_class().meta['name'])
        self.assertEqual('Gurobi', o.meta['name'])

    def test_get_collection_id(self):
        '''Test get collection id
        '''
        self.assertEqual('196', master_coad['Model'].get_collection_id(master_coad['Horizon'].meta['class_id']))

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
        actual = master_coad['Generator'].valid_properties_by_name['Emission']
        for (k,v) in expected.iteritems():
            self.assertIn(k, actual)
            self.assertEqual(v, actual[k])
        #self.assertEqual(expected, master_coad['Generator'].valid_properties_by_name['Emission'])

    def test_config(self):
        '''Get and set config elements
        '''
        self.assertEqual('0', master_coad.get_config('Dynamic'))
        master_coad.set_config('Dynamic', '-1')
        self.assertEqual('-1', master_coad.get_config('Dynamic'))

class TestObjectDict(unittest.TestCase):
    def setUp(self):
        # TODO: Move into setupclass for big files
        # May have to copy data in the write tests to avoid poisoning the other tests
        filename='coad/master.xml'
        #filename='test/118-Bus.xml'
        #filename='test/Solar33P.xml'
        #filename='test/WFIP-MISO.xml'
        #filename='test/WWSIS.xml'
        self.coad=COAD(filename)
        # Fix bad api for python 3.2
        if not hasattr(self,'assertItemsEqual'):
            self.assertItemsEqual=self.assertCountEqual

    def test_copy(self):
        oldobj = self.coad['Performance']['Gurobi']
        newobj = oldobj.copy()
        self.assertNotEqual(oldobj.meta['name'],newobj.meta['name'])
        for (k,v) in oldobj.items():
            self.assertEqual(v,newobj[k])
        self.coad.list('Performance')
        newobj = oldobj.copy()
        self.assertNotEqual(oldobj.meta['name'],newobj.meta['name'])
        oldobj = self.coad['Model']['Base']
        newobj = oldobj.copy('Test Base Model')
        self.assertIn('Test Base Model',self.coad['Model'])
        should_contain = [self.coad['Horizon']['Base'],self.coad['Report']['Base'],self.coad['ST Schedule']['Base']]
        self.assertItemsEqual(should_contain,self.coad['Model']['Test Base Model'].get_children())
        self.assertItemsEqual([self.coad['System']['System']], self.coad['Model']['Test Base Model'].get_parents())
        self.assertRaises(Exception, self.coad['Model']['Base'].copy, 'Test Base Model')

    def test_get_parents(self):
        should_contain = [master_coad['System']['System'],master_coad['Model']['Base']]
        self.assertItemsEqual(should_contain,master_coad['Horizon']['Base'].get_parents())
        self.assertItemsEqual([master_coad['Model']['Base']],master_coad['Horizon']['Base'].get_parents('Model'))

    def test_get_children(self):
        should_contain = [self.coad['Horizon']['Base'],self.coad['Report']['Base'],self.coad['ST Schedule']['Base']]
        self.assertItemsEqual(should_contain,self.coad['Model']['Base'].get_children())
        self.assertItemsEqual([self.coad['Horizon']['Base']],self.coad['Model']['Base'].get_children('Horizon'))

    def test_set_children(self):
        # Single new child
        self.coad['Model']['Base'].set_children(self.coad['Performance']['Gurobi'])
        should_contain = [self.coad['Horizon']['Base'],self.coad['Report']['Base'],self.coad['ST Schedule']['Base'],self.coad['Performance']['Gurobi']]
        self.assertEqual(should_contain,self.coad['Model']['Base'].get_children())
        # Duplicates
        self.coad['Model']['Base'].set_children(self.coad['Performance']['Gurobi'], replace=False)
        self.assertEqual(1, len(self.coad['Model']['Base'].get_children('Performance')))
        # TODO: Test multiple new children of different classes that overwrites existing
        # TODO: Test adding new child once collection functionality is understood
        # TODO: Add mix of new child classes once collection functionality is understood

    def test_get_class(self):
        g_class = self.coad['Performance']['Gurobi'].get_class()
        self.assertItemsEqual(self.coad['Performance'],g_class)

    def test_del(self):
        # Existing attribute
        del(self.coad['Model']['Base']['Enabled'])
        # Make sure the db has been modified
        fresh_obj=ObjectDict(self.coad,self.coad['Model']['Base'].meta)
        #print(fresh_obj)
        self.assertNotIn('Enabled',fresh_obj.keys())
        #self.coad.save('master_noenable.xml')

    def test_get_categories(self):
        '''Test class and object category retrieval
        '''
        self.assertEqual("-", self.coad['Performance'].get_categories()[0]['name'])
        self.assertEqual("-", self.coad['Performance']['Gurobi'].get_category())

    def test_7_400_2(self):
        '''Test changes related to the 7.400.2 version of master.xml
        '''
        coad=COAD('coad/master_7.400.2.xml')
        # Test model first, as the horizons will attach themselves to the copied base model
        oldobj = coad['Model']['Base']
        newobj = oldobj.copy('Test Base Model')
        self.assertIn('Test Base Model', coad['Model'])
        should_contain = [coad['Horizon']['Base'],coad['Report']['Base'],coad['ST Schedule']['Base']]
        self.assertItemsEqual(should_contain, coad['Model']['Test Base Model'].get_children())
        oldobj = coad['Horizon']['Base']
        newobj = oldobj.copy('New Horizon')
        self.assertNotEqual(oldobj.meta['name'],newobj.meta['name'])
        for (k,v) in oldobj.items():
            self.assertEqual(v,newobj[k])
        self.assertEqual(['Base', 'New Horizon'], coad.list('Horizon'))
        self.assertNotEqual(coad['Horizon']['Base'].meta['GUID'], coad['Horizon']['New Horizon'].meta['GUID'])
        newobj = oldobj.copy()
        self.assertNotEqual(oldobj.meta['name'],newobj.meta['name'])


class TestObjectDictProperties(unittest.TestCase):
    '''Test properties using multiple input files
    '''
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
                     'Load Point': ['20', '19.8', '16', '15.8'],
                     'Heat Rate': ['15063', '14499', '14500', '15000'],
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
        self.assertEqual(g.get_property('Load Point'), ['20', '19.8', '16', '15.8'])
        # Tagged properties
        print coad['Generator']['118-1'].get_properties()
        self.assertEqual(coad['Generator']['118-1'].get_properties()['Scenario.RT_UC'],{'Commit':'0'})

    def test_single_properties(self):
        '''Tests related to properties with a single value
        '''
        filename='coad/test/118-Bus.xml'
        coad=COAD(filename)
        # Get properties
        line = coad['Line']['126']
        props = {'Reactance': '0.0202', 'Max Flow': '9900', 'Min Flow': '-9900', 'Resistance': '0.00175'}
        self.assertEqual(line.get_properties()['System.System'], props)
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
        self.assertEqual(line.get_properties()['System.System'], props)
        line_a = coad['Line']['027']
        self.assertEqual(line_a.get_properties()['System.System'], new_props)

    def test_multi_properties(self):
        '''Tests related to properties with a list of values
        '''
        filename='coad/test/RTS-96.xml'
        coad=COAD(filename)
        # Get properties
        g = coad['Generator']['101-1']
        props = {'Mean Time to Repair': '50',
                 'Load Point': ['20', '19.8', '16', '15.8'],
                 'Heat Rate': ['15063', '14499', '14500', '15000'],
                 'Min Up Time': '1',
                 'Max Ramp Up': '3',
                 'Min Down Time': '1',
                 'Min Stable Level': '15.8',
                 'Units': '1',
                 'Start Cost Time': ['0', '1'],
                 'Maintenance Frequency': '2',
                 'Maintenance Rate': '3.84',
                 'Max Capacity': '20',
                 'Forced Outage Rate': '10'}
        self.assertEqual(g.get_properties()['System.System'], props)
        # Get property
        self.assertEqual(g.get_property('Load Point'), ['20', '19.8', '16', '15.8'])
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
        self.assertEqual(g.get_properties()['System.System'], props)
        g2 = saved_coad['Generator']['123-3']
        self.assertEqual(g2.get_property('Maintenance Rate'), new_props['Maintenance Rate'])
        self.assertEqual(g2.get_property('Heat Rate'), new_props['Heat Rate'])

    def test_modify_single_tagged_properties(self):
        '''Tests related to modifying tagged properties with a single value
        '''
        filename = 'coad/test/RTS-96.xml'
        coad = COAD(filename)
        self.assertEqual(coad['Generator']['118-1'].get_properties()['Scenario.RT_UC'],{'Commit':'0'})
        coad['Generator']['118-1'].set_property('Commit', 'totally_committed', 'Scenario.RT_UC')
        # Test that dynamic flag is not set for Unit Commitment Optimality
        self.assertEqual(coad['Generator'].valid_properties['System']['12']['is_dynamic'],'false')
        self.assertRaises(Exception, coad['Generator']['118-1'].set_property, 'Unit Commitment Optimality', 'I hate input masks', 'Scenario.RT_UC')
        # Make sure invalid assignment does not trigger is_dynamic=true
        self.assertEqual(coad['Generator'].valid_properties['System']['12']['is_dynamic'],'false')
        # This tag isn't set at all
        coad['Generator']['118-1'].set_property('Unit Commitment Optimality', 'Rounded Relaxation', 'Scenario.RT_UC')
        self.assertEqual(coad['Generator'].valid_properties['System']['12']['is_dynamic'],'true')
        coad.save('coad/test/RTS-96_props_test.xml')
        saved_coad = COAD('coad/test/RTS-96_props_test.xml')
        expected = {'Commit':'totally_committed', 'Unit Commitment Optimality':'Rounded Relaxation'}
        self.assertEqual(saved_coad['Generator']['118-1'].get_properties()['Scenario.RT_UC'], expected)

    def test_get_text(self):
        '''Get text values for Data File objects
        '''
        filename = 'coad/test/RTS-96.xml'
        coad = COAD(filename)
        expected = {u'Scenario.4HA_UC': {u'Filename': u'\\Model a_DA_Base Solution\\interval\\ST Generator(*).Units Generating.csv'}}
        result = coad['Data File']['4HA_UC'].get_text()
        self.assertEqual(result, expected)

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

    def test_add_set_category(self):
        '''Test category creation for class and set for object
        '''
        copy_coad = COAD('coad/master.xml')
        copy_coad['Performance'].add_category("Test Category")
        new_cat = copy_coad['Performance'].get_categories()[1]
        self.assertEqual("Test Category", new_cat['name'])
        self.assertEqual("1", new_cat['rank'])
        copy_coad['Performance']['Gurobi'].set_category("Test Category")
        self.assertEqual("Test Category", copy_coad['Performance']['Gurobi'].get_category())
