from coad.COAD import COAD, ObjectDict
import unittest

# Only load the master once
master_coad = COAD('coad/master.xml')

class TestCOAD(unittest.TestCase):
    _multiprocess_can_split_=False

    def test_load(self):
        self.assertEqual(master_coad['Performance']['Gurobi']['SOLVER'],'4')

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
        self.assertEqual(expected, master_coad.keys())

    def test_objects(self):
        expected = [u'MOSEK', u'CPLEX', u'Xpress-MP', u'Gurobi']
        self.assertEqual(expected, master_coad['Performance'].keys())

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

    def test_get_children(self):
        should_contain = [self.coad['Horizon']['Base'],self.coad['Report']['Base'],self.coad['ST Schedule']['Base']]
        self.assertItemsEqual(should_contain,self.coad['Model']['Base'].get_children())
        self.assertItemsEqual([self.coad['Horizon']['Base']],self.coad['Model']['Base'].get_children('Horizon'))

    def test_set_children(self):
        # Single new child
        self.coad['Model']['Base'].set_children(self.coad['Performance']['Gurobi'])
        should_contain = [self.coad['Horizon']['Base'],self.coad['Report']['Base'],self.coad['ST Schedule']['Base'],self.coad['Performance']['Gurobi']]
        self.assertEqual(should_contain,self.coad['Model']['Base'].get_children())
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
        self.assertEqual(g.get_properties(), props)
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
        self.assertEqual(g.get_properties(), props)
        g2 = saved_coad['Generator']['123-3']
        self.assertEqual(g2.get_property('Maintenance Rate'), new_props['Maintenance Rate'])
        self.assertEqual(g2.get_property('Heat Rate'), new_props['Heat Rate'])

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
