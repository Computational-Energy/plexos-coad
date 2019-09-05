from coad.COAD import COAD
from coad.ModelUtil import split_horizon, set_solver, plex_to_datetime, datetime_to_plex
import datetime
import unittest

class TestModelUtil(unittest.TestCase):
    #def setUp(self):
        #filename='master.xml'
        #filename='test/118-Bus.xml'
        #filename='test/Solar33P.db'
        #filename='coad/test/Solar33P.xml'
        #filename='test/WFIP-MISO.xml'
        #filename='test/WWSIS.xml'
        #coad=COAD(filename)

    def test_split_horizon(self):
        coad = COAD('coad/test/Solar33P.xml')
        split_horizon(coad,'M01 Cost Need 0603',8,0,True)
        test_no_offset = 'M01 Cost Need 0603_008P_OLd000_007'
        self.assertIn(test_no_offset,coad['Model'].keys())
        self.assertIn(coad['Horizon'][test_no_offset],coad['Model'][test_no_offset].get_children())
        self.assertEqual('43843.0',coad['Horizon'][test_no_offset]['Chrono Date From'])
        self.assertEqual('2.0',coad['Horizon'][test_no_offset]['Chrono Step Count'])
        split_horizon(coad,'M01 Cost Need 0603',8,1,True)
        test_day_offset = 'M01 Cost Need 0603_008P_OLd001_006'
        self.assertIn(test_no_offset,coad['Model'].keys())
        self.assertIn(coad['Horizon'][test_day_offset],coad['Model'][test_day_offset].get_children())
        self.assertEqual('43840.0',coad['Horizon'][test_day_offset]['Chrono Date From'])
        self.assertEqual('3.0',coad['Horizon'][test_day_offset]['Chrono Step Count'])
        self.assertIn(coad['Model'][test_no_offset], coad['System']['WECC'].get_children('Model'))
        self.assertIn(coad['Horizon'][test_no_offset], coad['System']['WECC'].get_children('Horizon'))

    def test_set_solver(self):
        coad = COAD('coad/test/Solar33P.xml')
        set_solver(coad,'PP1 Xpress-MP')
        self.assertEqual(coad['Performance']['PP1 Xpress-MP'],coad['Model']['M01 Cost Need 0603'].get_children('Performance')[0])

    def test_datetime_conv(self):
        ''' Test conversion between plexos time and datetime
        '''
        self.assertEqual(plex_to_datetime(45487), datetime.datetime(2024, 7, 14, 0, 0))
        self.assertEqual(datetime_to_plex(datetime.datetime(2024, 7, 14, 0, 0)), 45487)

    def test_split_horizon_split_type(self):
        '''Test splitting a horizon on a different step type
        '''
        coad = COAD('coad/test/horizon_split_test.xml')
        split_horizon(coad, 'Base', 52, 0, split_type=2)
        test_name = 'Base_052P_OLd000_001'
        self.assertIn(test_name, coad['Model'].keys())
        # assertEqual will always return true with objects of the same type, even if they're different objects
        horizons = coad['Model'][test_name].get_children("Horizon")
        self.assertEqual(1, len(horizons))
        self.assertEqual(test_name, horizons[0].meta['name'])
        self.assertEqual('43831.0',coad['Horizon'][test_name]['Chrono Date From'])
        self.assertEqual('168.0',coad['Horizon'][test_name]['Chrono Step Count'])
        self.assertEqual('1',coad['Horizon'][test_name]['Chrono Step Type'])
        test_name = 'Base_052P_OLd000_025'
        horizons = coad['Model'][test_name].get_children("Horizon")
        self.assertEqual(1, len(horizons))
        self.assertEqual(test_name, horizons[0].meta['name'])
        self.assertEqual('43999.0',coad['Horizon'][test_name]['Chrono Date From'])
        self.assertEqual('168.0',coad['Horizon'][test_name]['Chrono Step Count'])
        self.assertEqual('1',coad['Horizon'][test_name]['Chrono Step Type'])
        test_name = 'Base_052P_OLd000_052'
        horizons = coad['Model'][test_name].get_children("Horizon")
        self.assertEqual(1, len(horizons))
        self.assertEqual(test_name, horizons[0].meta['name'])
        self.assertEqual('44188.0',coad['Horizon'][test_name]['Chrono Date From'])
        self.assertEqual('216.0',coad['Horizon'][test_name]['Chrono Step Count'])
        self.assertEqual('1',coad['Horizon'][test_name]['Chrono Step Type'])
        # Make sure the original model only has the one horizon
        horizons = coad['Model']['Base'].get_children("Horizon")
        self.assertEqual(1, len(horizons))
        self.assertEqual('Base', horizons[0].meta['name'])
