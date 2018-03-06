from coad.COAD import COAD
from coad.ModelUtil import split_horizon, set_solver, get_horizon_slice, plex_to_datetime, datetime_to_plex
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

    def test_get_horizon_slice(self):
        coad = COAD('coad/test/Solar33P.xml')
        test_no_offset = get_horizon_slice(coad,'M01 Cost Need 0603',8,7)
        self.assertEqual(43843.0,test_no_offset['Chrono Date From'])
        self.assertEqual(2.0,test_no_offset['Chrono Step Count'])
        test_offset = get_horizon_slice(coad,'M01 Cost Need 0603',8,6,1)
        self.assertEqual(43840.0,test_offset['Chrono Date From'])
        self.assertEqual(3.0,test_offset['Chrono Step Count'])

    def test_set_solver(self):
        coad = COAD('coad/test/Solar33P.xml')
        set_solver(coad,'PP1 Xpress-MP')
        self.assertEqual(coad['Performance']['PP1 Xpress-MP'],coad['Model']['M01 Cost Need 0603'].get_children('Performance')[0])

    def test_datetime_conv(self):
        ''' Test conversion between plexos time and datetime
        '''
        self.assertEqual(plex_to_datetime(45487), datetime.datetime(2024, 7, 14, 0, 0))
        self.assertEqual(datetime_to_plex(datetime.datetime(2024, 7, 14, 0, 0)), 45487)
