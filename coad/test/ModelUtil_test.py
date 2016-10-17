from COAD import COAD
from ModelUtil import split_horizon, set_solver, get_horizon_slice
import unittest

class TestModelUtil(unittest.TestCase):
  def setUp(self):
    #filename='master.xml'
    #filename='test/118-Bus.xml'
    #filename='test/Solar33P.db'
    filename='test/Solar33P.xml'
    #filename='test/WFIP-MISO.xml'
    #filename='test/WWSIS.xml'
    self.coad=COAD(filename)

  def test_split_horizon(self):
    split_horizon(self.coad,'M01 Cost Need 0603',8,0,True)
    test_no_offset = 'M01 Cost Need 0603_008P_OLd000_007'
    self.assertIn(test_no_offset,self.coad['Model'].keys())
    self.assertIn(self.coad['Horizon'][test_no_offset],self.coad['Model'][test_no_offset].get_children())
    self.assertEqual(43843.0,self.coad['Horizon'][test_no_offset]['Chrono Date From'])
    self.assertEqual(2.0,self.coad['Horizon'][test_no_offset]['Chrono Step Count'])
    split_horizon(self.coad,'M01 Cost Need 0603',8,1,True)
    test_day_offset = 'M01 Cost Need 0603_008P_OLd001_006'
    self.assertIn(test_no_offset,self.coad['Model'].keys())
    self.assertIn(self.coad['Horizon'][test_day_offset],self.coad['Model'][test_day_offset].get_children())
    self.assertEqual(43840.0,self.coad['Horizon'][test_day_offset]['Chrono Date From'])
    self.assertEqual(3.0,self.coad['Horizon'][test_day_offset]['Chrono Step Count'])

  def test_get_horizon_slice(self):
    test_no_offset = get_horizon_slice(self.coad,'M01 Cost Need 0603',8,7)
    self.assertEqual(43843.0,test_no_offset['Chrono Date From'])
    self.assertEqual(2.0,test_no_offset['Chrono Step Count'])
    test_offset = get_horizon_slice(self.coad,'M01 Cost Need 0603',8,6,1)
    self.assertEqual(43840.0,test_offset['Chrono Date From'])
    self.assertEqual(3.0,test_offset['Chrono Step Count'])


  def test_set_solver(self):
    set_solver(self.coad,'PP1 Xpress-MP')
    self.assertEqual(self.coad['Performance']['PP1 Xpress-MP'],self.coad['Model']['M01 Cost Need 0603'].get_children('Performance')[0])
