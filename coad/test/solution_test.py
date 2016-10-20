import copy
from nose.plugins.skip import SkipTest
from coad.solution import PlexosSolution, compress_interval_py
from coad.test.array_data import merged_data, merged_data_results
import unittest

class TestPlexosSolution(unittest.TestCase):

    def test_load(self):
        ps = PlexosSolution('coad/sample_solution.xml')
        # TODO: Assert something

    def test_compression_py(self):
        self.assertEqual(compress_interval_py(copy.deepcopy(merged_data)), merged_data_results)

    def test_compression_c(self):
        try:
            from compress_interval import compress_interval
            self.assertEqual(compress_interval(copy.deepcopy(merged_data)), merged_data_results)
        except:
            raise SkipTest("Unable to import optimized cython code for compressing interval data")
