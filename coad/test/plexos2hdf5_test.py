from datetime import datetime
import os
import unittest
from coad.plexos2hdf5 import plexos2hdf5, get_child_datasets, get_dataframe

class TestPlexosSolution(unittest.TestCase):

    def test_process(self):
        """Verify the zip file is processed properly
        """
        # New h5file for each test, as a failure will break tests run afterwards
        h5filename = "coad/test/mda_output_process.hdf5"
        h5file = plexos2hdf5("coad/test/mda_output.zip", h5filename)
        # Was data loaded
        times = h5file['times/period_0']
        self.assertEqual("16/04/2020 00:00:00", times[0])
        self.assertEqual("17/04/2020 23:00:00", times[47])
        # Was the phase interval->period done correctly
        phase_times = h5file['times/phase_4']
        self.assertEqual("16/04/2020 00:00:00", phase_times[0])
        self.assertEqual("17/04/2020 23:00:00", phase_times[47])
        h5file.close()
        os.remove(h5filename)

    def test_object_values(self):
        """Verify object values are available
        """
        h5filename = "coad/test/mda_output_values.hdf5"
        h5file = plexos2hdf5("coad/test/mda_output.zip", h5filename)
        expected = [-0.935319116500001, -0.6970154267499986, -0.5217735017499989,
                    -0.41615258650000153, -0.3980630747500005, -0.46516376499999984,
                    -0.7597340485000006, -1.2800584555000007, -1.812169899250002,
                    -2.0393797997500016, -2.1432084820000004, -2.20546277575,
                    -2.2587450190000005, -2.15386336825, -2.0509797174999984,
                    -1.98446034625, -1.9687104047500001, -2.1013393862500007,
                    -2.4032077540000008, -2.3716624119999983, -2.0844381467499993,
                    -1.7796791724999996, -1.4374390120000011, -1.1613561009999995]
        self.assertEqual(expected, list(h5file["/Line/B1_B2/Flow/period_0/phase_4"]))
        h5file.close()
        os.remove(h5filename)

    def test_object_times(self):
        """Verify object times are available
        """
        h5filename = "coad/test/mda_output_times.hdf5"
        h5file = plexos2hdf5("coad/test/mda_output.zip", h5filename)
        expected = ["16/04/2020 %02d:00:00"%x for x in range(24)]
        # Phase 4 times span the entire range, although data is only output for
        # the first 24 items
        self.assertEqual(expected, list(h5file['times/phase_4'][0:24]))
        h5file.close()
        os.remove(h5filename)

    def test_object_unit(self):
        """Verify object property units are available
        """
        h5filename = "coad/test/mda_output_times.hdf5"
        h5file = plexos2hdf5("coad/test/mda_output.zip", h5filename)
        self.assertEqual("kV", h5file['/Node/B0/Voltage/period_0/phase_4'].attrs["unit"])
        h5file.close()
        os.remove(h5filename)

    def test_child_datasets(self):
        """Verify getting recursive lists of datasets
        """
        h5filename = "coad/test/mda_output_times.hdf5"
        h5file = plexos2hdf5("coad/test/mda_output.zip", h5filename)
        self.assertEqual(["/Node/B0/Voltage/period_0/phase_4",
                          "/Node/B0/Voltage/period_3/phase_4",
                          "/Node/B0/Voltage/period_4/phase_4"],
                         get_child_datasets(h5file["/Node/B0/Voltage/"]))
        h5file.close()
        os.remove(h5filename)

    def test_get_dataset(self):
        """Verify retrieval of time series datasets
        """
        h5filename = "coad/test/mda_output_times.hdf5"
        h5file = plexos2hdf5("coad/test/mda_output.zip", h5filename)
        dframe = get_dataframe(h5file, "/Line/B1_B2/Flow/period_0/phase_4")
        self.assertEqual(-0.935319116500001, dframe.loc["16/04/2020 00:00:00"]["/Line/B1_B2/Flow/period_0/phase_4"])
        h5file.close()
        os.remove(h5filename)


'''
    def test_class_data(self):
        """Verify class data is returned properly
        """
        ps = PlexosOutput('test/mda_output.zip')
        df = ps['Line'].get_data('Flow')
        dat = df.loc['2020-04-16 06:00:00']
        self.assertAlmostEqual(4.759734, dat['B0_B1'])
        self.assertEqual(4.0, dat['B0_B2'])
        self.assertAlmostEqual(-0.759734, dat['B1_B2'])

    def test_class_data_limited(self):
        """Verify class data with a subset of objects is returned properly
        """
        ps = PlexosOutput('test/mda_output.zip')
        df = ps['Line'].get_data('Flow', object_names=['B0_B1', 'B1_B2'])
        self.assertEqual(['B0_B1', 'B1_B2'], list(df.columns.values))

'''
