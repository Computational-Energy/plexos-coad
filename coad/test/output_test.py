from datetime import datetime
import unittest
from output import PlexosOutput

class TestPlexosSolution(unittest.TestCase):

    def test_create(self):
        """Verify the zip file is processed properly
        """
        ps = PlexosOutput('test/mda_output.zip')
        # Was data loaded
        cur = ps['Line']._dbcon.cursor()
        cur.execute("SELECT count(*) FROM data_0")
        self.assertEqual(4128, cur.fetchone()[0])
        # Was the phase interval->period done correctly
        cur.execute("SELECT count(*) FROM phase_time_4")
        self.assertEqual(48, cur.fetchone()[0])

    def test_object_values(self):
        """Verify object values are available
        """
        ps = PlexosOutput('test/mda_output.zip')
        expected = [-0.935319116500001, -0.6970154267499986, -0.5217735017499989,
                    -0.41615258650000153, -0.3980630747500005, -0.46516376499999984,
                    -0.7597340485000006, -1.2800584555000007, -1.812169899250002,
                    -2.0393797997500016, -2.1432084820000004, -2.20546277575,
                    -2.2587450190000005, -2.15386336825, -2.0509797174999984,
                    -1.98446034625, -1.9687104047500001, -2.1013393862500007,
                    -2.4032077540000008, -2.3716624119999983, -2.0844381467499993,
                    -1.7796791724999996, -1.4374390120000011, -1.1613561009999995]
        self.assertEqual(expected, ps['Line']['B1_B2'].get_data_values('Flow'))

    def test_object_times(self):
        """Verify object times are available
        """
        ps = PlexosOutput('test/mda_output.zip')
        expected = [datetime(2020, 4, 16, x) for x in range(24)]
        self.assertEqual(expected, ps['Line']['B1_B2'].get_data_times('Flow'))

    def test_object_unit(self):
        """Verify object property units are available
        """
        ps = PlexosOutput('test/mda_output.zip')
        self.assertEqual("kV", ps['Node']["B0"].get_data_unit("Voltage"))

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
