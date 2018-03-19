"""Read in the plexos solution database and attempt simpler ways to access
the data
"""
import argparse
import collections
from datetime import datetime
import logging
import os
import re
import sqlite3 as sql
import struct
from zipfile import ZipFile

from pandas import DataFrame
from . import plexos_database
from ._compat import cmp

class PlexosOutput(collections.Mapping):
    """Expose the plexos output zipfile for limited queries
        Acts as a Mapping with the classnames as keys, with a PlexosClass as value
    """
    def __init__(self, filename):
        """Attempt to bind filename as sqlite database, unless the filename ends
            in .zip.  Then attempt to process the zip.
        """
        self.logger = logging.getLogger(__name__)
        self._store = dict()
        if filename.endswith(".zip"):
            self.process_zip(filename)
        else:
            self.dbcon = sql.connect(filename)
        self.populate_store()

    def populate_store(self):
        ''' Populate this map with class names and pointers to their classDict
            objects
            '''
        cur = self.dbcon.cursor()
        cur.execute("SELECT * FROM class")
        for row in cur.fetchall():
            c_meta = dict(zip([d[0] for d in cur.description], row))
            self._store[c_meta['name']] = PlexosClass(self.dbcon, c_meta)

    def process_zip(self, zipfilename):
        """Create sqlite db from xml file inside zip, read binary files into
            sqlite db, and set internal variable to database connection
        """
        #self.zipfilename = zipfilename
        dbfilename = zipfilename[:-4]+'.db'
        try:
            os.remove(dbfilename)
        except OSError:
            pass
        sol_zip = ZipFile(zipfilename)
        sol_filelist = sol_zip.namelist()
        self.logger.info("Zip contains: %s", sol_filelist)
        model_xml = None
        data_files = []
        model_log = None
        # Needs ^Model.*xml$, ^t_data_[0-4].BIN$, and ^Model.*Log.*.txt$
        for sol_f in sol_filelist:
            if re.match("^Model.*xml$", sol_f):
                model_xml = sol_f
            elif re.match("^t_data_[0-4].BIN$", sol_f):
                data_files.append(sol_f)
            elif re.match("^Model.*Log.*.txt$", sol_f):
                model_log = sol_f
        self.logger.info("Zipfile contains files:")
        self.logger.info("    Model xml: %s", model_xml)
        self.logger.info("    Data files: %s", data_files)
        self.logger.info("    Model log: %s", model_log)
        if model_xml is None or len(data_files) == 0 or model_log is None:
            self.logger.error("Missing required files from zipfile.  Found: %s",
                              sol_filelist)
            raise Exception("Invalid zipfile %s"%zipfilename)
        self.dbcon = plexos_database.load(sol_zip.open(model_xml), dbfilename,
                                          remove_invalid_chars=True)
        # Update database with the binary data
        cur = self.dbcon.cursor()
        for period in range(5):
            # Check if binary file exists, otherwise, skip this period
            bin_name = "t_data_%d.BIN"%period
            if bin_name not in sol_filelist:
                continue
            bin_file = sol_zip.open(bin_name, "r")
            self.logger.info("Reading period %d binary data", period)
            num_read = 0
            self.dbcon.execute("""CREATE TABLE data_%d
                ("key_id" INTEGER,
                "period_id" INTEGER,
                "value" REAL)"""%period)
            # TODO: Data position/length/offset error checking
            # Do not order by position, it was created as a string
            cur.execute('''SELECT key_id, length, position FROM key_index
                           WHERE period_type_id=?''', (period,))
            for row in cur.fetchall():
                length = int(row[1])
                value_data = list(struct.unpack('<%dd'%length, bin_file.read(8*length)))
                cmd = """INSERT INTO data_%d (key_id, period_id, value)
                        VALUES (?,?,?)"""%period
                self.dbcon.executemany(cmd, zip([row[0]]*length, range(1, length+1), value_data))
                num_read += length
            self.dbcon.commit()
            self.logger.info("Read %s values", num_read)
        # Create Time tables for each phase, needed as the period to
        # interval data sometimes comes out dirty
        cur.execute("SELECT phase_id FROM key GROUP BY phase_id")
        for (phase_id,) in cur.fetchall():
            self.logger.info("Creating time tables for phase %s", phase_id)
            cur.execute("""CREATE TABLE phase_time_%d
                        ("interval_id" INTEGER,
                         "period_id" INTEGER,
                         "datetime" TEXT)"""%phase_id)
            cur.execute("""INSERT INTO phase_time_%d
                SELECT min(p.interval_id), p.period_id, pe.datetime FROM phase_%d p
                INNER JOIN period_0 pe ON p.interval_id=pe.interval_id
                GROUP BY p.period_id"""%(phase_id, phase_id))
            self.dbcon.commit()

    def __getitem__(self, key):
        return self._store[key]

    def __iter__(self):
        return iter(self._store)

    def __len__(self):
        return len(self._store)

class PlexosClass(collections.Mapping):
    """Read-only representation of a Plexos Class from an output file

        _meta is a dictionary describing the class to match the
        database entry
    """
    def __init__(self, dbcon, meta):
        self._store = dict()
        self._dbcon = dbcon
        self._meta = meta
        self.logger = logging.getLogger(__name__)
        cur = self._dbcon.cursor()
        cur.execute("SELECT * FROM object WHERE class_id=?", [self._meta['class_id']])
        for row in cur.fetchall():
            obj = dict(zip([d[0] for d in cur.description], row))
            if obj['name'] in self._store:
                msg = 'Duplicate name of object %s in class %s'
                raise Exception(msg%(obj['name'], self._meta['name']))
            self._store[obj['name']] = PlexosObject(dbcon, obj)

    def get_data(self, property_name, object_names=None, period=0, phase=None):
        """Retrieve a set of data for all objects of the class as a pandas dataframe

        Args:
            property_name (String) property name
            object_names (list) list of object names to include in data set,
                defaults to None returning all objects in class
            period (int) select the period, defaults to 0 (interval data)
            phase (int) select the phase, defaults to None.  If data set has more
                than one phase this will raise an exception

        Returns:
            pandas dataframe with the datetime as an index and each object as a
            column

        Raises:
            Exception for times different between objects
        """
        data = {}
        times = None
        for (name, obj) in self.items():
            if object_names is not None and name not in object_names:
                continue
            #legend_name = "%s (%s)"%(name, obj.get_data_unit(property_name, period, phase))
            obj_data = obj.get_data_values(property_name, period, phase)
            if obj_data is None:
                continue
            data[name] = obj_data
            if times is None:
                times = obj.get_data_times(property_name, period, phase)
            elif cmp(times, obj.get_data_times(property_name, period, phase)) != 0:
                raise Exception("Object %s does not match time period already set"%name)
        return DataFrame(data=data, index=times)


    def get_unit(self, property_name, period=0, phase=None):
        """Retrieve the common unit for the property data of objects of this class

        Args:
            property_name (String) property name
            period (int) select the period, defaults to 0 (interval data)
            phase (int) select the phase, defaults to None.  If data set has more
                than one phase this will raise an exception

        Returns:
            string of unit name

        Raises:
            Exception for units different between objects
        """
        unit = None
        for (name, obj) in self.items():
            t_unit = obj.get_data_unit(property_name, period, phase)
            if t_unit is None:
                continue
            if unit is None:
                unit = t_unit
            if unit != t_unit:
                raise Exception("Unit %s for %s does not match other unit %s"%(t_unit, name, unit))
        return unit

    def get_property_names(self):
        """List all property names associated with this class
        """
        cur = self._dbcon.cursor()
        cur.execute("""SELECT p.name FROM property p
            INNER JOIN collection c ON c.collection_id=p.collection_id
            WHERE c.child_class_id=?""", (self._meta['class_id'],))
        prop_names = []
        for (name,) in cur.fetchall():
            prop_names.append(name)
        return prop_names

    def __getitem__(self, key):
        return self._store[key]

    def __iter__(self):
        return iter(self._store)

    def __len__(self):
        return len(self._store)

class PlexosObject(collections.Mapping):
    """Read-only representation of a Plexos Object from an output file.  Exposes
        methods for reading data related to the object.

        _meta is a dictionary describing the object as it is described in the
        database
    """
    def __init__(self, dbcon, meta):
        self._store = dict()
        self._dbcon = dbcon
        self._meta = meta
        self.logger = logging.getLogger(__name__)

    def get_property_keys(self):
        """Retrieve property name, key_id, period, phase for this object

            Returns: list of rows representing data
        """
        cur = self._dbcon.cursor()
        cur.execute("""SELECT p.name, k.key_id, ki.period_type_id, k.phase_id FROM key k
            INNER JOIN membership m ON m.membership_id=k.membership_id
            INNER JOIN property p ON p.property_id=k.property_id
            INNER JOIN key_index ki ON k.key_id=ki.key_id
            WHERE m.child_object_id=? """, (self._meta['object_id'],))
        return cur.fetchall()

    def get_key_id(self, property_name, period=0, phase=None):
        """Retrieve a key_id that matches this object, property name, period and
            phase
        Args:
            property_name (String) property name
            period (int) select the period, defaults to 0 (interval data)
            phase (int) select the phase, defaults to None.  If data set has more
                than one phase this will raise an exception

        Returns:
            list of floats in order of period_id

        Raises:
            Exception for multiple phases when no phase is selected
        """
        sel_txt = """SELECT k.key_id, k.phase_id FROM key k
            INNER JOIN membership m ON m.membership_id=k.membership_id
            INNER JOIN property p ON p.property_id=k.property_id
            INNER JOIN key_index ki ON k.key_id=ki.key_id
            WHERE m.child_object_id=? AND p.name=? AND ki.period_type_id=?"""
        sel_list = [self._meta['object_id'], property_name, period]
        if phase is not None:
            sel_txt += " AND k.phase_id=?"
            sel_list.append(phase)
        cur = self._dbcon.cursor()
        cur.execute(sel_txt, sel_list)
        key_ids = cur.fetchall()
        if len(key_ids) > 1:
            phases = [x[1] for x in key_ids]
            msg = "Multiple phases found for property %s, please select one phase: %s"
            raise Exception(msg%(property_name, phases))
            #raise Exception("Expected one key, got %d"%len(key_ids))
        elif len(key_ids) == 0:
            self.logger.info("No key id found for object %s, property %s, period %d, phase %d",
                             self._meta['name'], property_name, period, phase)
            return None
        key_id = key_ids[0][0]
        return key_id

    def get_data_values(self, property_name, period=0, phase=None):
        """Retrieve data values for property of object

        Args:
            property_name (String) property name
            period (int) select the period, defaults to 0 (interval data)
            phase (int) select the phase, defaults to None.  If data set has more
                than one phase this will raise an exception

        Returns:
            list of floats in order of period_id, None if no data exists
        """
        key_id = self.get_key_id(property_name, period, phase)
        if key_id is None:
            return None
        cur = self._dbcon.cursor()
        cur.execute("SELECT value FROM data_%d WHERE key_id=? ORDER BY period_id"%period,
                    (key_id,))
        #print("Key_id: %s   Type: %d   Size: %d"%(key_id, period_type_id, len(cur.fetchall())))
        #print("Type %d: %s"%(period_type_id, [x[0] for x in cur.fetchall()]))
        return [x[0] for x in cur.fetchall()]

    def get_data_unit(self, property_name, period=0, phase=None):
        """Retrieve data unit for property of object

        Args:
            property_name (String) property name
            period (int) select the period, defaults to 0 (interval data)
            phase (int) select the phase, defaults to None.  If data set has more
            than one phase this will raise an exception

        Returns:
            string of unit, None if no data exists
            """
        key_id = self.get_key_id(property_name, period, phase)
        if key_id is None:
            return None
        cur = self._dbcon.cursor()
        cur.execute("""SELECT u.value FROM unit u
                    INNER JOIN property p ON p.unit_id=u.unit_id
                    INNER JOIN key k ON k.property_id = p.property_id
                    WHERE key_id=?""", (key_id,))
        return cur.fetchone()[0]

    def get_data_times(self, property_name, period=0, phase=None):
        """Retrieve data times for property of object in order of period_id

        Args:
            property_name (String) property name
            period (int) select the period, defaults to 0 (interval data)
            phase (int) select the phase, defaults to None.  If data set has more
                than one phase this will raise an exception

        Returns:
            list of datetimes in order of period_id, None if no data exists
        """
        # TODO: Multiple phases
        key_id = self.get_key_id(property_name, period, phase)
        if key_id is None:
            return None
        cur = self._dbcon.cursor()
        cur.execute("""SELECT p.datetime FROM period_%d p
                    INNER JOIN data_%d d ON d.period_id=p.interval_id
                    WHERE d.key_id=? ORDER BY p.interval_id"""%(period, period), (key_id,))
        times = [datetime.strptime(x[0], "%d/%m/%Y %H:%M:%S") for x in cur.fetchall()]
        return times

    def __getitem__(self, key):
        return self._store[key]

    def __iter__(self):
        return iter(self._store)

    def __len__(self):
        return len(self._store)


def main():
    """Parse args and process solution zipfile
    """
    parser = argparse.ArgumentParser(description="Process plexos output zipfile")
    parser.add_argument('-d', '--debug', action='store_true',
                        help='show detailed logs')
    parser.add_argument('zipfile', help='plexos output zipfile')
    args = parser.parse_args()
    if args.debug:
        logging.basicConfig(level=logging.INFO)
    PlexosOutput(args.zipfile)

if __name__ == "__main__":
    main()
