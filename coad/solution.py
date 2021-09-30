'''Process Plexos output into sqlite database.  Conversion of the R utility.
'''
import argparse
import logging
import os
import re
import sqlite3 as sql
import struct
from zipfile import ZipFile

import pandas as pd
from . import plexos_database

# Some mappings for plexos
TIMES = ("interval", "day", "week", "month", "year")

def clean_string(line):
    '''Remove invalid table name characters from a string'''
    return line.translate(dict.fromkeys(map(ord, " |&|'|-|\\."), None))

def compress_interval_py(interval_data):
    """Python fallback for data compression of interval data
    """
    row = interval_data.pop(0)
    final_data = [[row[0], 1, row[1], row[2]]]
    last_value = row[2]
    last_intid = row[1]
    for row in interval_data:
        if row[2] != last_value:
            final_data[-1][2] = last_intid
            last_value = row[2]
            final_data.append([row[0], row[1], row[1], last_value])
        last_intid = row[1]
    final_data[-1][2] = last_intid
    return final_data

class PlexosSolution(object):
    """Load Plexos solution XML files into a SQLite database using best
        guesses at tables, data types and relationships.
    """
    def __init__(self, zipfilename):
        """Eventually, check for proper zipfile - do nothing for now.

        #<SolutionDataset xmlns="http://tempuri.org/SolutionDataset.xsd">
        self.namespace = "{http://tempuri.org/SolutionDataset.xsd}"
        self.logger = logging.getLogger(__name__)
        # TODO: Remove if not needed
        self.pk_exceptions = ['band'] # tables that don't adhere to PK standards

        try:
            with open(filename):
                pass
        except:
            self.logger.error('Unable to open %s', filename)
            raise
        if filename.endswith('.db'):
            self.dbfilename=filename
            self.dbcon = sql.connect(self.dbfilename)
        elif filename.endswith('.xml'):
            #TODO: Check for existing database for overwrites
            self.load(filename, create_db_file)
        else:
            raise Exception('Invalid filename suffix for %s'%filename)
        """
        self.logger = logging.getLogger(__name__)
        self.zipfilename = zipfilename

    def new_database(self, cur):
        """Copy functionality from process_solution.R

            Args: cur - sqlite cursor
        """
        # Rename the tables to the xml element name
        cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = [t[0] for t in cur.fetchall()]
        for tab in tables:
            if tab == plexos_database.META_TABLE:
                continue
            self.logger.info("Renaming table %s", tab)
            cur.execute("ALTER TABLE '%s' RENAME TO 't_%s';"%(tab, tab))
        for i in range(4):
            cmd = "CREATE TABLE t_data_%s (key_id integer, period_id integer, value double)"
            cur.execute(cmd%i)
            cmd = "CREATE TABLE IF NOT EXISTS t_phase_%s (interval_id integer, period_id integer)"
            cur.execute(cmd%i)
            cur.execute("CREATE INDEX t_phase_%s_int_lookup ON t_phase_%s (interval_id)"%(i, i))
        # Fix t_key_index types
        cmd = """CREATE TABLE IF NOT EXISTS t_key_index_tmp
                 (key_id integer, period_type_id integer, position long,
                  length integer, period_offset integer);"""
        cur.execute(cmd)
        cur.execute("INSERT INTO t_key_index_tmp SELECT * FROM t_key_index;")
        cur.execute("DROP TABLE t_key_index;")
        cur.execute("ALTER TABLE t_key_index_tmp RENAME TO t_key_index;")
        cur.execute("CREATE INDEX t_key_lookup ON t_key_index (key_id)")
        cur.execute("CREATE INDEX t_period_0_int_lookup ON t_period_0 (interval_id)")

    def add_extra_tables(self, tempdb):
        """More functionality from process_solution.R

            Args: tempdb - sqlite connection
        """
        self.logger.info("add_extra_tables(): Adding extra tables to the database")
        cur = tempdb.cursor()
        # View with class and class_group
        cur.execute("""CREATE VIEW temp_class AS
      SELECT tc.class_id class_id,
             tc.name class,
             tcg.name class_group
      FROM t_class tc
      LEFT JOIN t_class_group tcg
        ON tc.class_group_id = tcg.class_group_id;""")
        # View with object, category, class, class_group
        cur.execute("""CREATE VIEW temp_object AS
      SELECT o.object_id object_id,
             o.name name,
             cat.name category,
             c.class_group class_group,
             c.class class
      FROM t_object o
      JOIN temp_class c
        ON o.class_id = c.class_id
      JOIN t_category cat
        ON o.category_id = cat.category_id;""")
        # Create a new table making long version of property
        cur.execute("""CREATE TABLE temp_property AS
      SELECT p.property_id property_id,
             '0' period_type_id,
             p.name property,
             c.name collection,
             u.value unit
      FROM t_property p
      JOIN t_collection c
        ON c.collection_id = p.collection_id
     JOIN t_unit u
       ON u.unit_id = p.unit_id;""")
        cur.execute("""INSERT INTO temp_property
      SELECT p.property_id property_id,
             '1' period_type_id,
             p.summary_name property,
             c.name collection,
             u.value unit
      FROM t_property p
      JOIN t_collection c
        ON c.collection_id = p.collection_id
     JOIN t_unit u
       ON u.unit_id = p.summary_unit_id""")
        # View with memberships, collection, parent and child objects
        cur.execute("""CREATE VIEW temp_membership AS
      SELECT m.membership_id membership_id,
             m.parent_object_id parent_object_id,
             m.child_object_id child_object_id,
             c.name collection,
             p.name parent_name,
             p.class parent_class,
             p.class_group parent_group,
             p.category parent_category,
             ch.name child_name,
             ch.class child_class,
             ch.class_group child_group,
             ch.category child_category
      FROM t_membership m
      JOIN t_collection c
        ON c.collection_id = m.collection_id
      JOIN temp_object p
        ON p.object_id = m.parent_object_id
      JOIN temp_object ch
        ON ch.object_id = m.child_object_id;""")
        # Views to list zones
        cur.execute("""CREATE VIEW temp_zones_id AS
      SELECT child_object_id,
             min(parent_object_id) parent_object_id
      FROM temp_membership
      WHERE collection = 'Generators'
            AND parent_class = 'Region'
      GROUP BY child_object_id;""")
        cur.execute("""CREATE VIEW temp_zones AS
      SELECT a.child_object_id,
             b.name region,
             b.category zone
      FROM temp_zones_id a
      JOIN temp_object b
      WHERE a.parent_object_id = b.object_id;""")
        # Read key data and transform it
        key = pd.read_sql_query("""SELECT k.key_id key,
             m.child_name child_name,
             m.parent_name parent_name,
             m.child_class child_class,
             m.parent_class parent_class,
             m.child_group child_group,
             m.parent_group parent_group,
             m.child_category child_category,
             m.parent_category parent_category,
             m.collection child_collection,
             p.property property,
             p.unit unit,
             ts.name timeslice,
             k.band_id band,
             k.sample_id sample,
             k.period_type_id period_type_id,
             k.phase_id phase_id,
             z.region region,
             z.zone zone
      FROM t_key k
      JOIN temp_membership m
        ON m.membership_id = k.membership_id
      JOIN t_timeslice ts
        ON ts.timeslice_id = k.timeslice_id
      JOIN temp_property p
        ON p.property_id = k.property_id
           AND
           p.period_type_id = k.period_type_id
      LEFT OUTER JOIN temp_zones z
           on z.child_object_id = m.child_object_id;""", tempdb)
        # Fix zone
        key['zone'].fillna("", inplace=True)
        # Fix region
        key['region'].fillna("", inplace=True)
        # Create collection
        class_lambda = lambda x: x['child_class'] if x['parent_class'] == "System" \
                       else x['parent_class'] + "." + x['child_collection']
        key['collection'] = key.apply(class_lambda, axis=1)
        # Create table_name
        key['table_name'] = key.apply(lambda x: "data_interval_" +
                                      clean_string(x['collection']) + "_" +
                                      clean_string(x['property']), axis=1)
        # Swap sample int for string
        stats_map = {0:"Mean", -1:"StDev", -2:"Min", -3:"Max"}
        key.replace({'sample':stats_map}, inplace=True)
        # Write to new table
        key.rename(columns={'child_name':'name', 'parent_name':'parent',
                            'child_category':'category', 'child_class':'class',
                            'child_group':'class_group'}, inplace=True)
        key[['key', 'table_name', 'collection', 'property', 'unit', 'name',
             'parent', 'category', 'region', 'zone', 'class', 'class_group',
             'phase_id', 'period_type_id', 'timeslice', 'band',
             'sample']].to_sql('temp_key', tempdb, index=False)
        # Check that t_key and temp_key have the same number of rows
        cur.execute("select count(*) from t_key;")
        self.logger.info("   t_key has %d rows", cur.fetchone()[0])
        cur.execute("select count(*) from temp_key;")
        self.logger.info("temp_key has %d rows", cur.fetchone()[0])
        cur.execute("CREATE INDEX temp_key_lookup ON temp_key (key)")
        # Create tables to hold interval, day, week, month, and yearly timestamps
        for i in range(5):
            cmd = """CREATE TABLE temp_period_%s
                     (phase_id INT, period_id INT, interval_id INT, time real)"""
            cur.execute(cmd%i)
        # For each phase add corresponding values to the time tables
        column_times = ["day_id", "week_id", "month_id", "fiscal_year_id"]
        for phase in range(1, 5):
            # Join t_period_0 and t_phase
            cur.execute("""CREATE VIEW temp_phase_%s AS
                         SELECT p.*, ph.period_id, julianday(p.year || '-' || substr(0 || p.month_of_year, -2)
                         || '-' || substr(0 || p.day_of_month, -2) || 'T' || substr(p.datetime, -8)) AS correct_time
                         FROM t_period_0 p
                         INNER JOIN t_phase_%s ph
                         ON p.interval_id = ph.interval_id"""%(phase, phase))
            # Fix time stamps in t_period_0 (interval)
            cur.execute("""INSERT INTO temp_period_0
                        SELECT %s, period_id, interval_id, correct_time time
                        FROM temp_phase_%s"""%(phase, phase))
            # Fix time stamps in t_period_X (summary data)
            for idx, column in enumerate(column_times):
                cur.execute("""INSERT INTO temp_period_%s
                      SELECT %s, %s, min(interval_id), min(correct_time) time
                      FROM temp_phase_%s
                      GROUP BY %s"""%(idx + 1, phase, column, phase, column))
        # Perf gain
        cur.execute("CREATE INDEX period_0_lookup ON temp_period_0 (phase_id, period_id)")

    #@profile
    def process_solution(self):
        """Same as the function in process_solution.R

            Returns filename of final sqlite database
        """
        # Database name will match that of the zip file
        filename = self.zipfilename
        tempdbfilename = filename[:-4]+'-temp.db'
        try:
            os.remove(tempdbfilename)
        except OSError:
            pass
        finaldbfilename = filename[:-4]+'-rplexos.db'
        try:
            os.remove(finaldbfilename)
        except OSError:
            pass
        sol_zip = ZipFile(filename)
        sol_filelist = sol_zip.namelist()
        self.logger.info("Zip contains: %s", sol_filelist)
        model_xml = None
        data_files = []
        model_log = None
        # Needs ^Model.*xml$, ^t_data_[0-4].BIN$, and ^Model.*Log.*.txt$
        for filename in sol_filelist:
            if re.match("^Model.*xml$", filename):
                model_xml = filename
            elif re.match("^t_data_[0-4].BIN$", filename):
                data_files.append(filename)
            elif re.match("^Model.*Log.*.txt$", filename):
                model_log = filename
        self.logger.info("Zipfile contains files:")
        self.logger.info("    Model xml: %s", model_xml)
        self.logger.info("    Data files: %s", data_files)
        self.logger.info("    Model log: %s", model_log)
        if model_xml is None or len(data_files) == 0 or model_log is None:
            self.logger.error("Missing required files from zipfile.  Found:")
        tempdbcon = plexos_database.load(sol_zip.open(model_xml), tempdbfilename,
                                         remove_invalid_chars=True)
        tempdb = tempdbcon.cursor()
        self.new_database(tempdb)
        self.add_extra_tables(tempdbcon)
        tempdbcon.commit()
        tempdbcon.close()
        self.logger.info("Creating final database and adding basic data")
        finaldbcon = sql.connect(finaldbfilename)
        finaldb = finaldbcon.cursor()
        finaldb.execute("CREATE TABLE data_time (phase_id INT, interval INT, time TEXT)")
        finaldb.execute("""CREATE VIEW time AS
                       SELECT phase_id, interval, datetime(time) time
                       FROM data_time""")
        # Turn PRAGMA OFF
        finaldb.execute("PRAGMA synchronous = OFF")
        finaldb.execute("PRAGMA journal_mode = MEMORY")
        finaldb.execute("PRAGMA temp_store = MEMORY")
        # Attach temporary database to final database
        finaldb.execute("ATTACH '%s' AS tmp"%tempdbfilename)
        # Add config table
        finaldb.execute("CREATE TABLE config AS SELECT * FROM tmp.t_config")
        #tempdb.execute("INSERT INTO config VALUES ('rplexos', '%s')"%packageVersion("rplexos"))
        # Add time data
        finaldb.execute("""INSERT INTO data_time
                      SELECT phase_id, interval_id, time
                      FROM tmp.temp_period_0""")
        # Collate information to key (first period data, then summary data)
        finaldb.execute("""CREATE TABLE key (key INT PRIMARY KEY,
                                table_name TEXT,
                                collection TEXT,
                                property TEXT,
                                unit TEXT,
                                name TEXT,
                                parent TEXT,
                                category TEXT,
                                region TEXT,
                                zone TEXT,
                                class TEXT,
                                class_group TEXT,
                                phase_id INT,
                                period_type_id INT,
                                timeslice TEXT,
                                band INT,
                                sample TEXT)""")
        finaldb.execute("""INSERT INTO key SELECT * FROM tmp.temp_key""")
        # Define columns from the key table that go into the views
        key_list = ["key", "collection", "property",
                    "unit", "name", "parent", "category", "region",
                    "zone", "phase_id", "period_type_id", "timeslice",
                    "band", "sample"]
        view_k2 = ", ".join(["k." + x for x in key_list])
        self.logger.info("Creating data tables and views")
        for tim in TIMES[1:]:
            finaldb.execute("CREATE TABLE data_%s (key integer, time real, value double)"%tim)
            finaldb.execute("""CREATE VIEW %s AS
                    SELECT %s, datetime(d.time) AS time, d.value
                    FROM data_%s d NATURAL LEFT JOIN key k"""%(tim, view_k2, tim))
        # Create interval data tables and views
        finaldb.execute("""SELECT DISTINCT table_name FROM key WHERE period_type_id = 0""")
        for tab in [t[0] for t in finaldb.fetchall()]:
            cmd = "CREATE TABLE '%s' (key INT, time_from INT, time_to INT, value DOUBLE)"
            finaldb.execute(cmd%tab)
            view_name = tab.replace("data_interval_", "")
            finaldb.execute("""CREATE VIEW %s AS
                               SELECT %s, t1.time time_from, t2.time time_to, d.value
                               FROM %s d
                               NATURAL JOIN key k
                               JOIN time t1
                               ON t1.interval = d.time_from
                               AND t1.phase_id = k.phase_id
                               JOIN time t2
                               ON t2.interval = d.time_to
                               AND t2.phase_id = k.phase_id
                               WHERE k.table_name = '%s'"""%(view_name, view_k2, tab, tab))
        # Create table for list of properties
        finaldb.execute("""CREATE TABLE property AS
            SELECT DISTINCT class_group,
                          class,
                          collection,
                          property,
                          unit,
                          phase_id,
                          period_type_id AS is_summary,
                          table_name,
                          COUNT(DISTINCT band) AS count_band,
                          COUNT(DISTINCT sample) AS count_sample,
                          COUNT(DISTINCT timeslice) AS count_timeslice
            FROM key
            GROUP BY class_group, class, collection, property, unit, phase_id, period_type_id, table_name
            ORDER BY phase_id, period_type_id, class_group, class, collection, property""")
        # Get length and offset data
        finaldb.execute("""SELECT period_type_id,
               MAX(position / 8 + length),
               MAX(position / 8 + length - period_offset),
               SUM(length),
               SUM(length - period_offset)
               FROM tmp.t_key_index
               GROUP BY period_type_id;""")
        length_check = {}
        for row in finaldb.fetchall():
            length_check[row[0]] = {"JustLength":row[1], "JustLengthMinusOffset":row[2],
                                    "SumLength":row[3], "SumLengthMinusOffset":row[4]}
        try:
            # Cython check here
            from compress_interval import compress_interval
        except ImportError:
            self.logger.warn("Unable to import optimized cython code for compressing interval data")
            compress_interval = compress_interval_py

        # Add binary data
        for period in range(5):
            # Check if binary file exists, otherwise, skip this period
            period_name = TIMES[period]
            bin_name = "t_data_%s.BIN"%period
            if bin_name not in sol_filelist:
                continue
            bin_con = sol_zip.open(bin_name, "r")
            self.logger.info("Reading %s binary data", period_name)
            # Check if length in t_key_index is correct
            length = length_check[period]
            correct_length = length["JustLength"] == length["SumLength"] or \
                             length["JustLengthMinusOffset"] != length["SumLengthMinusOffset"]
            # Read t_key_index entries for period data
            cmd = """SELECT nk.[key], nk.phase_id, nk.table_name, tki.period_offset,
                            tki.length
                     FROM tmp.t_key_index tki
                     JOIN tmp.temp_key nk ON tki.key_id = nk.[key]
                     WHERE tki.period_type_id = %s
                     ORDER BY tki.position"""
            finaldb.execute(cmd%period)
            # All the data is inserted in one transaction
            # Read N rows from the query based on period
            num_rows = 1000
            if period == 0:
                num_rows = 1
            def chunks(line, num):
                '''Yield chunks of line with size num'''
                for i in range(0, len(line), num):
                    yield line[i:i+num]
            # SQLite3 module has inconsistent results for multiple cursors in nested loops
            allrows = finaldb.fetchall()
            num_read = 0
            # Iterate through the query results
            for trow in chunks(allrows, num_rows):
                # Fix length if necessary
                if correct_length is False:
                    self.logger.info("Correcting length for period %s", period)
                    for row in trow:
                        row[4] = row[4] - row[3]
                # Expand data
                finaldb.execute("""CREATE TABLE data_df
                    ("key" INTEGER,
                    "phase_id" INTEGER,
                    "period_id" INTEGER,
                    "value" REAL);
                    """)
                finaldb.execute("CREATE INDEX data_df_lookup ON data_df (phase_id, period_id)")
                cmd = """CREATE TEMP VIEW merged_data_df
                         AS SELECT d.key, t.time, t.interval_id, d.value FROM data_df d
                         INNER JOIN tmp.temp_period_%s t ON d.phase_id=t.phase_id
                            AND d.period_id=t.period_id
                        ORDER BY t.interval_id"""
                finaldb.execute(cmd%period)
                # This is expand_tkey.cpp
                out_key = []
                out_phase = []
                out_period = []
                for row in trow:
                    offset = int(row[3])
                    length = int(row[4])
                    out_key += [row[0]]*length
                    out_phase += [row[1]]*length
                    out_period += range(1 + offset, length + offset + 1)
                value_data = list(struct.unpack('<%dd'%len(out_period),
                                                bin_con.read(8*len(out_period))))
                num_read += len(value_data)
                finaldb.executemany("""INSERT INTO data_df (key, phase_id, period_id, value)
                    VALUES (?,?,?,?)""", zip(out_key, out_phase, out_period, value_data))
                # Add data to SQLite
                if period > 0:
                    finaldb.execute("""INSERT INTO data_%s (key, time, value)
                        SELECT key, time, value FROM merged_data_df"""%TIMES[period])
                else:
                    # Eliminate consecutive repeats
                    # TODO: This loop takes more time than any other part of the
                    # code.  Do not use iterrows(), it is extremely slow

                    # Use sql query into arrays
                    table_name = trow[0][2]
                    finaldb.execute("SELECT key, interval_id, value FROM merged_data_df")
                    final_data = compress_interval(finaldb.fetchall())
                    cmd = "INSERT INTO %s (key, time_from, time_to, value) VALUES(?, ?, ?, ?)"
                    finaldb.executemany(cmd%table_name, final_data)
                finaldb.execute("DROP VIEW IF EXISTS merged_data_df")
                finaldb.execute("DROP TABLE IF EXISTS data_df")
                finaldbcon.commit()
            self.logger.info("Read %s values", num_read)
        # Read Log file into memory
        self.logger.info("Reading log file %s", model_log)
        log_content = sol_zip.open(model_log).read()
        # Look for a phase in the log output and extract time (and infeasibilities)
        def get_time(pattern, txt, add_inf=False):
            """Find the block of logfile for pattern
                Returns:
            List of phase, time, rel_gap_perd, infeas with None for missing times
            """
            pat = pattern + ".*?(?:\n|\r\n)"
            if add_inf:
                pat = pat + ".*?Infeasibilities.*?(?:\n|\r\n)"
            chunk = re.search(pat, txt, re.S)
            ret_list = [pattern[:-len(" Completed")]]
            if chunk is None:
                ret_list += [None, None, None]
            else:
                for idx, point in enumerate([pattern, "Relative Gap", "Infeasibilities"]):
                    for line in chunk.group(0).split('\n'):
                        num = re.search(point+".*?([0-9].*)$", line)
                        if num is not None:
                            ret_list.append(num.group(1))
                            break
                    if len(ret_list) <= idx + 1:
                        ret_list.append(None)
            #self.logger.info(ret_list)
            return ret_list
        # Parse log file
        # Get summary for each step
        log_info = [get_time("Primary Compilation Completed", log_content),
                    get_time("Secondary Compilation Completed", log_content),
                    get_time("PASA Completed", log_content, True),
                    get_time("MT Schedule Completed", log_content, True),
                    get_time("ST Schedule Completed", log_content, True)]
        # Write log entries to database
        finaldb.execute("""CREATE TABLE log_info
                              ("phase" TEXT,
                                "time" TEXT,
                                "rel_gap_perc" TEXT,
                                "infeas" TEXT);""")
        finaldb.executemany("""INSERT INTO log_info (phase, time, rel_gap_perc, infeas)
            VALUES(?, ?, ?, ?)""", log_info)
        finaldbcon.commit()
        self.logger.info("Parsing step times from log file %s", model_log)
        step_list = []
        steps = re.findall("Completed .*? Step +[0-9]+ of [0-9]+.*?(?:\n|\r\n)", log_content, re.S)
        def get_sec(tstring):
            '''Return seconds represented by time string'''
            (hrs, mins, secs) = tstring.split(':')
            return 3600 * int(hrs) + 60 * int(mins) + float(secs)
        for step in steps:
            #self.logger.info("step %s", step)
            # Names left in to make it easier to match entries
            vals = re.match(r"Completed (?P<phase>.*) Step\s*(?P<step>[0-9]*) of\s*(?P<total_step>[0-9]*).*Time: (?P<time>\S*)\..* (?P<elapsed>\S*)", step.strip())
            val_list = list(vals.groups())
            val_list[3] = get_sec(val_list[3])
            val_list[4] = get_sec(val_list[4])
            step_list.append(val_list)
        # Write log entries to database
        finaldb.execute("""CREATE TABLE log_steps
                              ("phase" TEXT,
                                "step" INTEGER,
                                "total_step" INTEGER,
                                "time" REAL,
                                "elapsed" REAL);""")
        finaldb.executemany("""INSERT INTO log_steps (phase, step, total_step, time, elapsed)
            VALUES(?, ?, ?, ?, ?)""", step_list)
        finaldbcon.commit()
        return finaldbfilename


def main():
    """Create PlexosSolution object based on command line arguments and run
        process_solution"""
    parser = argparse.ArgumentParser(description="Process plexos output zipfile")
    parser.add_argument('-d', '--debug', action='store_true',
                        help='show detailed logs')
    parser.add_argument('zipfile', help='plexos output zipfile')
    args = parser.parse_args()
    if args.debug:
        logging.basicConfig(level=logging.INFO)
    else:
        logging.basicConfig()
    psol = PlexosSolution(args.zipfile)
    psol.process_solution()


if __name__ == '__main__':
    main()
