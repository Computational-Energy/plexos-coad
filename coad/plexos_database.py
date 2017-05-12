"""Load and save Plexos XML files into a SQLite database using best
    guesses at tables, data types and relationships.
"""
import codecs
import logging
import os
has_resource = False
try:
    # unix-specific package
    import resource
    has_resource = True
except ImportError: pass
import sqlite3 as sql
import sys
import tempfile
import time
import xml.etree.cElementTree as etree

# A meta table is needed to record certain properties of the XML file that
# aren't part of the data stored within
META_TABLE = "plexos_meta"

# tables that don't adhere to PK standards
PK_EXCEPTIONS = ['band']

# Some common invalid chars found in the xml
INVALID_CHARS = ['&#x08;', '&#x8;']
LOGGER = logging.getLogger(__name__)

def load(source, dbfilename=None, create_db_file=True, remove_invalid_chars=False):
    """Load the xml file into a sqlite database
    Trust nothing, assume the worst on input by placing
    all table and column names in single quotes
    and feeding the text via parameterized SQL whenever possible

    Args:   source - Plexos XML filename or file-like object to load
            create_db_file - False will create an in-memory database
                             Defaults to True
            dbfilename - Optional filename to save the database.  If set to None,
                         will take the prefix of filename and append .db
            remove_invalid_chars - True will remove invalid xml 1.0 chars by
                    creating a new tempfile and read/writing

    Returns: sqlite db
    """
    try:
        xml_file = open(source)
        filename = source
    except TypeError:
        xml_file = source
        filename = xml_file.name
    start_time = time.time()
    if remove_invalid_chars:
        new_xml_file = tempfile.NamedTemporaryFile(delete=False)
        for line in xml_file:
            for badc in INVALID_CHARS:
                line = line.replace(badc, "")
            new_xml_file.write(line)
        new_xml_file.seek(0)
        xml_file = new_xml_file
        filename = xml_file.name
    if create_db_file:
        if dbfilename is None:
            dbfilename = filename[:-4]+'.db'
        # Remove existing outdb
        try:
            os.remove(dbfilename)
        except OSError:
            pass
    else:
        dbfilename = ':memory:'
    LOGGER.info('Loading %s into %s', filename, dbfilename)
    tables = {}
    nsl = 0
    namespace = None
    root_element = None
    t_check = ""
    row_count = 0
    dbcon = sql.connect(dbfilename)
    context = etree.iterparse(xml_file, events=('end', 'start-ns', 'start'))
    forkeys = [] # Foreign key list to add at the end of upload
    for action, elem in context:
        if action == 'start-ns':
            namespace = elem[1]
            LOGGER.info("Setting namespace to %s", namespace)
            nsl = len(namespace)+2
            t_check = "{"+namespace+"}t_"
            continue
        if action == 'start':
            if root_element is None:
                # This should be the first element in the xml file
                root_element = elem.tag[nsl:]
            continue
        if not elem.tag.startswith(t_check):
            continue
        table_name = elem.tag[nsl+2:]
        col_names = []
        col_values = []
        for el_data in elem.getchildren():
            col_names.append(el_data.tag[nsl:])
            col_values.append(el_data.text)
        # Check for new tables
        if table_name not in tables.keys():
            cols = []
            for col_name in col_names:
                if col_name.endswith('_id'):
                    cols.append("'%s' INTEGER"%col_name)
                    if col_name[:-3] == table_name and table_name not in PK_EXCEPTIONS:
                        cols[-1] += " PRIMARY KEY"
                    else:
                        forkeys.append((table_name, col_name))
                else:
                    cols.append("'%s' TEXT"%col_name)
            c_table = "CREATE TABLE '%s'(%s)"%(table_name, ','.join(cols))
            LOGGER.info(c_table)
            dbcon.execute("DROP TABLE IF EXISTS '%s';"%table_name)
            dbcon.execute(c_table)
            tables[table_name] = col_names
        # Check for new columns
        new_cols = set(col_names) - set(tables[table_name])
        # TODO make sure order isn't random on set diff
        for new_col in new_cols:
            m_table = "ALTER TABLE '%s' ADD COLUMN '%s' "%(table_name, new_col)
            if new_col.endswith('_id'):
                forkeys.append((table_name, new_col))
                LOGGER.info("New FK found %s", (table_name, new_col))
                m_table += 'INTEGER'
            else:
                m_table += 'TEXT'
            LOGGER.info(m_table)
            dbcon.execute(m_table)
            tables[table_name].append(new_col)
        cmd = 'INSERT INTO %s (%s) VALUES (%s)'
        i_row = cmd%(table_name, ','.join("'"+item+"'" for item in col_names),
                     ','.join('?'*len(col_values)))
        try:
            dbcon.execute(i_row, col_values)
        except:
            LOGGER.error('Problem loading row %s with data %s', i_row, col_values)
            raise
        row_count += 1
    forkeys_tables = {}
    for (orig_table, orig_col) in forkeys:
        other_table = orig_col[:-3]
        if other_table in tables:
            if orig_table not in forkeys_tables:
                forkeys_tables[orig_table] = []
            forkeys_tables[orig_table].append(orig_col)
        # The following would be the best way to do this, but sqlite doesn't
        # support adding FKs after table creation
        # ALTER TABLE %s ADD CONSTRAINT %s FOREIGN KEY ('%s') REFERENCES %s('%s')
        #    %(orig_table,'forkeys_'+orig_col,orig_col,other_table,orig_col))'''
    # Have to move table, create new table with FKs, copy old data, delete old table
    for (table_name, forkeys_cols) in forkeys_tables.items():
        dbcon.executescript("DROP TABLE IF EXISTS %s_todelete;"%table_name)
        dbcon.execute("ALTER TABLE %s RENAME TO %s_todelete"%(table_name, table_name))
        col_cmds = []
        for col_name in tables[table_name]:
            if col_name.endswith('_id'):
                col_cmd = "'%s' INTEGER"%col_name
                if col_name[:-3] == table_name and table_name not in PK_EXCEPTIONS:
                    col_cmd += " PRIMARY KEY"
                col_cmds.append(col_cmd)
            else:
                col_cmds.append("'%s' TEXT"%col_name)
        # Foreign key defs must be after all column definitions
        for col_name in forkeys_cols:
            cmd = "FOREIGN KEY ('%s') REFERENCES '%s'('%s')"%(col_name,
                                                              col_name[:-3],
                                                              col_name)
            col_cmds.append(cmd)
        dbcon.executescript("DROP TABLE IF EXISTS '%s';"%table_name)
        c_table = "CREATE TABLE '%s'(%s)"%(table_name, ','.join(col_cmds))
        dbcon.execute(c_table)
        dbcon.execute("INSERT INTO %s SELECT * FROM %s_todelete"%(table_name, table_name))
        dbcon.executescript("DROP TABLE IF EXISTS %s_todelete;"%table_name)
    # Create and populate meta data
    dbcon.execute("DROP TABLE IF EXISTS '%s';"%META_TABLE)
    c_meta = "CREATE TABLE '%s'('%s_id' INTEGER PRIMARY KEY,'name' TEXT,'value' TEXT);"
    dbcon.execute(c_meta%(META_TABLE, META_TABLE))
    meta_ins = "INSERT INTO '%s' ('name', 'value') VALUES (?, ?)"%META_TABLE
    dbcon.execute(meta_ins, ('namespace', namespace))
    dbcon.execute(meta_ins, ('root_element', root_element))
    # Indexes needed to speed up certain operations
    index_list = [('attribute', 'object_id'),
                  ('attribute_data', 'object_id'),
                  ('attribute_data', 'attribute_id'),
                  ('data', 'membership_id'),
                  ('data', 'property_id'),
                  ('data', 'uid'),
                  ('membership', 'parent_object_id'),
                  ('membership', 'child_object_id'),
                  ('object', 'object_id'),
                  ('property', 'property_id')]
    for (tablename, colname) in index_list:
        if tablename in tables and colname in tables[tablename]:
            dbcon.execute("CREATE INDEX %s_%s_idx ON %s (%s) "%(tablename, colname, tablename, colname))

    LOGGER.info('Loaded %s rows in %d seconds',row_count,(time.time()-start_time))
    if has_resource:
        LOGGER.info('Memory usage: %s',resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
    return dbcon

def save(dbcon, filename):
    ''' Write contents of plexos sqlite database to xml filename

        Args:   dbcon - sqlite database connection
                filename - Location to save plexos XML file.  The file will be
                           overwritten if it exists

        No Return
    '''
    # TODO: Check for overwrite existing xml
    # Get list of objects with objname
    dbcon.row_factory = sql.Row
    cur = dbcon.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = [t[0] for t in cur.fetchall()]
    with codecs.open(filename, "w", "utf-8-sig") as fout:
        # file writing in Python3 is different than 2, have to convert
        # strings to bytes or open the file with an encoding.  There is no
        # easy write for all data types
        plexos_meta = {}
        try:
            cur.execute("SELECT name, value FROM '%s'"%(META_TABLE))
        except sql.Error:
            LOGGER.warning("No metadata found in table %s", META_TABLE)
            plexos_meta['namespace'] = "http://tempuri.org/MasterDataSet.xsd"
            plexos_meta['root_element'] = "MasterDataSet"
        else:
            for row in cur.fetchall():
                plexos_meta[row[0]] = row[1]
        fout.write('<%s xmlns="%s">\r\n'%(plexos_meta['root_element'], plexos_meta['namespace']))
        for table_name in sorted(tables):
            if table_name == META_TABLE:
                continue
            try:
                cur.execute("SELECT * FROM '%s'"%(table_name))
            except sql.Error:
                LOGGER.warning("Bad table %s", table_name)
                continue
            row_keys = [k[0] for k in cur.description]
            #cElementTree has no pretty print, so some convolution is needed
            row = cur.fetchone()
            while row is not None:
                fout.write('  ')
                ele = etree.Element('t_' + table_name)
                for (sube, val) in zip(row_keys, row):
                    # Uncommenting the following will ignore subelements with no values
                    # Sometimes missing subelements with no values were crashing plexos.
                    # See issue #54
                    if val is None:
                      continue
                    attr_ele = etree.SubElement(ele, sube)
                    if isinstance(val, int):
                        val = str(val)
                    attr_ele.text = val
                ele_slist = etree.tostringlist(ele)
                # This is done because in python2, to_string prepends the string with an
                # xml declaration.  Also in python2, the base class of 'bytes' is basestring
                # TODO: Will this ever process an element with no data?
                if isinstance(ele_slist[0], str):
                    ele_s = "".join(ele_slist)
                else:
                    # Python3 bytes object
                    ele_s = ""
                    for byte_list in ele_slist:
                        ele_s += byte_list.decode('UTF-8')
                fout.write(ele_s.replace('><', '>\r\n    <').replace('  </t_', '</t_'))
                fout.write('\r\n')
                row = cur.fetchone()
        fout.write('</%s>\r\n'%plexos_meta['root_element'])


def main():
    logging.basicConfig(level=logging.INFO)
    load(sys.argv[1])


if __name__ == '__main__':
    main()
