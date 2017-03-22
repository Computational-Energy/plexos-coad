"""Load and save Plexos XML files into a mongo database.
"""
import codecs
import logging
import os
import pymongo
has_resource = False
try:
    # unix-specific package
    import resource
    has_resource = True
except ImportError: pass
#import sqlite3 as sql
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

# Batch size to use when inserting documents
# For very large files (400MB) on a laptop
#    500 - 70 seconds
#   1000 - 70 seconds
#   5000 - 69 seconds
#  10000 - 70 seconds
BATCH_SIZE = 1000

def load(source, reset_db=True, host='localhost', port=27017, remove_invalid_chars=False):
    """Load the xml file into a mongo database named as the source file.

    Args:   source - Plexos XML filename
            reset_db - True will remove all existing values from database
                       Defaults to True
            host - Mongo hostname
            port - Mongo port
            remove_invalid_chars - True will remove invalid xml 1.0 chars by
                    creating a new tempfile and read/writing

    Returns: mongo db
    """
    try:
        xml_file = open(source)
        filename = source
    except TypeError:
        xml_file = source
        filename = xml_file.name
    dbname = os.path.basename(filename).translate(None, '.$')
    client = pymongo.MongoClient(host, port)
    db = client[dbname]
    if reset_db:
        for col in db.collection_names():
            db.drop_collection(col)
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
    LOGGER.info('Loading %s into mongo', filename)
    tables = {}
    nsl = 0
    namespace = None
    root_element = None
    t_check = ""
    doc_count = 0
    #dbcon = sql.connect(dbfilename)
    context = etree.iterparse(xml_file, events=('end', 'start-ns', 'start'))
    forkeys = [] # Foreign key list to add at the end of upload
    batch = [] # For batch insertion
    batch_collection = None
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
        collection_name = elem.tag[nsl+2:]
        if (collection_name != batch_collection and len(batch) > 0) or len(batch) > BATCH_SIZE:
            LOGGER.info("Loading %s documents to %s", len(batch), batch_collection)
            result = db[batch_collection].insert_many(batch)
            if result.acknowledged:
                doc_count += len(result.inserted_ids)
            else:
                LOGGER.error("Error adding document %s to %s", doc, collection_name)
            batch=[]
        batch_collection = collection_name
        # Create json document
        # Add document to collection
        doc = {}
        for el_data in elem.getchildren():
            doc[el_data.tag[nsl:]] = el_data.text
        batch.append(doc)
        #result = db[collection_name].insert_one(doc)
        #if result.acknowledged:
        #    doc_count += 1
        #else:
        #    LOGGER.error("Error adding document %s to %s", doc, collection_name)
    # Last batch
    if len(batch) > 0:
        result = db[batch_collection].insert_many(batch)
        if result.acknowledged:
            doc_count += len(result.inserted_ids)
        else:
            LOGGER.error("Error adding document %s to %s", doc, collection_name)
    LOGGER.info('Loaded %s documents in %d seconds',doc_count,(time.time()-start_time))
    # Indexes needed to speed up certain operations
    db['attribute'].create_index('object_id')
    db['attribute_data'].create_index('object_id')
    db['attribute_data'].create_index('attribute_id')
    db['data'].create_index('membership_id')
    db['data'].create_index('uid')
    db['membership'].create_index('parent_object_id')
    db['membership'].create_index('child_object_id')
    db['object'].create_index('object_id')
    if has_resource:
        LOGGER.info('Memory usage: %s',resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
    return db


def save(db, filename):
    ''' Write contents of plexos mongo database to xml filename

        Args:   db - mongo database
                filename - Location to save plexos XML file.  The file will be
                           overwritten if it exists

        No Return
    '''
    # TODO: Check for overwrite existing xml
    with codecs.open(filename, "w", "utf-8-sig") as fout:
        # file writing in Python3 is different than 2, have to convert
        # strings to bytes or open the file with an encoding.  There is no
        # easy write for all data types
        # TODO: Support all root_element and namespace defs
        fout.write('<%s xmlns="%s">\r\n'%("MasterDataSet", "http://tempuri.org/MasterDataSet.xsd"))
        for col in sorted(db.collection_names()):
            for doc in db[col].find({}, {'_id':0}):
                fout.write('  ')
                ele = etree.Element('t_' + col)
                for (sube, val) in doc.items():
                    # Uncommenting the following will ignore subelements with no values
                    # Sometimes missing subelements with no values were crashing plexos.
                    # See issue #54
                    #if val is None:
                    #  continue
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
        fout.write('</%s>\r\n'%"MasterDataSet")

def main():
    logging.basicConfig(level=logging.INFO)
    load(sys.argv[1])

if __name__ == '__main__':
    main()
