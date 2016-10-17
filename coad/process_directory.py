"""Process an directory recursively for plexos outputs in .zip and inputs in
    .xml formats
"""
import argparse
import fnmatch
import logging
from multiprocessing import Pool
import os
from plexos_database import load
from solution import PlexosSolution

def process_file(filename):
    """Run either plexos_database or solution on the filename based on suffix
        Returns: location of sqlite3 database of processed filename or None
    """
    if filename.endswith(".xml"):
        # Input
        sqldb = load(filename)
        cur = sqldb.cursor()
        cur.execute("PRAGMA database_list")
        row = cur.fetchone()
        return row[2]
    elif filename.endswith(".zip"):
        # Output
        sol = PlexosSolution(filename)
        return sol.process_solution()

    else:
        logging.warning("Invalid suffix for file %s", filename)
        return None

def process_directory(directory):
    """Find all files ending in .zip or .zml recursively and run in parallel the
        input or output plexos processing.  Return map of filenames to result
        filenames
    """
    infiles = []
    outfiles = []
    for root, _, filenames in os.walk(directory):
        infiles += [os.path.join(root, x) for x in fnmatch.filter(filenames, '*.xml')]
        outfiles += [os.path.join(root, x) for x in fnmatch.filter(filenames, '*.zip')]
    logging.info("Infiles are %s", infiles)
    logging.info("Outfiles are %s", outfiles)
    pool = Pool() # Default pool will launch one worker per core
    #logging.info("Running on %s workers", cpu_count()) # cpu_count returns threads
    results = pool.map(process_file, infiles + outfiles)
    result_map = dict(zip(infiles+outfiles, results))
    return result_map

def main():
    """Parse arguments and run process_directory on the directory arg
    """
    desc = "Search directory for plexos inputs and outputs, processing as found"
    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument('-d', '--debug', action='store_true',
                        help='show detailed logs')
    parser.add_argument('directory', help='directory to search')
    args = parser.parse_args()
    if args.debug:
        logging.basicConfig(level=logging.INFO)
    result_map = process_directory(args.directory)
    logging.info("Results:")
    logging.info(result_map)

if __name__ == "__main__":
    main()
