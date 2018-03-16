'''Tools to parse Plexos solution zipfiles'''

import argparse
import logging
import os
import re
import struct
from zipfile import ZipFile

import h5py
from pandas import DataFrame
from . import plexos_database

def plexos2hdf5(zipfilename, h5filename=None, save_db=False):
    """Read in the plexos solution zipfile and save data to an hdf5 file

    Args:
        zipfilename - string name for zipfile
        h5filename - string name for h5file to write to, will be overwritten if
            exists.  If None, use the base zipfilename with hdf5 suffix
        save_db - bool keep sqlite database file created if true, otherwise
            delete at end of process

    Returns:
        h5py.File
    """
    dbfilename = zipfilename[:-4]+'.db'
    try:
        os.remove(dbfilename)
    except OSError:
        pass
    if h5filename is None:
        h5filename = zipfilename[:-4]+'.hdf5'
    try:
        os.remove(h5filename)
    except OSError:
        pass
    h5file = h5py.File(h5filename, 'w')
    sol_zip = ZipFile(zipfilename)
    sol_filelist = sol_zip.namelist()
    logging.info("Zip contains: %s", sol_filelist)
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
    logging.info("Zipfile contains files:")
    logging.info("    Model xml: %s", model_xml)
    logging.info("    Data files: %s", data_files)
    logging.info("    Model log: %s", model_log)
    if model_xml is None or len(data_files) == 0 or model_log is None:
        logging.error("Missing required files from zipfile.  Found: %s", sol_filelist)
        raise Exception("Invalid zipfile %s"%zipfilename)
    dbcon = plexos_database.load(sol_zip.open(model_xml), dbfilename, remove_invalid_chars=True)
    # Create hdf5 groups for each class and object
    cur = dbcon.cursor()
    cur.execute("SELECT * FROM class")
    for row in cur.fetchall():
        c_meta = dict(zip([d[0] for d in cur.description], row))
        cls_grp = h5file.create_group(c_meta['name'])
        for (name, val) in c_meta.items():
            cls_grp.attrs[name] = val
        cur.execute("SELECT * FROM object WHERE class_id=?", [c_meta['class_id']])
        for row in cur.fetchall():
            o_meta = dict(zip([d[0] for d in cur.description], row))
            obj_grp = cls_grp.create_group(o_meta['name'])
            for (name, val) in o_meta.items():
                obj_grp.attrs[name] = val
    # Add a view for better access to key mapping -> dset location
    cur.execute("""CREATE VIEW key_to_dataset AS
        SELECT k.key_id, c.name || '/' || o.name || '/' || p.name AS core_path,
        ki.period_type_id, k.phase_id,
        p.is_multi_band, k.band_id, u.value AS unit,
        par.name AS parent_name
        FROM key k
        INNER JOIN key_index ki ON k.key_id=ki.key_id
        INNER JOIN membership m ON m.membership_id=k.membership_id
        INNER JOIN property p ON p.property_id=k.property_id
        INNER JOIN object par ON par.object_id=m.parent_object_id
        INNER JOIN unit u ON p.unit_id=u.unit_id
        INNER JOIN object o ON m.child_object_id=o.object_id
        INNER JOIN class c ON m.child_class_id=c.class_id""")
    # Update database with the binary data
    for period in range(5):
        # Check if binary file exists, otherwise, skip this period
        bin_name = "t_data_%d.BIN"%period
        if bin_name not in sol_filelist:
            continue
        bin_file = sol_zip.open(bin_name, "r")
        logging.info("Reading period %d binary data", period)
        num_read = 0
        # TODO: Data position/length/offset error checking
        # Do not order by position, it was created as TEXT
        cmd = "SELECT key_id, length, position FROM key_index WHERE period_type_id=?"
        cur.execute(cmd, (period,))
        for row in cur.fetchall():
            length = int(row[1])
            value_data = list(struct.unpack('<%dd'%length, bin_file.read(8*length)))
            # Need property.name, object.name, class.name, phase_id
            cur.execute("""SELECT core_path, period_type_id, phase_id, is_multi_band,
                band_id, unit, parent_name FROM key_to_dataset
                WHERE key_id=? AND period_type_id=?""", (row[0], period))
            dataset = cur.fetchall()
            if len(dataset) != 1:
                logging.error("Multiple datasets returned for key %s", row[0])
                logging.error(dataset)
                continue
            (core_path, period_type_id, phase_id, is_multi_band, band_id, unit,
             parent_name) = dataset[0]
            dataset_path = core_path
            if parent_name != "System":
                # Something to allow this data to be set
                dataset_path = dataset_path + '/' + parent_name
            dataset_path = dataset_path + '/period_%d'%period_type_id + '/phase_%d'%phase_id
            if is_multi_band == "true":
                dataset_path = dataset_path + '/band_%d'%band_id
            logging.info("Creating key %s dataset_path %s", row[0], dataset_path)
            dset = h5file.create_dataset(dataset_path, data=value_data)
            dset.attrs["key_id"] = row[0]
            dset.attrs['unit'] = unit
            num_read += length
        #dbcon.commit()
        logging.info("Read %s values", num_read)
    # Add time lists for each period.  Each period has its own way of storing time
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' and name LIKE 'period_%'")
    period_list = [x[0] for x in cur.fetchall()]
    time_grp = h5file.create_group("times")
    if "period_0" in period_list:
        cur.execute("SELECT datetime FROM period_0 ORDER BY interval_id")
        time_grp.create_dataset("period_0", data=[x[0].encode('utf8') for x in cur.fetchall()])
    if "period_1" in period_list:
        cur.execute("SELECT date FROM period_1 ORDER BY day_id")
        time_grp.create_dataset("period_1", data=[x[0].encode('utf8') for x in cur.fetchall()])
    if "period_2" in period_list:
        logging.warn("Period 2 not implemented yet")
        # TODO: Find something with period 2
    if "period_3" in period_list:
        cur.execute("SELECT month_beginning FROM period_3 ORDER BY month_id")
        time_grp.create_dataset("period_3", data=[x[0].encode('utf8') for x in cur.fetchall()])
    if "period_4" in period_list:
        cur.execute("SELECT year_ending FROM period_4 ORDER BY fiscal_year_id")
        time_grp.create_dataset("period_4", data=[x[0].encode('utf8') for x in cur.fetchall()])
    # Create Time lists for each phase, needed as the period to
    # interval data sometimes comes out dirty
    cur.execute("SELECT phase_id FROM key GROUP BY phase_id")
    for (phase_id,) in cur.fetchall():
        logging.info("Creating time tables for phase %s", phase_id)
        cur.execute("""SELECT min(pe.datetime) FROM phase_%d p
            INNER JOIN period_0 pe ON p.interval_id=pe.interval_id
            GROUP BY p.period_id"""%(phase_id))
        dat = [x[0].encode('utf8') for x in cur.fetchall()]
        time_grp.create_dataset("phase_%d"%phase_id, data=dat)
    if not save_db:
        os.remove(dbfilename)
    h5file.close()
    h5file = h5py.File(h5filename, 'r')
    return h5file

def get_child_datasets(h5_group, dataset_list=None):
    """Recursively search h5_group for datasets, adding each path to the dataset_list

    Args:
        h5_group - h5 object to start search
        dataset_arr - list to store all datasets under h5_group

    Returns:
        List updated with datasets found
    """
    if dataset_list is None:
        dataset_list = []
    if isinstance(h5_group, h5py.Group) or isinstance(h5_group, h5py.File):
        for h5_obj in h5_group.values():
            dataset_list = get_child_datasets(h5_obj, dataset_list)
    elif isinstance(h5_group, h5py.Dataset):
        dataset_list.append(h5_group.name)
    return dataset_list

def get_dataframe(h5file, data_path):
    """Prepare a pandas dataframe of time vs data for data_path

    Args:
        h5file - h5py H5File object
        data_path - String path of dataset

    Returns:
        Pandas DataFrame of time series
    """
    dset = h5file[data_path]
    if not isinstance(dset, h5py.Dataset):
        raise Exception("Data path %s is not an hdf5 Dataset"%data_path)
    # Get time data
    # Time data is stored at /times/period_n or /times/phase_n for period_0
    # Data paths are stored so that the time period is the last /period_n
    periods = re.search(r"/period_(\d+)", data_path)
    if periods is None:
        raise Exception("No period in data path %s"%data_path)
    period_id = periods.groups()[-1]
    # Only period_0 has multiple phases supported at this time
    if period_id == '0':
        phases = re.search(r"/phase_(\d+)", data_path)
        if phases is None:
            msg = "No phase id in data path %s, using unaltered phase_0 time series"
            logging.warn(msg, data_path)
            time_path = "/times/period_0"
        else:
            time_path = "/times/phase_%s"%phases.groups()[-1]
    else:
        time_path = "/times/period_%s"%period_id
    # TODO: Use pandas read_hdf for efficiency
    time_data = h5file[time_path]
    # Trim time data for length of value data
    dset_len = len(dset)
    if len(time_data) != dset_len:
        time_data = time_data[0:dset_len]
    # Create dataset with time data as index
    dframe = DataFrame(data={data_path:dset}, index=time_data)
    return dframe

def main():
    """Parse args and process solution zipfile into hdf5
    """
    parser = argparse.ArgumentParser(description="Process plexos output zipfile into hdf5 file")
    parser.add_argument('-d', '--debug', action='store_true',
                        help='show detailed logs')
    parser.add_argument('-o', '--output', help='hdf5 filename to write')
    parser.add_argument('zipfile', help='plexos output zipfile')
    args = parser.parse_args()
    if args.debug:
        logging.basicConfig(level=logging.INFO)
    plexos2hdf5(args.zipfile, args.output, args.debug)

if __name__ == "__main__":
    main()
