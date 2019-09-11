''' Code relating to manipulation of Model data in PlexOS files
'''
import calendar
import datetime
import logging
import pandas
import sys

_logger = logging.getLogger(__name__)
# Taken from Plexos documentation
chrono_units_per_day = [1440.0, 24.0, 1.0, 1.0/7.0]

def plex_to_datetime(plex_date, datemode=0):
    '''Convert plexos date to datetime
    '''
    # datemode: 0 for 1900-based, 1 for 1904-based
    return datetime.datetime(1899, 12, 30) + datetime.timedelta(days=plex_date + 1462 * datemode)

def datetime_to_plex(dtime, datemode=0):
    '''Convert datetime to plexos date
    '''
    # datemode: 0 for 1900-based, 1 for 1904-based
    return (dtime - datetime.datetime(1899, 12, 30)).days - 1462 * datemode

def get_steps_per_day(horizon):
    '''Calculate how many steps per day for horizon

    Args: horizon - dict containing time data for horizon

    Returns: float of steps_per_day
    '''
    # Directly maps to "Step Type" attribute
    step_type = 2
    if "Chrono Step Type" in horizon:
        step_type = int(horizon["Chrono Step Type"])
    at_a_time = 1
    if "Chrono At a Time" in horizon:
        at_a_time = int(horizon["Chrono At a Time"])
    steps_per_day = float(chrono_units_per_day[step_type]) / float(at_a_time)
    return steps_per_day

def split_horizon(coad, model_name, num_partitions, start_day_overlap=0,
                  write_rindex_file=False, rindex_file=sys.stdout, split_type=None,
                  planning_horizon=None):
    ''' Split the horizons associated with model by creating new models and horizons for every split

    coad - COAD object
    model_name - name of the model to split horizons
    num_partitions - number of partitions, can handle 1000 max
    start_day_overlap - start horizon this many days before partition
    write_rindex_file - Whether or not to write the index file of partition information
    rindex_file - The file-like object to write the index file of parition information
    split_type - Split as a different step type to avoid small fractions in dates
    planning_horizon - True to set a monthly step planning horizon encompassing the split
    '''
    if num_partitions > 1000:
        raise Exception('Too many partitions: must be less than 1000')
    new_namelen = len(model_name) + 17
    if new_namelen > 50:
        raise Exception('Plexos will not support names greater than 50 characters.'
                        '  Try renaming the base model')
    if write_rindex_file:
        line = '{:^' + str(new_namelen) + 's},{:^19s},{:^19s}\n'
        rindex_file.write(line.format('New Model', 'Start', 'End'))
    model = coad["Model"][model_name]
    horizon = model.get_children('Horizon')[0]
    if split_type is None:
        step_count =  float(horizon['Chrono Step Count'])
        steps_per_day = get_steps_per_day(horizon)
    else:
        old_steps = float(horizon['Chrono Step Count'])
        old_timespan = old_steps / chrono_units_per_day[int(horizon['Chrono Step Type'])]
        _logger.info("Timespan is %s days", old_timespan)
        step_count = old_timespan * chrono_units_per_day[int(split_type)]
        _logger.info("Old step count is %s, new is %s", old_steps, step_count)
        step_ratio = chrono_units_per_day[int(horizon['Chrono Step Type'])]/chrono_units_per_day[int(split_type)]
        _logger.info("Step ratio is %s", step_ratio)
        horizon_data = {"Chrono Step Type":int(split_type)}
        # TODO: Not sure if this works correctly
        if "Chrono At a Time" in horizon:
            horizon_data["Chrono At a Time"] = int(horizon["Chrono At a Time"])
        steps_per_day = get_steps_per_day(horizon_data)

    for i in range(1, num_partitions+1):
        new_name = '%s_%03uP_OLd%03u_%03u' % (model_name, int(num_partitions),
                                              int(start_day_overlap), i)
        # New Model and Horizon have the same name
        new_model = model.copy(new_name)
        new_horizon = horizon.copy(new_name)
        # Set this horizon as the horizon member of the new model object
        new_model.set_children(new_horizon, replace=True)
        new_chrono_steps = float(round(step_count/float(num_partitions)))
        not_i = float(i != 1)
        horizon_idx = float(i - 1)
        steps_with_overlap = steps_per_day * float(start_day_overlap) * not_i
        if i < num_partitions:
            new_horizon['Chrono Step Count'] = new_chrono_steps + steps_with_overlap
        else:
            new_horizon['Chrono Step Count'] = step_count \
                                               - new_chrono_steps * horizon_idx \
                                               + steps_with_overlap
        new_horizon['Chrono Date From'] = float(horizon['Chrono Date From']) \
                                          + new_chrono_steps * horizon_idx / steps_per_day \
                                          - float(start_day_overlap) * not_i
        _logger.info("Split step count is %s", new_horizon['Chrono Step Count'])
        # Fix step counts for other type
        if split_type is not None:
            new_horizon['Chrono Step Count'] = new_horizon['Chrono Step Count'] * step_ratio
        if write_rindex_file:
            hor_start = new_horizon['Chrono Date From']
            hor_end = hor_start + new_horizon['Chrono Step Count']/steps_per_day
            rindex_file.write('%s,%s,%s\n'%(new_name, plex_to_datetime(hor_start),
                                            plex_to_datetime(hor_end)))
        if planning_horizon:
            set_planning_horizon(new_horizon, step_type=planning_horizon)
    # Clean up the additional children for the base model
    model.set_children(horizon, replace=True)

def set_solver(coad, solver_name):
    ''' Set the performance child of all models to solver_name
    '''
    solver = coad['Performance'][solver_name]
    for model in coad['Model'].values():
        model.set_children(solver)

def set_planning_horizon(horizon, step_type=3):
    ''' Set the planning horizon to encompass all months containing the ST Schedule
        Defaults to month planning (Step Type 3).  Planning step types:
        Day (value = 1)
        Week (value = 2) * TBD
        Month (value = 3)
        Year (value = 4) * TBD
    '''
    st_start = plex_to_datetime(float(horizon['Chrono Date From']))
    st_end = plex_to_datetime(float(horizon['Chrono Date From']) + float(horizon['Chrono Step Count'])/chrono_units_per_day[int(horizon['Chrono Step Type'])])
    if step_type == 1:
        date_from = horizon['Chrono Date From']
        step_count = (st_end - st_start).days + 1
    elif step_type == 3:
        # Get the start of the month containing Chrono Date From
        plan_start = datetime.datetime(st_start.year, st_start.month, 1)
        # Get the end of the month containing Chrono Date From + Chrono Step Count * Chrono Step Type
        #(wd, last_day) = calendar.monthrange(st_end.year, st_end.month)
        plan_end = datetime.datetime(st_end.year, st_end.month, 1)
        # Calculate number of months
        extra_year = st_end.year - st_start.year
        if extra_year:
            #subract months from year ends
            step_count = 13 - st_start.month + st_end.month
        else:
            step_count = st_end.month - st_start.month + 1
        # Set Date From
        date_from = datetime_to_plex(plan_start)

    else:
        raise Exception("Only daily(1) and monthly(3) step types supported")
    horizon['Date From'] = date_from
    horizon['Step Count'] = step_count
    horizon['Step Type'] = step_type

def show_data_files(coad):
    '''Show datafiles associated with a plexos input file.  Displays data in
    a table format with property, object, and collection information
    '''
    cmd = """SELECT p.name AS property_name, o.name AS object_name, c.name AS collection_name, t.value FROM text t
    INNER JOIN data d ON t.data_id=d.data_id
    INNER JOIN property p ON p.property_id=d.property_id
    INNER JOIN membership m ON d.membership_id=m.membership_id
    INNER JOIN collection c ON m.collection_id=c.collection_id
    INNER JOIN object o ON m.child_object_id=o.object_id
    """
    pandas.set_option('display.max_columns', None)
    pandas.set_option('display.max_colwidth', -1)
    df = pandas.read_sql(cmd, coad.dbcon)
    print(df)
