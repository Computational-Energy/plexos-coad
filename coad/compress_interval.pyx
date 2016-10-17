def compress_interval(interval_data):
    """Python fallback for data compression of interval data
    """
    row  = interval_data.pop(0)
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
