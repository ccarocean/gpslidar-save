import datetime as dt
import os
import sys
from .output import RinexWrite


def save_lidar(data, data_directory, loc):
    """ Function for saving lidar data from API. """
    t0 = dt.datetime(1970, 1, 1) + dt.timedelta(seconds=data[0][1])
    t0_day = dt.datetime(t0.year, t0.month, t0.day)
    secs = (t0_day - dt.datetime(1970, 1, 1)).total_seconds()

    t = [i[1] - secs for i in data]
    meas = [i[2] for i in data]
    try:
        with open(os.path.join(data_directory, loc, 'lidar', t0_day.strftime('%Y-%m-%d.txt')), 'a+') as f:
            for i, j in zip(t, meas):
                f.write(f'{i} {j}\n')
    except FileNotFoundError:
        print("Data directory is bad. Try again. ")
        sys.exit(0)


def save_raw_gps(data, data_directory, loc, lat, lon, alt, longname):
    """ Function for saving raw GPS data to a file. """
    writer = RinexWrite(os.path.join(data_directory, loc, 'rawgps'), lat, lon, alt, data[0].week, data[0].rcvTow,
                        data[0].leapS, loc, longname)
    for i in data:
        writer.write_data(i)


def save_gps_pos(data, data_directory, loc):
    """ Function for saving the gps position data. """
    t0 = dt.datetime(1980, 1, 6)
    t = [t0 + dt.timedelta(days=7*i[2], microseconds=i[1]*1000) for i in data]
    lat = [i[4] for i in data]
    lon = [i[3] for i in data]
    height = [i[5] for i in data]
    f_name = os.path.join(data_directory, loc, 'position', t[0].strftime('%Y-%m-%d.txt'))
    try:
        with open(f_name, 'a+') as f:
            for i, j, k, l in zip(t, lat, lon, height):
                f.write(f'{i} {j} {k} {l}\n')  # Write to file
    except FileNotFoundError:
        print('Data directory is bad. Try again. ')
        sys.exit(0)
