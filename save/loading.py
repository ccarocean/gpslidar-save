import os
import datetime as dt


def load_cata_coops(coops_dir, date):
    data = []
    firstline = True
    try:
        with open(os.path.join(coops_dir, 'cata', date.strftime('%Y-%m.csv')), 'r') as f:
            alldata = f.readlines()
            for line in alldata:
                if firstline:
                    firstline = False
                else:
                    d = line.split(',')
                    data.append({'time': dt.datetime.strptime(d[0], '%Y/%m/%d %H:%M'), 'A1': d[1], 'A1_t1': d[2],
                                 'A1_t2': d[3], 'B1': d[4], 'E1': d[5], 'F1': d[6], 'L1_1': d[7], 'L1_2': d[8],
                                 'U1': d[9], 'P6': d[10], 'W1': d[11]
                                 })
        return data
    except IOError:
        return None


def load_harv_coops(coops_dir, date):
    data = []
    firstline = True
    try:
        with open(os.path.join(coops_dir, 'harv', date.strftime('%Y-%m.csv')), 'r') as f:
            alldata = f.readlines()
            for line in alldata:
                if firstline:
                    firstline = False
                else:
                    d = line.split(',')
                    data.append({'time': dt.datetime.strptime(d[0], '%Y/%m/%d %H:%M'), 'D1': d[1], 'F1': d[2],
                                 'L1_1': d[3], 'L1_2': d[4], 'N1_1': d[5], 'N1_2': d[6], 'U1': d[7], 'Y1_1': d[8],
                                 'Y1_2': d[9], 'P6': d[10], 'W1': d[11]
                                 })
        return data
    except IOError:
        return None


def load_lidar(lidar_dir, loc, month):
    data = []
    firstline = True
    for root, dirs, files in os.walk(os.path.join(lidar_dir, loc)):
        for i in sorted(files):
            date = dt.datetime.strptime(i, '%Y-%m-%d.txt')
            if date.month == month.month and date.year == month.year:
                with open(os.path.join(lidar_dir, loc, i), 'r') as f:
                    alldata = f.readlines()
                    for line in alldata:
                        if firstline:
                            firstline = False
                        else:
                            d = line.split()
                            data.append({'time': dt.datetime.strptime(d[0] + ' ' + d[1], '%Y-%m-%d %H:%M:%S'),
                                         'l_mean': d[2], 'l_max': d[3], 'l_min': d[4], 'l_median': d[5], 'l_n': d[6],
                                         'l_skew': d[7], 'l_std': d[8], 'l_Hs': d[9], 'l': d[10]
                                         })
    return data
