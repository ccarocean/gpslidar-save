import datetime as dt
import os
import numpy as np
from scipy import stats
from .loading import load_lidar, load_raw_lidar


def quadreg_lid(x, y, t, day):
    """ Function for performing quadratic regression on LiDAR data. """
    if len(x) > 0:
        try:
            xnum = (x - (t - day).total_seconds()) / 3600
        except TypeError:
            xnum = (x - t).total_seconds() / 3600
        p = np.polyfit(xnum, y, 2)
        b_0, b_1, b_2 = p[2], p[1], p[0]
        return b_0
    else:
        return float('nan')


def ovavg(data_dir, loc, ov_file):
    time = []
    with open(ov_file, 'r') as f:
        data = f.readlines()
        for line in data:
            d = line.split()
            time.append(dt.datetime.strptime(d[0] + ' ' + d[1], '%Y-%m-%d %H:%M:%S'))

    timedelt_2h = dt.timedelta(hours=2)
    timedelt_1100s = dt.timedelta(seconds=1100)

    outdata = []
    for i in time:
        day = dt.datetime(i.year, i.month, i.day)
        if (i - day).total_seconds() < (2 * 3600):
            data_6min = load_lidar(os.path.join(data_dir, loc, 'lidar_sixmin'), day - dt.timedelta(days=1))
            data_6min += load_lidar(os.path.join(data_dir, loc, 'lidar_sixmin'), day)
            data_raw = load_raw_lidar(os.path.join(data_dir, loc, 'lidar',
                                                   (day - dt.timedelta(days=1)).strftime('%Y-%m-%d.txt')))
            data_raw += load_raw_lidar(os.path.join(data_dir, loc, 'lidar', day.strftime('%Y-%m-%d.txt')))
        elif (i - day).total_seconds() > 22 * 3600:
            data_6min = load_lidar(os.path.join(data_dir, loc, 'lidar_sixmin'), day)
            data_6min += load_lidar(os.path.join(data_dir, loc, 'lidar_sixmin'), day + dt.timedelta(days=1))
            data_raw = load_raw_lidar(os.path.join(data_dir, loc, 'lidar', day.strftime('%Y-%m-%d.txt')))
            data_raw += load_raw_lidar(os.path.join(data_dir, loc, 'lidar',
                                                    (day + dt.timedelta(days=1)).strftime('%Y-%m-%d.txt')))
        else:
            data_6min = load_lidar(os.path.join(data_dir, loc, 'lidar_sixmin'), day)
            data_raw = load_raw_lidar(os.path.join(data_dir, loc, 'lidar', day.strftime('%Y-%m-%d.txt')))

        time_6min = np.array([j['time'] for j in data_6min])
        meas_6min = np.array([j['l_mean'] for j in data_6min])
        time_raw = np.array([j['time'] for j in data_raw])
        meas_raw = np.array([j['meas'] for j in data_raw])
        ind_2h_6min = np.where((time_6min < (i + timedelt_2h)) & (time_6min > (i - timedelt_2h)))[0]
        ind_2h_raw = np.where((time_raw < (i + timedelt_2h)) & (time_raw > (i - timedelt_2h)))[0]
        ind_1100s_raw = np.where((time_raw < (i + timedelt_1100s)) & (time_raw > (i - timedelt_1100s)))

        outdata.append({'time': i,
                        'l_6m_quad2h': quadreg_lid(time_6min[ind_2h_6min], meas_6min[ind_2h_6min], i, day),
                        'l_mean_1100s': np.mean(meas_raw[ind_1100s_raw]),
                        'l_quad2h': quadreg_lid(time_raw[ind_2h_raw], meas_raw[ind_2h_raw], i, day),
                        'l_max_1100s': np.max(meas_raw[ind_1100s_raw]),
                        'l_min_1100s': np.min(meas_raw[ind_1100s_raw]),
                        'l_median_1100s': np.median(meas_raw[ind_1100s_raw]),
                        'l_n_1100s': len(meas_raw[ind_1100s_raw]),
                        'l_skew_1100s': stats.skew(meas_raw[ind_1100s_raw]),
                        'l_std_1100s': np.std(meas_raw[ind_1100s_raw])
                        })

    with open(os.path.join(data_dir, loc, 'overflight_data.txt'), 'w') as f:
        f.write(f'{"date": >10} {"time": >8} {"l_mean_1100s": >12} {"l_max_1100s": >11} {"l_min_1100s": >11} '
                f'{"l_median_1100s": >14} {"l_n_1100s": >9} {"l_skew_1100s": >12} {"l_std_1100s": >11} '
                f'{"l_6m_quad2h": >11} {"l_quad2h": >9}\n')
        for i in outdata:
            f.write(f'{i["time"].strftime("%Y-%m-%d %H:%M:%S"): >19s} '
                    f'{i["l_mean_1100s"]: >12.4f} '
                    f'{i["l_max_1100s"]: >11d} '
                    f'{i["l_min_1100s"]: >11d} '
                    f'{i["l_median_1100s"]: >14d} '
                    f'{i["l_n_1100s"]: >9d} '
                    f'{i["l_skew_1100s"]: >12.5f} '
                    f'{i["l_std_1100s"]: >11.5f} '
                    f'{i["l_6m_quad2h"]: >11.4f} '
                    f'{i["l_quad2h"]: >9.4f}\n')
