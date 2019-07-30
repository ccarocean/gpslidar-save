import datetime as dt
import numpy as np
from scipy import stats
import os


def six_min(t0, loc):
    t_prev = t0 - dt.timedelta(days=1)

    y_prev = t_prev.year
    m_prev = t_prev.month
    day_prev = t_prev.day

    y = t0.year
    m = t0.month
    day = t0.day

    bias = 0  # TODO: What should bias be
    t0 = dt.datetime(y, m, day)

    f_prev_raw = f'/srv/data/gpslidar/{loc}/lidar/{y_prev}-{m_prev:>02d}-{day_prev:>02d}.txt'
    f_raw = f'/srv/data/gpslidar/{loc}/lidar/{y}-{m:>02d}-{day:>02d}.txt'
    f_6min = f'/srv/data/gpslidar/{loc}/lidar_sixmin/{y}-{m:>02d}-{day:>02d}.txt'

    t = []
    meas = []
    if os.path.isfile(f_prev_raw):
        with open(f_prev_raw, 'r') as f:
            for i in f.readlines():
                d = i.split()
                t_tmp = float(d[0])
                if t_tmp > (24*60*60 - 3*60):
                    meas.append(int(d[1]))
                    t.append(t_tmp - 24*60*60)

    with open(f_raw, 'r') as f:
        for i in f.readlines():
            d = i.split()
            meas.append(int(d[1]))
            t.append(float(d[0]))

    sixmin_timevec = [t0]
    while sixmin_timevec[-1] < (t0 + dt.timedelta(days=1) - dt.timedelta(minutes=6)):
        sixmin_timevec.append(sixmin_timevec[-1] + dt.timedelta(minutes=6))

    sixmin_timevec = np.array(sixmin_timevec)
    t = np.array(t)
    meas = np.array(meas)

    sixmin_measvec = []

    td_3m = 3*60
    with open(f_6min, 'w') as f:
        f.write(f'{"date": >10} {"time": >8} {"l_mean": >9} {"l_max": >5} {"l_min": >5} '
                f'{"l_median": >8} {"l_n": >6} {"l_skew": >8} {"l_std": >9} '
                f'{"l_Hs": >9} {"l": >9}\n')
        for i in sixmin_timevec:
            secs = (i - t0).total_seconds()
            data = meas[(t < (secs + td_3m)) & (t > (secs - td_3m))]
            if len(data) > 0:
                l_mean = float(np.nanmean(data))
                l_max = int(np.max(data))
                l_min = int(np.min(data))
                l_median = int(np.median(data))
                l_n = len(data)
                l_skew = stats.skew(data)
                l_std = np.std(data)
                l_Hs = 4 * l_std
                l = -l_mean + bias
                t_print = i.strftime("%Y-%m-%d %H:%M:%S")
                f.write(f'{t_print: >19s} '
                        f'{l_mean: >9.4f} '
                        f'{l_max: >5d} '
                        f'{l_min: >5d} '
                        f'{l_median: >8d} {l_n: >6d} {l_skew: >8.5f} {l_std: >9.5f} '
                        f'{l_Hs: >9.4f} {l: >9.4f}\n')

    with open(f_6min, 'a+') as f:
        for i, j in zip(sixmin_timevec, sixmin_measvec):
            f.write(f'{i} {j}\n')