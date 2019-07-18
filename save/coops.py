import datetime as dt
import os
from collections import defaultdict
from operator import itemgetter
from .loading import load_cata_coops, load_harv_coops, load_alllidar


def check_for_coops(data_dir, loc):
    for root, dirs, files in os.walk(os.path.join(data_dir, loc, 'co-ops')):
        for i in sorted(files):
            date = dt.datetime.strptime(i, '%Y-%m.csv')
            if not os.path.isfile(os.path.join(data_dir, loc, 'lidar_monthly', date.strftime('%Y-%m.txt'))):
                monthly_product(data_dir, loc, date)


def monthly_product(data_dir, loc, date):
    if loc == 'harv':
        coops_data = load_harv_coops(os.path.join(data_dir, loc, 'co-ops'), date)
        if coops_data is None:
            return None

        lidar_data = load_alllidar(os.path.join(data_dir, loc, 'lidar_sixmin'), date)

    elif loc == 'cata':
        coops_data = load_cata_coops(os.path.join(data_dir, loc, 'co-ops'), date)
        if coops_data is None:
            return None

        lidar_data = load_alllidar(os.path.join(data_dir, loc, 'co-ops'), date)

    else:
        return None

    d = defaultdict(dict)
    for l in (lidar_data, coops_data):
        for elem in l:
            d[elem['time']].update(elem)
    data = sorted(d.values(), key=itemgetter("index"))

    write_monthly(os.path.join(data_dir, loc, 'lidar_monthly'), data, date, loc)


def write_monthly(monthly_dir, data, month, loc):
    if loc == 'cata':
        with open(os.path.join(monthly_dir, month.strftime('%Y-%m.txt')), 'w') as f:
            f.write(f'{"date": >10} {"time": >8} {"l_mean": >9} {"l_max": >5} {"l_min": >5} {"l_median": >8} '
                    f'{"l_n": >6} {"l_skew": >8} {"l_std": >9} {"l_Hs": >9} {"l": >9} {"A1": >5} {"A1_t1": >4} '
                    f'{"A1_t2": >4} {"B1": >5} {"E1": >4} {"F1": >6} {"L1_1": >4} {"L1_2": >4} {"U1": >5} '
                    f'{"P6": >5} {"W1": >5}\n')
            for i in data:
                f.write(f'{i["date"]: >19s} {i["l_mean"]: >9.4f} {i["l_max"]: >5d} {i["l_min"]: >5d} '
                        f'{i["l_median"]: >8d} {i["l_n"]: >6d} {i["l_skew"]: >8.5f} {i["l_std"]: >9.5f} '
                        f'{i["l_Hs"]: >9.4f} {i["l"]: >9.4f} {i["A1"]: >5.3f} {i["A1_t1"]: >4.1f} {i["A1_t2"]: >4.1f} '
                        f'{i["B1"]: >5.3f} {i["E1"]: >4.1f} {i["F1"]: >6.1f} {i["L1_1"]: >4.1f} {i["L1_2"]: >4.1f} '
                        f'{i["U1"]: >5.3f} {i["P6"]: >5.3f} {i["W1"]: >5.3f}\n')

    elif loc == 'harv':
        with open(os.path.join(monthly_dir, month.strftime('%Y-%m.txt')), 'w') as f:
            f.write(f'{"date": >10} {"time": >8} {"l_mean": >9} {"l_max": >5} {"l_min": >5} {"l_median": >8} '
                    f'{"l_n": >6} {"l_skew": >8} {"l_std": >9} {"l_Hs": >9} {"l": >9} {"D1": >4} {"F1": >6} '
                    f'{"L1_1": >4} {"L1_2": >4} {"N1_1": >6} {"N1_2": >6} {"U1": >6} {"Y1_1": >6} {"Y1_2": >6} '
                    f'{"P6": >6} {"W1": >6}\n')
            for i in data:
                f.write(f'{i["date"]: >19s} {i["l_mean"]: >9.4f} {i["l_max"]: >5d} {i["l_min"]: >5d} '
                        f'{i["l_median"]: >8d} {i["l_n"]: >6d} {i["l_skew"]: >8.5f} {i["l_std"]: >9.5f} '
                        f'{i["l_Hs"]: >9.4f} {i["l"]: >9.4f} {i["D1"]: >4.1f} {i["F1"]: >6.1f} {i["L1_1"]: >4.1f} '
                        f'{i["L1_2"]: >4.1f} {i["N1_1"]: >6.3f} {i["N1_2"]: >6.3f} {i["U1"]: >6.3f} {i["Y1_1"]: >6.3f} '
                        f'{i["Y1_2"]: >6.3f} {i["P6"]: >5.3f} {i["W1"]: >5.3f}\n')
    else:
        return None
