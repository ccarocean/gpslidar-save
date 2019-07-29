import matplotlib.pyplot as plt
import datetime as dt
from .loading import load_lidar
import os
import numpy as np


def plot_lidar(loc, save_dir, data_dir):
    data = []
    for root, dirs, files in os.walk(data_dir):
        for i in sorted(files):
            date = dt.datetime.strptime(i, '%Y-%m-%d.txt')
            data.append(load_lidar(data_dir, date))

    td = dt.datetime.utcnow() - dt.timedelta(days=1)
    td = dt.datetime(td.year, td.month, td.day)

    time = np.array([i['time'] for i in data])
    height = np.array([i['l_mean'] for i in data])

    # Find Today's Data
    ind = np.where(time >= td)
    height_today = height[ind]
    time_today = time[ind]

    # Find Week Data
    timedelt7 = dt.timedelta(days=7)
    ind = np.where(time >= (td - timedelt7))
    height_week = height[ind]
    time_week = time[ind]

    # Plot all Height Data
    plt.figure(figsize=(12, 10), dpi=80, facecolor='w')
    plt.plot(time, height, 'bo', markersize=3)
    plt.title('All ' + loc + ' LIDAR Height Data')
    plt.xticks(rotation=90)
    plt.grid(b=True)
    plt.xlabel('Date')
    plt.ylabel('Distance from LIDAR (cm)')
    plt.savefig(os.path.join(save_dir, 'all_lidar.png'), bbox_inches='tight')

    # Plot weeks height data
    plt.figure(figsize=(12, 10), dpi=80, facecolor='w')
    plt.plot(time_week, height_week, 'bo')
    plt.title('Recent ' + loc + ' LIDAR Height Data')
    plt.xticks(rotation=90)
    plt.grid(b=True)
    plt.xlabel('Date')
    plt.ylabel('Distance from LIDAR (m)')
    plt.savefig(os.path.join(save_dir, 'week_lidar.png'), bbox_inches='tight')

    # Plot yesterday's height data
    plt.figure(figsize=(12, 10), dpi=80, facecolor='w')
    plt.plot(time_today, height_today, 'bo')
    plt.title('Today\'s ' + loc + ' LIDAR Height Data')
    plt.xticks(rotation=90)
    plt.grid(b=True)
    plt.xlabel('Date')
    plt.ylabel('Distance from LIDAR (m)')
    plt.savefig(os.path.join(save_dir, 'day_lidar.png'), bbox_inches='tight')