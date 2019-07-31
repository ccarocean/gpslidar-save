import matplotlib.pyplot as plt
import datetime as dt
from .loading import load_lidar, load_gps
import os
import numpy as np


def plot_lidar(longname, save_dir, data_dir):
    data = []
    for root, dirs, files in os.walk(data_dir):
        for i in sorted(files):
            date = dt.datetime.strptime(i, '%Y-%m-%d.txt')
            for j in load_lidar(data_dir, date):
                data.append(j)

    td = dt.datetime.utcnow() - dt.timedelta(days=1)
    td = dt.datetime(td.year, td.month, td.day)

    time = np.array([i['time'] for i in data])
    height = np.array([float(i['l_mean']) for i in data])

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
    plt.title('All LIDAR Height Data from ' + longname)
    plt.xticks(rotation=90)
    plt.grid(b=True)
    plt.xlabel('Date')
    plt.ylabel('Distance from LIDAR (cm)')
    plt.savefig(os.path.join(save_dir, 'all_lidar.png'), bbox_inches='tight')
    plt.close()

    # Plot weeks height data
    plt.figure(figsize=(12, 10), dpi=80, facecolor='w')
    plt.plot(time_week, height_week, 'bo')
    plt.title('Week\'s LIDAR Height Data from ' +  longname)
    plt.xticks(rotation=90)
    plt.grid(b=True)
    plt.xlabel('Date')
    plt.ylabel('Distance from LIDAR (m)')
    plt.savefig(os.path.join(save_dir, 'week_lidar.png'), bbox_inches='tight')
    plt.close()

    # Plot yesterday's height data
    plt.figure(figsize=(12, 10), dpi=80, facecolor='w')
    plt.plot(time_today, height_today, 'bo')
    plt.title('Today\'s LIDAR Height Data from ' + longname)
    plt.xticks(rotation=90)
    plt.grid(b=True)
    plt.xlabel('Date')
    plt.ylabel('Distance from LIDAR (m)')
    plt.savefig(os.path.join(save_dir, 'day_lidar.png'), bbox_inches='tight')
    plt.close()


def plot_gps(longname, save_dir, data_dir):
    data = []
    for root, dirs, files in os.walk(data_dir):
        for i in sorted(files):
            for j in load_gps(i):
                data.append(j)

    time = np.array([i['time'] for i in data])
    lat = np.array([float(i['lat']) for i in data])
    lon = np.array([float(i['lon']) for i in data])
    alt = np.array([float(i['alt']) for i in data])

    # Plot lat, lon, and alt in subplots
    fig, axs = plt.subplots(3, 1, figsize=(12, 10), dpi=80, facecolor='w', sharex=True)
    axs[0].plot(time, lat, 'bo', markersize=3)
    axs[1].plot(time, lon, 'bo', markersize=3)
    axs[2].plot(time, alt, 'bo', markersize=3)
    plt.suptitle('All gps data from ' + longname)
    plt.xticks(rotation=90)
    plt.grid(b=True)
    axs[2].set_xlabel('Date')
    axs[0].set_ylabel(u'Latitude [\N{DEGREE SIGN}]')
    axs[1].set_ylabel(u'Longitude [\N{DEGREE SIGN}]')
    axs[2].set_ylabel('Altitude [m]')
    plt.savefig(os.path.join(save_dir, 'all_gps.png'), bbox_inches='tight')
    plt.close()
