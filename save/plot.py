import matplotlib.pyplot as plt
import matplotlib
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
    ind = np.where(time >= td & time <= (td + dt.timedelta(days=1)))
    height_today = height[ind]
    time_today = time[ind]

    # Find Week Data
    timedelt7 = dt.timedelta(days=7)
    ind = np.where(time >= (td - timedelt7) & time <= (td + dt.timedelta(days=1)))
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
    plt.title('Week\'s LIDAR Height Data from ' + longname)
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
            for j in load_gps(os.path.join(root, i)):
                data.append(j)

    time = np.array([i['time'] for i in data])
    lat = np.array([float(i['lat']) for i in data])
    lon = np.array([float(i['lon']) for i in data])
    alt = np.array([float(i['alt']) for i in data])

    latstd, latmean = np.std(lat), np.mean(lat)
    lonstd, lonmean = np.std(lon), np.mean(lon)
    altstd, altmean = np.std(alt), np.mean(alt)

    indlat = np.where(np.abs(lat - latmean) < 3*latstd)
    indlon = np.where(np.abs(lon - lonmean) < 3*lonstd)
    indalt = np.where(np.abs(alt - altmean) < 3*altstd)
    lat_no = lat[indlat]
    lon_no = lon[indlon]
    alt_no = alt[indalt]
    timelat = time[indlat]
    timelon = time[indlon]
    timealt = time[indalt]

    # Plot lat, lon, and alt in subplots
    fig, axs = plt.subplots(3, 1, figsize=(12, 10), dpi=80, facecolor='w', sharex=True)
    axs[0].plot(time, lat, 'bo', markersize=3)
    axs[1].plot(time, lon, 'bo', markersize=3)
    axs[2].plot(time, alt, 'bo', markersize=3)
    plt.suptitle('All GPS Data from ' + longname)
    plt.xticks(rotation=90)
    plt.grid(b=True)
    axs[2].set_xlabel('Date')
    axs[0].set_ylabel(u'Latitude [\N{DEGREE SIGN}]')
    axs[1].set_ylabel(u'Longitude [\N{DEGREE SIGN}]')
    axs[2].set_ylabel('Altitude [m]')
    y_formatter = matplotlib.ticker.ScalarFormatter(useOffset=False)
    axs[0].yaxis.set_major_formatter(y_formatter)
    axs[1].yaxis.set_major_formatter(y_formatter)
    axs[2].yaxis.set_major_formatter(y_formatter)
    plt.savefig(os.path.join(save_dir, 'all_gps.png'), bbox_inches='tight')
    plt.close()

    fig, axs = plt.subplots(3, 1, figsize=(12, 10), dpi=80, facecolor='w', sharex=True)
    axs[0].plot(timelat, lat_no, 'bo', markersize=3)
    axs[1].plot(timelon, lon_no, 'bo', markersize=3)
    axs[2].plot(timealt, alt_no, 'bo', markersize=3)
    plt.suptitle('All GPS Data Without Outliers from ' + longname)
    plt.xticks(rotation=90)
    plt.grid(b=True)
    axs[2].set_xlabel('Date')
    axs[0].set_ylabel(u'Latitude [\N{DEGREE SIGN}]')
    axs[1].set_ylabel(u'Longitude [\N{DEGREE SIGN}]')
    axs[2].set_ylabel('Altitude [m]')
    y_formatter = matplotlib.ticker.ScalarFormatter(useOffset=False)
    axs[0].yaxis.set_major_formatter(y_formatter)
    axs[1].yaxis.set_major_formatter(y_formatter)
    axs[2].yaxis.set_major_formatter(y_formatter)
    plt.savefig(os.path.join(save_dir, 'all_gps_nooutliers.png'), bbox_inches='tight')
    plt.close()
