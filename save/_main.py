import argparse
import sqlalchemy as db
import datetime as dt
import os
from .save import save_gps_pos, save_raw_gps, save_lidar
from .messages import RxmRawx


def main():
    # Parse arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--directory', type=str, default='/srv/data/gpslidar',
                        help='Directory for output data. Default is /srv/data/gpslidar')
    args = parser.parse_args()

    engine = db.create_engine(dname)
    meta = db.MetaData()
    connection = engine.connect()

    stations = db.Table('stations', meta, autoload=True, autoload_with=engine)
    lidar = db.Table('lidar', meta, autoload=True, autoload_with=engine)
    gps_raw = db.Table('gps_raw', meta, autoload=True, autoload_with=engine)
    gps_measurement = db.Table('gps_measurement', meta, autoload=True, autoload_with=engine)
    gps_position = db.Table('gps_position', meta, autoload=True, autoload_with=engine)

    # Stations
    stations_data = connection.execute(db.select([stations])).fetchall()

    now = dt.datetime.utcnow()
    today = dt.datetime(now.year, now.month, now.day)
    yesterday = today - dt.timedelta(days=1)
    unix_today = (today - dt.datetime(1970, 1, 1)).total_seconds()
    unix_yesterday = (yesterday - dt.datetime(1970, 1, 1)).total_seconds()
    yesterday_week = (yesterday - dt.datetime(1980, 1, 6)).total_seconds() / (3600 * 24) // 7
    yesterday_itow = (yesterday - dt.datetime(1980, 1, 6)).total_seconds() % (7 * 24 * 3600) * 1000
    today_itow = yesterday_itow + 24 * 3600 * 1000
    yesterday_rtow = (yesterday - dt.datetime(1980, 1, 6)).total_seconds() % (7 * 24 * 3600)
    today_rtow = yesterday_rtow + 24 * 3600

    for s in stations_data:
        # Make directories if they don't exist
        if not os.path.isdir(os.path.join(args.directory, s[1])):
            os.mkdir(os.path.join(args.directory, s[1]))
        if not os.path.isdir(os.path.join(args.directory, s[1], 'lidar')):
            os.mkdir(os.path.join(args.directory, s[1], 'lidar'))
        if not os.path.isdir(os.path.join(args.directory, s[1], 'position')):
            os.mkdir(os.path.join(args.directory, s[1], 'position'))
        if not os.path.isdir(os.path.join(args.directory, s[1], 'rawgps')):
            os.mkdir(os.path.join(args.directory, s[1], 'rawgps'))

        # LiDAR
        lidar_data = connection.execute(db.select([lidar])
                                        .where(lidar.columns.unix_time < unix_today)
                                        .where(lidar.columns.unix_time > unix_yesterday)
                                        .where(lidar.columns.station_id == s[0])
                                        ).fetchall()
        if len(lidar_data) > 0:
            save_lidar(lidar_data, args.directory, s[1])
            lidar_ids = [i[0] for i in lidar_data]
            connection.execute(db.delete([lidar]).where(lidar.columns.id.in_(lidar_ids)))

        # GPS Position
        gpspos_data = connection.execute(db.select([gps_position])
                                         .where(gps_position.columns.week == yesterday_week)
                                         .where(gps_position.columns.i_tow > yesterday_itow)
                                         .where(gps_position.columns.i_tow < today_itow)
                                         .where(gps_position.columns.station_id == s[0])
                                         ).fetchall()
        if len(gpspos_data) > 0:
            save_gps_pos(gpspos_data, args.directory, s[1])
            gpspos_ids = [i[0] for i in gpspos_data]
            connection.execute(db.delete([gps_position]).where(gps_position.columns.id.in_(gpspos_ids)))

        # Raw GPS overall data
        raw_list = []
        gpsraw_data = connection.execute(db.select([gps_raw])
                                         .where(gps_raw.columns.week == yesterday_week)
                                         .where((gps_raw.columns.rcv_tow - gps_raw.columns.leap_seconds) > yesterday_rtow)
                                         .where((gps_raw.columns.rcv_tow - gps_raw.columns.leap_seconds) < today_rtow)
                                         .where(gps_raw.columns.station_id == s[0])
                                         ).fetchall()

        if len(gpsraw_data) > 0:
            for i in gpsraw_data:
                measurements = connection.execute(db.select([gps_measurement])
                                                  .where(gps_measurement.columns.gps_raw_id == i[0])
                                                  ).fetchall()
                raw_list.append(RxmRawx(i[1], i[2], i[3], measurements))

            save_raw_gps(raw_list, args.directory, s[1], s[2], s[3], s[4], s[6])
            gpsraw_ids = [i[0] for i in gpsraw_data]
            connection.execute(db.delete([gps_raw]).where(gps_raw.columns.id.in_(gpsraw_ids)))
            connection.execute(db.delete([gps_measurement]).where(gps_measurement.columns.gps_raw_id.in_(gpsraw_ids)))
