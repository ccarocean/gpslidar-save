import argparse
import sqlalchemy as db
import datetime as dt
import os
from .save import save_gps_pos, save_raw_gps, save_lidar
from .messages import RxmRawx


def main():
    dname = os.environ['GPSLIDAR_DNAME']
    data_dir = os.environ['GPSLIDARDATADIRECTORY']

    print(dname)
    print(data_dir)

    # Parse arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--directory', type=str, default=data_dir,
                        help='Directory for output data. Default is ' + data_dir)
    args = parser.parse_args()

    engine = db.create_engine(dname)
    meta = db.MetaData()
    connection = engine.connect()
    print('Database connected')

    stations = db.Table('stations', meta, autoload=True, autoload_with=engine)
    lidar = db.Table('lidar', meta, autoload=True, autoload_with=engine)
    gps_raw = db.Table('gps_raw', meta, autoload=True, autoload_with=engine)
    gps_measurement = db.Table('gps_measurement', meta, autoload=True, autoload_with=engine)
    gps_position = db.Table('gps_position', meta, autoload=True, autoload_with=engine)

    print('Tables created')

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

    print('Dates found')

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

        print('Starting lidar')
        # LiDAR for previous day
        lidar_data = connection.execute(db.select([lidar])
                                        .where(lidar.columns.unix_time < unix_today)
                                        .where(lidar.columns.unix_time > unix_yesterday)
                                        .where(lidar.columns.station_id == s[0])
                                        .order_by(lidar.columns.unix_time)
                                        ).fetchmany(100000)
        lidar_ids = False
        while len(lidar_data) > 0:
            print('a')
            save_lidar(lidar_data, args.directory, s[1])
            lidar_ids = [i[0] for i in lidar_data]
            print('b')
            connection.execute(db.delete(lidar).where(lidar.columns.id.in_(lidar_ids)))
            lidar_data = connection.execute(db.select([lidar])
                                            .where(lidar.columns.unix_time < unix_today)
                                            .where(lidar.columns.unix_time > unix_yesterday)
                                            .where(lidar.columns.station_id == s[0])
                                            .order_by(lidar.columns.unix_time)
                                            ).fetchmany(100000)
            print('c')
        if lidar_ids:
            print("LiDAR Data saved for " + s[1])

        # GPS Position for previous day
        gpspos_data = connection.execute(db.select([gps_position])
                                         .where(gps_position.columns.week == yesterday_week)
                                         .where(gps_position.columns.i_tow > yesterday_itow)
                                         .where(gps_position.columns.i_tow < today_itow)
                                         .where(gps_position.columns.station_id == s[0])
                                         .order_by(gps_position.columns.week, gps_position.columns.i_tow)
                                         ).fetchall()
        if len(gpspos_data) > 0:
            save_gps_pos(gpspos_data, args.directory, s[1])
            gpspos_ids = [i[0] for i in gpspos_data]
            connection.execute(db.delete(gps_position).where(gps_position.columns.id.in_(gpspos_ids)))
            print("GPS Position Data saved for " + s[1])

        # Raw GPS overall data for previous day
        raw_list = []
        gpsraw_data = connection.execute(db.select([gps_raw])
                                         .where(gps_raw.columns.week == yesterday_week)
                                         .where((gps_raw.columns.rcv_tow - gps_raw.columns.leap_seconds) > yesterday_rtow)
                                         .where((gps_raw.columns.rcv_tow - gps_raw.columns.leap_seconds) < today_rtow)
                                         .where(gps_raw.columns.station_id == s[0])
                                         .order_by(gps_raw.columns.week, gps_raw.columns.rcv_tow)
                                         ).fetchmany(100000)
        gpsraw_ids = False
        while len(gpsraw_data) > 0:
            for i in gpsraw_data:
                measurements = connection.execute(db.select([gps_measurement])
                                                  .where(gps_measurement.columns.gps_raw_id == i[0])
                                                  ).fetchall()
                raw_list.append(RxmRawx(i[1], i[2], i[3], measurements))

            save_raw_gps(raw_list, args.directory, s[1], s[2], s[3], s[4], s[6])
            gpsraw_ids = [i[0] for i in gpsraw_data]
            connection.execute(db.delete(gps_measurement).where(gps_measurement.columns.gps_raw_id.in_(gpsraw_ids)))
            connection.execute(db.delete(gps_raw).where(gps_raw.columns.id.in_(gpsraw_ids)))
            gpsraw_data = connection.execute(db.select([gps_raw])
                                             .where(gps_raw.columns.week == yesterday_week)
                                             .where((gps_raw.columns.rcv_tow - gps_raw.columns.leap_seconds) > yesterday_rtow)
                                             .where((gps_raw.columns.rcv_tow - gps_raw.columns.leap_seconds) < today_rtow)
                                             .where(gps_raw.columns.station_id == s[0])
                                             .order_by(gps_raw.columns.week, gps_raw.columns.rcv_tow)
                                             ).fetchmany(100000)

        if gpsraw_ids:
            print("Raw GPS Data saved for " + s[1])

        # Check if old lidar data exists
        lidar_old = connection.execute(db.select([lidar])
                                       .where(lidar.columns.unix_time < unix_yesterday)
                                       .where(lidar.columns.station_id == s[0])
                                       .order_by(lidar.columns.unix_time)
                                       ).fetchmany(100000)
        lidar_ids = False
        while len(lidar_old) > 0:
            day = dt.datetime(1970, 1, 1) + dt.timedelta(days=lidar_old[0][1]//(3600*24))
            unix_st = (day - dt.datetime(1970, 1, 1)).total_seconds()
            unix_end = (day + dt.timedelta(days=1) - dt.datetime(1970, 1, 1)).total_seconds()

            data = connection.execute(db.select([lidar])
                                      .where(lidar.columns.unix_time > unix_st)
                                      .where(lidar.columns.unix_time < unix_end)
                                      .where(lidar.columns.station_id == s[0])
                                      .order_by(lidar.columns.unix_time)
                                      ).fetchall()
            save_lidar(data, args.directory, s[1])
            lidar_ids = [i[0] for i in data]
            connection.execute(db.delete(lidar).where(lidar.columns.id.in_(lidar_ids)))
            lidar_old = connection.execute(db.select([lidar])
                                           .where(lidar.columns.unix_time < unix_yesterday)
                                           .where(lidar.columns.station_id == s[0])
                                           .order_by(lidar.columns.unix_time)
                                           ).fetchmany(100000)

        if lidar_ids:
            print("Old LiDAR Data saved for " + s[1])

        # Check if old position data exists
        pos_old = connection.execute(db.select([gps_position])
                                     .where(db.or_(gps_position.columns.week < yesterday_week,
                                                   db.and_(gps_position.columns.week == yesterday_week,
                                                           gps_position.columns.i_tow < yesterday_itow)))
                                     .where(gps_position.columns.station_id == s[0])
                                     .order_by(gps_position.columns.week, gps_position.columns.i_tow)
                                     ).fetchall()

        while len(pos_old) > 0:
            day = dt.datetime(1980, 1, 6) + dt.timedelta(days=7*pos_old[0][2]) + \
                  dt.timedelta(days=pos_old[0][1] // (1000*3600*24))
            week = (day - dt.datetime(1980, 1, 6)).total_seconds() / (3600 * 24) // 7
            itow_st = (day - dt.datetime(1980, 1, 6)).total_seconds() % (7 * 24 * 3600) * 1000
            itow_end = itow_st + 24 * 3600 * 1000

            data = connection.execute(db.select([gps_position])
                                      .where(gps_position.columns.week == week)
                                      .where(gps_position.columns.i_tow > itow_st)
                                      .where(gps_position.columns.i_tow < itow_end)
                                      .where(gps_position.columns.station_id == s[0])
                                      .order_by(gps_position.columns.week, gps_position.columns.i_tow)
                                      ).fetchall()

            save_gps_pos(data, args.directory, s[1])
            gpspos_ids = [i[0] for i in data]
            connection.execute(db.delete(gps_position).where(gps_position.columns.id.in_(gpspos_ids)))
            print("Old GPS Position Data saved for " + s[1])
            pos_old = connection.execute(db.select([gps_position])
                                         .where(db.or_(gps_position.columns.week < yesterday_week,
                                                       db.and_(gps_position.columns.week == yesterday_week,
                                                               gps_position.columns.i_tow < yesterday_itow)))
                                         .where(gps_position.columns.station_id == s[0])
                                         .order_by(gps_position.columns.week, gps_position.columns.i_tow)
                                         ).fetchall()

        # Check if old raw gps data exists
        raw_old = connection.execute(db.select([gps_raw])
                                     .where(db.or_(gps_raw.columns.week < yesterday_week,
                                                   db.and_(gps_raw.columns.week == yesterday_week,
                                                           (gps_raw.columns.rcv_tow - gps_raw.columns.leap_seconds) <
                                                           yesterday_rtow)))
                                     .where(gps_raw.columns.station_id == s[0])
                                     .order_by(gps_raw.columns.week, gps_raw.columns.rcv_tow)
                                     ).fetchmany(100000)
        gpsraw_ids = False
        while len(raw_old) > 0:
            day = dt.datetime(1980, 1, 6) + dt.timedelta(days=7 * raw_old[0][2]) + \
                  dt.timedelta(days=(raw_old[0][1]-raw_old[0][3]) // (3600 * 24))
            week = (day - dt.datetime(1980, 1, 6)).total_seconds() / (3600 * 24) // 7
            rtow_st = (day - dt.datetime(1980, 1, 6)).total_seconds() % (7 * 24 * 3600)
            rtow_end = rtow_st + 24 * 3600

            data = connection.execute(db.select([gps_raw])
                                      .where(gps_raw.columns.week == week)
                                      .where((gps_raw.columns.rcv_tow - gps_raw.columns.leap_seconds) > rtow_st)
                                      .where((gps_raw.columns.rcv_tow - gps_raw.columns.leap_seconds) < rtow_end)
                                      .where(gps_raw.columns.station_id == s[0])
                                      .order_by(gps_raw.columns.week, gps_raw.columns.rcv_tow)
                                      ).fetchall()

            for i in data:
                old_measurements = connection.execute(db.select([gps_measurement])
                                                      .where(gps_measurement.columns.gps_raw_id == i[0])
                                                      ).fetchall()
                raw_list.append(RxmRawx(i[1], i[2], i[3], measurements))

            save_raw_gps(raw_list, args.directory, s[1], s[2], s[3], s[4], s[6])
            gpsraw_ids = [i[0] for i in gpsraw_data]
            connection.execute(db.delete(gps_measurement).where(gps_measurement.columns.gps_raw_id.in_(gpsraw_ids)))
            connection.execute(db.delete(gps_raw).where(gps_raw.columns.id.in_(gpsraw_ids)))

            raw_old = connection.execute(db.select([gps_raw])
                                         .where(db.or_(gps_raw.columns.week < yesterday_week,
                                                       db.and_(gps_raw.columns.week == yesterday_week,
                                                               (
                                                                           gps_raw.columns.rcv_tow - gps_raw.columns.leap_seconds) <
                                                               yesterday_rtow)))
                                         .where(gps_raw.columns.station_id == s[0])
                                         .order_by(gps_raw.columns.week, gps_raw.columns.rcv_tow)
                                         ).fetchmany(100000)

        if gpsraw_ids:
            print("Old Raw GPS Data saved for " + s[1])
