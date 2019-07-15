import argparse
import sqlalchemy as db
import datetime as dt
import os
from .save import save_gps_pos, save_raw_gps, save_lidar
from .messages import RxmRawx
from .avg import six_min


def main():
    dname = os.environ['GPSLIDAR_DNAME']
    data_dir = os.environ['GPSLIDARDATADIRECTORY']

    # Parse arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--directory', type=str, default=data_dir,
                        help='Directory for output data. Default is ' + data_dir)
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
    unix_today = (today - dt.datetime(1970, 1, 1)).total_seconds()
    today_week = (today - dt.datetime(1980, 1, 6)).total_seconds() / (3600 * 24) // 7
    today_itow = (today - dt.datetime(1980, 1, 6)).total_seconds() % (7 * 24 * 3600) * 1000
    today_rtow = (today - dt.datetime(1980, 1, 6)).total_seconds() % (7 * 24 * 3600)

    for s in stations_data:
        # Make directories if they don't exist
        if not os.path.isdir(os.path.join(args.directory, s[1])):
            os.mkdir(os.path.join(args.directory, s[1]))
        if not os.path.isdir(os.path.join(args.directory, s[1], 'lidar')):
            os.mkdir(os.path.join(args.directory, s[1], 'lidar'))
        if not os.path.isdir(os.path.join(args.directory, s[1], 'lidar_sixmin')):
            os.mkdir(os.path.join(args.directory, s[1], 'lidar_sixmin'))
        if not os.path.isdir(os.path.join(args.directory, s[1], 'position')):
            os.mkdir(os.path.join(args.directory, s[1], 'position'))
        if not os.path.isdir(os.path.join(args.directory, s[1], 'rawgps')):
            os.mkdir(os.path.join(args.directory, s[1], 'rawgps'))

        # Save lidar data
        lidar_data = connection.execute(db.select([lidar])
                                        .where(lidar.columns.unix_time < unix_today)
                                        .where(lidar.columns.station_id == s[0])
                                        .order_by(lidar.columns.unix_time)
                                        ).fetchmany(1)

        while len(lidar_data) > 0:
            day = dt.datetime(1970, 1, 1) + dt.timedelta(days=lidar_data[0][1]//(3600*24))
            unix_st = (day - dt.datetime(1970, 1, 1)).total_seconds()
            unix_end = (day + dt.timedelta(days=1) - dt.datetime(1970, 1, 1)).total_seconds()

            data = connection.execute(db.select([lidar])
                                      .where(lidar.columns.unix_time > unix_st)
                                      .where(lidar.columns.unix_time < unix_end)
                                      .where(lidar.columns.station_id == s[0])
                                      .order_by(lidar.columns.unix_time)
                                      ).fetchmany(5000000)
            while len(data) > 0:
                save_lidar(data, args.directory, s[1])
                lidar_ids = [i[0] for i in data]
                connection.execute(db.delete(lidar).where(lidar.columns.id.in_(lidar_ids)))
                data = connection.execute(db.select([lidar])
                                          .where(lidar.columns.unix_time > unix_st)
                                          .where(lidar.columns.unix_time < unix_end)
                                          .where(lidar.columns.station_id == s[0])
                                          .order_by(lidar.columns.unix_time)
                                          ).fetchmany(5000000)
            lidar_data = connection.execute(db.select([lidar])
                                            .where(lidar.columns.unix_time < unix_today)
                                            .where(lidar.columns.station_id == s[0])
                                            .order_by(lidar.columns.unix_time)
                                            ).fetchmany(1)
            six_min(day, s[1])
            print("LiDAR Data saved for " + s[1] + ': ' + day.strftime('%Y-%m-%d'))

        # Save position data
        pos_data = connection.execute(db.select([gps_position])
                                      .where(db.or_(gps_position.columns.week < today_week,
                                                    db.and_(gps_position.columns.week == today_week,
                                                            gps_position.columns.i_tow < today_itow)))
                                      .where(gps_position.columns.station_id == s[0])
                                      .order_by(gps_position.columns.week, gps_position.columns.i_tow)
                                      ).fetchmany(1)

        while len(pos_data) > 0:
            day = dt.datetime(1980, 1, 6) + dt.timedelta(days=7*pos_data[0][2]) + \
                  dt.timedelta(days=pos_data[0][1] // (1000*3600*24))
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
            print("GPS Position Data saved for " + s[1] + ': ' + day.strftime('%Y-%m-%d'))
            pos_data = connection.execute(db.select([gps_position])
                                          .where(db.or_(gps_position.columns.week < today_week,
                                                        db.and_(gps_position.columns.week == today_week,
                                                                gps_position.columns.i_tow < today_itow)))
                                          .where(gps_position.columns.station_id == s[0])
                                          .order_by(gps_position.columns.week, gps_position.columns.i_tow)
                                          ).fetchall()

        # Save raw gps data
        raw_data = connection.execute(db.select([gps_raw])
                                      .where(db.or_(gps_raw.columns.week < today_week,
                                                    db.and_(gps_raw.columns.week == today_week,
                                                            (gps_raw.columns.rcv_tow - gps_raw.columns.leap_seconds) <
                                                            today_rtow)))
                                      .where(gps_raw.columns.station_id == s[0])
                                      .order_by(gps_raw.columns.week, gps_raw.columns.rcv_tow)
                                      ).fetchmany(1)

        while len(raw_data) > 0:
            timetmp = dt.datetime.utcnow()
            day = dt.datetime(1980, 1, 6) + dt.timedelta(days=7 * raw_data[0][2]) + \
                  dt.timedelta(days=(raw_data[0][1]-raw_data[0][3]) // (3600 * 24))
            week = (day - dt.datetime(1980, 1, 6)).total_seconds() / (3600 * 24) // 7
            rtow_st = (day - dt.datetime(1980, 1, 6)).total_seconds() % (7 * 24 * 3600)
            rtow_end = rtow_st + 24 * 3600
            print('a: ' + str((dt.datetime.utcnow()-timetmp).total_seconds()))

            data = connection.execute(db.select([gps_raw])
                                      .where(gps_raw.columns.week == week)
                                      .where((gps_raw.columns.rcv_tow - gps_raw.columns.leap_seconds) > rtow_st)
                                      .where((gps_raw.columns.rcv_tow - gps_raw.columns.leap_seconds) < rtow_end)
                                      .where(gps_raw.columns.station_id == s[0])
                                      .order_by(gps_raw.columns.week, gps_raw.columns.rcv_tow)
                                      ).fetchmany(100000)
            print('b: ' + str((dt.datetime.utcnow()-timetmp).total_seconds()))

            while len(data) > 0:
                raw_list = []
                gpsraw_ids = [i[0] for i in data]
                tmp = connection.execute(db.select([gps_measurement])
                                         .where(gps_measurement.columns.gps_raw_id.in_(gpsraw_ids))).fetchall()
                for i in data:
                    print('first: ' + str((dt.datetime.utcnow() - timetmp).total_seconds()))
                    measurements1 = connection.execute(db.select([gps_measurement])
                                                      .where(gps_measurement.columns.gps_raw_id == i[0])
                                                      ).fetchall()
                    measurements2 = [j for j in tmp if j[8] == i[0]]
                    import pdb; pdb.set_trace()
                    print(measurements1)
                    print('\n')
                    print(measurements2)
                    input()
                    print('second: ' + str((dt.datetime.utcnow() - timetmp).total_seconds()))
                    raw_list.append(RxmRawx(i[1], i[2], i[3], measurements))
                    print('third: ' + str((dt.datetime.utcnow() - timetmp).total_seconds()))
                print('c: ' + str((dt.datetime.utcnow()-timetmp).total_seconds()))
                save_raw_gps(raw_list, args.directory, s[1], s[2], s[3], s[4], s[6])
                print('d: ' + str((dt.datetime.utcnow()-timetmp).total_seconds()))
                gpsraw_ids = [i[0] for i in data]
                print('e: ' + str((dt.datetime.utcnow()-timetmp).total_seconds()))
                connection.execute(db.delete(gps_measurement).where(gps_measurement.columns.gps_raw_id.in_(gpsraw_ids)))
                print('f: ' + str((dt.datetime.utcnow()-timetmp).total_seconds()))
                connection.execute(db.delete(gps_raw).where(gps_raw.columns.id.in_(gpsraw_ids)))
                print('g: ' + str((dt.datetime.utcnow()-timetmp).total_seconds()))
                data = connection.execute(db.select([gps_raw])
                                          .where(gps_raw.columns.week == week)
                                          .where((gps_raw.columns.rcv_tow - gps_raw.columns.leap_seconds) > rtow_st)
                                          .where((gps_raw.columns.rcv_tow - gps_raw.columns.leap_seconds) < rtow_end)
                                          .where(gps_raw.columns.station_id == s[0])
                                          .order_by(gps_raw.columns.week, gps_raw.columns.rcv_tow)
                                          ).fetchmany(100000)
                print('h: ' + str((dt.datetime.utcnow()-timetmp).total_seconds()))

            raw_data = connection.execute(db.select([gps_raw])
                                          .where(db.or_(gps_raw.columns.week < today_week,
                                                        db.and_(gps_raw.columns.week == today_week,
                                                                (gps_raw.columns.rcv_tow - gps_raw.columns.leap_seconds)
                                                                < today_rtow)))
                                          .where(gps_raw.columns.station_id == s[0])
                                          .order_by(gps_raw.columns.week, gps_raw.columns.rcv_tow)
                                          ).fetchmany(1)

            print('i: ' + str((dt.datetime.utcnow() - timetmp).total_seconds()))

            print("Raw GPS Data saved for " + s[1] + ': ' + day.strftime('%Y-%m-%d'))
