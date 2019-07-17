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

    for s in stations_data:  # For each station:
        # Make directories if they don't exist
        if not os.path.isdir(os.path.join(data_dir, s[1])):
            os.mkdir(os.path.join(data_dir, s[1]))
        if not os.path.isdir(os.path.join(data_dir, s[1], 'lidar')):
            os.mkdir(os.path.join(data_dir, s[1], 'lidar'))
        if not os.path.isdir(os.path.join(data_dir, s[1], 'lidar_sixmin')):
            os.mkdir(os.path.join(data_dir, s[1], 'lidar_sixmin'))
        if not os.path.isdir(os.path.join(data_dir, s[1], 'position')):
            os.mkdir(os.path.join(data_dir, s[1], 'position'))
        if not os.path.isdir(os.path.join(data_dir, s[1], 'rawgps')):
            os.mkdir(os.path.join(data_dir, s[1], 'rawgps'))

        # Grab first lidar data point for station
        lidar_data = connection.execute(db.select([lidar])
                                        .where(lidar.columns.station_id == s[0])
                                        .order_by(lidar.columns.unix_time)
                                        ).fetchmany(1)
        # Basically a do-while loop
        while len(lidar_data) > 0:
            day = dt.datetime(1970, 1, 1) + dt.timedelta(days=lidar_data[0][1]//(3600*24))
            unix_st = (day - dt.datetime(1970, 1, 1)).total_seconds()
            unix_end = (day + dt.timedelta(days=1) - dt.datetime(1970, 1, 1)).total_seconds()
            # Grab all data in day (up to 5 million points)
            data = connection.execute(db.select([lidar])
                                      .where(lidar.columns.unix_time > unix_st)
                                      .where(lidar.columns.unix_time < unix_end)
                                      .where(lidar.columns.station_id == s[0])
                                      .order_by(lidar.columns.unix_time)
                                      ).fetchmany(5000000)
            # Basically another do-while loop so all lidar data from the day is grabbed
            while len(data) > 0:
                save_lidar(data, data_dir, s[1])  # Save lidar data
                lidar_ids = [i[0] for i in data]  # Find ids of saved data to be deleted from database
                connection.execute(db.delete(lidar).where(lidar.columns.id.in_(lidar_ids)))  # Delete from db
                data = connection.execute(db.select([lidar])
                                          .where(lidar.columns.unix_time > unix_st)
                                          .where(lidar.columns.unix_time < unix_end)
                                          .where(lidar.columns.station_id == s[0])
                                          .order_by(lidar.columns.unix_time)
                                          ).fetchmany(5000000)  # Do part of do-while loop
            lidar_data = connection.execute(db.select([lidar])
                                            .where(lidar.columns.station_id == s[0])
                                            .order_by(lidar.columns.unix_time)
                                            ).fetchmany(1)  # Do part of do-while loop
            six_min(day, s[1])  # Average data to six minutes for comparison to NOAA data
            print("LiDAR Data saved for " + s[1] + ': ' + day.strftime('%Y-%m-%d'))

        # Grab first position data point for station
        pos_data = connection.execute(db.select([gps_position])
                                      .where(gps_position.columns.station_id == s[0])
                                      .order_by(gps_position.columns.week, gps_position.columns.i_tow)
                                      ).fetchmany(1)
        # Basically a do-while loop
        while len(pos_data) > 0:
            day = dt.datetime(1980, 1, 6) + dt.timedelta(days=7*pos_data[0][2]) + \
                  dt.timedelta(days=pos_data[0][1] // (1000*3600*24))
            week = (day - dt.datetime(1980, 1, 6)).total_seconds() / (3600 * 24) // 7
            itow_st = (day - dt.datetime(1980, 1, 6)).total_seconds() % (7 * 24 * 3600) * 1000
            itow_end = itow_st + 24 * 3600 * 1000
            # Grab ALL data from current day
            data = connection.execute(db.select([gps_position])
                                      .where(gps_position.columns.week == week)
                                      .where(gps_position.columns.i_tow > itow_st)
                                      .where(gps_position.columns.i_tow < itow_end)
                                      .where(gps_position.columns.station_id == s[0])
                                      .order_by(gps_position.columns.week, gps_position.columns.i_tow)
                                      ).fetchall()

            save_gps_pos(data, data_dir, s[1])  # Save position data
            gpspos_ids = [i[0] for i in data]  # Find ids to be deleted from database
            connection.execute(db.delete(gps_position).where(gps_position.columns.id.in_(gpspos_ids)))  # Delete from db
            print("GPS Position Data saved for " + s[1] + ': ' + day.strftime('%Y-%m-%d'))
            pos_data = connection.execute(db.select([gps_position])
                                          .where(gps_position.columns.station_id == s[0])
                                          .order_by(gps_position.columns.week, gps_position.columns.i_tow)
                                          ).fetchall()  # Do part of do-while loop

        # Grab first raw data point for current station
        raw_data = connection.execute(db.select([gps_raw])
                                      .where(gps_raw.columns.station_id == s[0])
                                      .order_by(gps_raw.columns.week, gps_raw.columns.rcv_tow)
                                      ).fetchmany(1)
        # Basically a do-while loop
        while len(raw_data) > 0:
            day = dt.datetime(1980, 1, 6) + dt.timedelta(days=7 * raw_data[0][2]) + \
                  dt.timedelta(days=(raw_data[0][1]-raw_data[0][3]) // (3600 * 24))
            week = (day - dt.datetime(1980, 1, 6)).total_seconds() / (3600 * 24) // 7
            rtow_st = (day - dt.datetime(1980, 1, 6)).total_seconds() % (7 * 24 * 3600)
            rtow_end = rtow_st + 24 * 3600
            # Grab all data from current day (up to 1 million measurements)
            data = connection.execute(db.select([gps_raw])
                                      .where(db.or_(db.and_(gps_raw.columns.week == week,
                                                            (gps_raw.columns.rcv_tow - gps_raw.columns.leap_seconds) <
                                                            rtow_end,
                                                            (gps_raw.columns.rcv_tow - gps_raw.columns.leap_seconds) >=
                                                            rtow_st),
                                                    db.and_(gps_raw.columns.week == (week+1),
                                                            (gps_raw.columns.rcv_tow - gps_raw.columns.leap_seconds) <
                                                            0)
                                                    )
                                             )
                                      .where(gps_raw.columns.station_id == s[0])
                                      .order_by(gps_raw.columns.week, gps_raw.columns.rcv_tow)
                                      ).fetchmany(100000)
            # Basically another do-while loop
            while len(data) > 0:
                raw_list = []
                for i in data:  # For each data point grab all satellite measurements and put in data structure
                    measurements = connection.execute(db.select([gps_measurement])
                                                      .where(gps_measurement.columns.gps_raw_id == i[0])
                                                      ).fetchall()
                    raw_list.append(RxmRawx(i[1], i[2], i[3], measurements))
                save_raw_gps(raw_list, data_dir, s[1], s[2], s[3], s[4], s[6])  # Save to RINEX file
                gpsraw_ids = [i[0] for i in data]  # Find ids to be deleted
                # Delete from measurement table
                connection.execute(db.delete(gps_measurement).where(gps_measurement.columns.gps_raw_id.in_(gpsraw_ids)))
                # Delete from raw table
                connection.execute(db.delete(gps_raw).where(gps_raw.columns.id.in_(gpsraw_ids)))
                data = connection.execute(db.select([gps_raw])
                                          .where(db.or_(db.and_(gps_raw.columns.week == week,
                                                                (
                                                                            gps_raw.columns.rcv_tow - gps_raw.columns.leap_seconds) <
                                                                rtow_end,
                                                                (
                                                                            gps_raw.columns.rcv_tow - gps_raw.columns.leap_seconds) >=
                                                                rtow_st),
                                                        db.and_(gps_raw.columns.week == (week + 1),
                                                                (
                                                                            gps_raw.columns.rcv_tow - gps_raw.columns.leap_seconds) <
                                                                0)
                                                        )
                                                 )
                                          .where(gps_raw.columns.station_id == s[0])
                                          .order_by(gps_raw.columns.week, gps_raw.columns.rcv_tow)
                                          ).fetchmany(100000)  # Do part of do-while loop

            raw_data = connection.execute(db.select([gps_raw])
                                          .where(gps_raw.columns.station_id == s[0])
                                          .order_by(gps_raw.columns.week, gps_raw.columns.rcv_tow)
                                          ).fetchmany(1)  # Do part of do-while loop

            print("Raw GPS Data saved for " + s[1] + ': ' + day.strftime('%Y-%m-%d'))
