#!/usr/bin/env python3

from datetime import datetime, timedelta
from influxdb import InfluxDBClient
import pg
import logging


influx_database_info ={'database': 'x',
                       'host': 'localhost',
                       'port': 8086,
                       'database_exist': False,
                       }

pg_database_info = {'host': 'x',
                    'port': 5432,
                    'database_name': 'x',
                    'user': 'x',
                    'password': 'x'}
sql = "your sql"


def logging_config():
    logging.basicConfig(filename='postgress_to_influx.log', level=logging.ERROR,
                        format='%(levelname)s:%(message)s')


def datetime_range(start, end, delta):
    current = start
    while current < end:
        yield current
        current += delta


dates = [dt for dt in datetime_range(datetime(2017, 12, 1), datetime(2019, 3, 18),
                                     timedelta(days=1))]


def check_database_exist(databases):
    for db in databases:
        if db['name'] == influx_database_info['database']:
            influx_database_info['database_exist'] = True
            return


def insert_data_influx_db(query):

    # inserting data
    try:
        influx_client.write(query, {'db': influx_database_info['database']}, 204, 'line')
    except Exception as e:
        logging.error(e)


def postgres_connect():
    try:
        conn = pg.DB(
            host=pg_database_info['host'],
            port=pg_database_info['port'],
            dbname=pg_database_info['database_name'],
            user=pg_database_info['user'],
            passwd=pg_database_info['password'])
        return conn

    except Exception as e:
        logging.error(e)


def db_operation():

    connect = postgres_connect()

    for date in dates:
        result = connect.query(sql.format(str(date), str(date), str(date), str(date))).dictresult()

        query1 = []
        query2 = []
        for r in result:
            query1.append('mobile_count,branch='+str(r['branch']) + " mobile_count=" + str(r['mobileno'])
                          + " " + str(int(date.timestamp()) * 1000000000))
            query2.append('account_count,branch=' + str(r['branch']) + " account_count=" + str(r['account'])
                          + " " + str(int(date.timestamp()) * 1000000000))

        insert_data_influx_db(query1)
        insert_data_influx_db(query2)


if __name__ == "__main__":
    logging_config()

    # initialize connection
    influx_client = InfluxDBClient(host=influx_database_info['host'], port=influx_database_info['port'])
    databases = influx_client.get_list_database()

    # checking existing of database
    check_database_exist(databases)

    # create database if not exist
    if not influx_database_info['database_exist']:
        influx_client.create_database(influx_database_info['database'])
    db_operation()
