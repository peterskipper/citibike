""" Module adds values to citibike db """
import requests
from pandas.io.json import json_normalize
import sqlite3 as lite
import time
import dateutil.parser as du
import collections


def populate_db(db_name='citi-bike.db'):
    """ Create db and add values """

    # Get reference data from citibike
    ref = requests.get('http://www.citibikenyc.com/stations/json')

    # Connect to db
    conn = lite.connect(db_name)
    curs = conn.cursor()

    # Create reference db
    with conn:
        curs.execute('DROP TABLE if exists citibike_reference;')
        curs.execute('CREATE TABLE citibike_reference (id INT PRIMARY KEY, '
                     'totalDocks INT, city TEXT, altitude INT, stAddress2 '
                     'TEXT, longitude NUMERIC, postalCode TEXT, testStation '
                     'TEXT, stAddress1 TEXT, stationName TEXT, landMark TEXT, '
                     'latitude NUMERIC, location TEXT )')

    # Add reference data
    refsql = ('INSERT INTO citibike_reference (id, totalDocks, city, '
              'altitude, stAddress2, longitude, postalCode, testStation, '
              'stAddress1, stationName, landMark, latitude, location) '
              'VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)')
    with conn:
        for station in ref.json()['stationBeanList']:
            curs.execute(refsql, (station['id'], station['totalDocks'],
                                  station['city'],
                                  station['altitude'],
                                  station['stAddress2'],
                                  station['longitude'],
                                  station['postalCode'],
                                  station['testStation'],
                                  station['stAddress1'],
                                  station['stationName'],
                                  station['landMark'],
                                  station['latitude'],
                                  station['location']))

    # Create available bike table
    station_ids = json_normalize(ref.json()['stationBeanList'])['id'].tolist()
    station_ids = ['_' + str(x) + ' INT' for x in station_ids]
    with conn:
        curs.execute('DROP TABLE if exists available_bikes')
        curs.execute('CREATE TABLE available_bikes (execution_time INT, ' +
                     ', '.join(station_ids) + ');')

    # Add available bike data
    for rep in xrange(60):
        req = requests.get('http://www.citibikenyc.com/stations/json')
        exec_time = du.parse(req.json()['executionTime'])
        id_bikes = collections.defaultdict(int)
        for station in req.json()['stationBeanList']:
            id_bikes[station['id']] = station['availableBikes']
        with conn:
            curs.execute('INSERT INTO available_bikes (execution_time)'
                         'VALUES (?)', (exec_time.strftime('%s'),))
        with conn:
            for key, val in id_bikes.iteritems():
                curs.execute('UPDATE available_bikes SET _' + str(key) +
                             ' = ' + str(val) + ' WHERE execution_time = ' +
                             exec_time.strftime('%s') + ';')
        time.sleep(60)
        print "Completed rep " + str(rep)

if __name__ == '__main__':
    populate_db()
