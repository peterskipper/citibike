""" Module analyzes Bike checkouts from NYC open data portal """
import pandas as pd
import matplotlib.pyplot as plt
import sqlite3 as lite
from populate_db import populate_db
import os

def analyze_bike_data():
    """ Use downloaded data to find busiest station """
    # Connect to Database
    conn = lite.connect('citi-bike.db')
    curs = conn.cursor()

    # Create Data Frame
    data = pd.read_sql_query('select * from available_bikes order by '
                             'execution_time;', conn,
                             index_col='execution_time')

    # Find Busiest Station
    diffed = data.diff().dropna()

    def abs_sum(col):
        """Helper func get sum of abs. value of column elements"""
        total = 0
        for val in col:
            total += abs(val)
        return total

    totals = diffed.apply(abs_sum).sort_index()

    top_station = totals.idxmax()[1:]
    top_value = int(totals.max())

    curs.execute('SELECT stationName from citibike_reference WHERE id = "' +
                 top_station + '"')
    result = curs.fetchone()
    print ("The most active bike station is {} where {} "
           "bikes were taken out or returned in 1 hour"
           .format(result[0], top_value))
    indices = [int(key[1:]) for key in totals.keys()]
    plt.bar(indices, totals)
    plt.xlabel('Station ID')
    plt.ylabel('Bikes Taken or Returned')
    plt.title('1 Hour of Citibike Borrowing Data in NYC')
    plt.savefig('bike_usage.png')

if __name__ == '__main__':
    if not os.path.isfile('citi-bike.db'):
        populate_db()
    analyze_bike_data()

