#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Copyright (c) 2019-2020 wutongjushi2020@gmail.com

import argparse
import os
import csv
import mysql.connector
from datetime import date, datetime, timedelta


status = {
    'invalid_command': {
        'code': 1,
        'message': 'Invalid command'
    }
}

column_maps = {
    'redfin': {
        'ADDRESS': 'address',
        'PROPERTY TYPE': 'property_type',
        'ZIP OR POSTAL CODE': 'zip',
        'STATUS': 'status',
        'DAYS ON MARKET': 'days_on_market',
        'BATHS': 'baths',
        'LOT SIZE': 'lot',
        'SOLD DATE': 'sold_date',
        'SALE TYPE': 'sale_type',
        'PRICE': 'price',
        'HOA/MONTH' : 'hoa',
        'SQUARE FEET': 'sq_feet',
        'BEDS': 'beds',
        'LATITUDE': 'latitude',
        'LONGITUDE': 'longitude'
    }

}
cnx = mysql.connector.connect(user='root',
                              password='realestate2020',
                              host='127.0.0.1')
cursor = cnx.cursor(dictionary=True)
sql_query = 'USE realestate'
cursor.execute(sql_query, )


def insert_property(property_data, import_date, last_status, last_price):
    print('DEBUG: Insert', property_data['address'], property_data['zip'],
          property_data['status'], last_status, last_price)
    db_updated = False
    days_on_market = int(property_data['days_on_market'])
    cur_price = int(property_data['price'])
    hoa = 0 if property_data['hoa'] == '' else int(property_data['hoa'])
    list_date = import_date.date() - timedelta(days=days_on_market)
    baths = 0 if property_data['baths'] == '' else property_data['baths']
    lot = 0 if property_data['lot'] == '' else property_data['lot']
    beds = 0 if property_data['beds'] == '' else property_data['beds']
    sq_feet = 0 if property_data['sq_feet'] == '' else property_data['sq_feet']

    # New property or property first seen
    sql_query = """INSERT INTO properties (
                       address, zip, price, hoa, beds, baths, sq_feet, lot, date, 
                       status, latitude, longitude, property_type)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

    if last_status is None:
        new_status = 'Active'
        if property_data['status'] == 'Sold':
            # past sale imported for the first time. make list date one day earlier
            # so that sale date is always more recent
            new_date = list_date - timedelta(days=1)
        else:
            new_date = list_date

        print('INFO: add {} {} {}'.format(
            property_data['address'], property_data['status'], property_data['price']))
        sql_data = (property_data['address'], property_data['zip'],
                    cur_price, hoa, beds, baths, sq_feet, lot,
                    new_date, new_status, property_data['latitude'],
                    property_data['longitude'], property_data['property_type'])
        cursor.execute(sql_query, sql_data)
        last_status = new_status
        last_price = cur_price
        db_updated = True

    # check if status and/or price is updated
    if last_status is not None and (property_data['status'] != last_status or
                                    cur_price != last_price):
        print('INFO: Update {}: previous: {} {} current: {} {}'.format(
            property_data['address'], last_status, last_price,
            property_data['status'], cur_price))
        if property_data['status'] == 'Sold':
            # use the sold date from the import data
            new_date = datetime.strptime(property_data['sold_date'], '%B-%d-%Y')
        else:
            # use the file export date
            new_date = import_date

        sql_data = (property_data['address'], property_data['zip'],
                    cur_price, hoa, beds, baths, sq_feet, lot,
                    new_date, property_data['status'], property_data['latitude'],
                    property_data['longitude'], property_data['property_type'])
        cursor.execute(sql_query, sql_data)
        db_updated = True

    if db_updated is False:
        print('INFO: Not updated:', property_data['address'])
        return 0
    else:
        return 1


def map_property_data(property_data_in, column_map):
    property_data = {}
    for k in column_map.keys():
        property_data[column_map[k]] = property_data_in[k]

    return property_data


# Redfin exports data as a csv file
def import_redfin_data(file, file_date=None):
    if file_date is None:
        # figure out the date from the filename
        filename = os.path.splitext(os.path.basename(file))[0]
        file_date = datetime.strptime(filename, 'redfin_%Y-%m-%d-%H-%M-%S')
        print('INFO:', filename, file_date)

    with open(file) as csvfile:
        reader = csv.DictReader(csvfile)
        total_cnt = 0
        update_cnt = 0
        for i,row in enumerate(reader):
            total_cnt += 1
            property_data = map_property_data(row, column_maps['redfin'])
            if property_data['status'] == '':
                print('WARNING: Skip row {}: {}: Unknown status'.format(
                    i+1, property_data['address'], property_data['status']))
                continue

            if property_data['zip'] is None:
                print('WARNING: skip row {}: zip code is missing.'.format(i+1))
                continue

            # Get the last record of this property
            sql_query = """SELECT id, address, zip, status, date, price 
                           FROM properties 
                           WHERE address=%s AND zip=%s 
                           ORDER BY date DESC LIMIT 1"""
            sql_data = (property_data['address'], property_data['zip'])
            cursor.execute(sql_query, sql_data)
            sql_result = cursor.fetchall()

            if len(sql_result) > 0:
                #print('DEBUG: row {}: Existing house'.format(i))
                #print('DEBUG: last status/price:', sql_result[0]['status'],
                #      sql_result[0]['price'])
                #print('DEBUG: current status/price:', property_data['status'],
                #      property_data['price'])
                last_status = sql_result[0]['status']
                last_price = sql_result[0]['price']
            else:
                last_status = None
                last_price = None

            update_cnt += insert_property(property_data, file_date, last_status,
                                          last_price)
    cnx.commit()

    print('INFO: total properties in file: {}, updated: {}'.format(total_cnt, update_cnt))


def import_data(file, source='redfin'):
    if source == 'redfin':
        import_redfin_data(file)


def main():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest='command')
    ###########################################################################
    # import subcommand
    ###########################################################################
    parser_import = subparsers.add_parser('import', help='Import data')
    parser_import.add_argument('file', help='Data file')
    parser_import.add_argument('--source', dest='source', default='redfin',
                               help='Data file source')

    args = parser.parse_args()  # returns data from the options specified (echo)

    if args.command is None:
        parser.print_help()
        exit(status['invalid_command']['code'])

    if args.command == 'import':
        import_data(args.file, args.source)

if __name__ == "__main__":
    main()
