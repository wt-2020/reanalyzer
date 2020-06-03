#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Copyright (c) 2019-2020 jimw567@gmail.com

import argparse
import mysql.connector
import csv
from datetime import date, datetime, timedelta

cnx = mysql.connector.connect(user='root',
                              password='realestate2020',
                              host='127.0.0.1')
cursor = cnx.cursor()


def create_databases():
    db_name = 'realestate'
    sql_query = 'CREATE DATABASE IF NOT EXISTS {}'.format(db_name)
    cursor.execute(sql_query,)
    print('INFO: created database', db_name)

    sql_query = 'USE {}'.format(db_name)
    cursor.execute(sql_query,)

    table_name = 'properties'
    sql_query = """CREATE TABLE IF NOT EXISTS {} (
                     id INT AUTO_INCREMENT,
                     status varchar(16),
                     address varchar(64),
                     property_type varchar(64),
                     zip varchar(10),
                     beds int,
                     baths float,
                     sq_feet int,
                     lot int,
                     date date,
                     price int,
                     hoa int,
                     latitude float,
                     longitude float,
                     PRIMARY KEY (id))""".format(table_name)
    cursor.execute(sql_query,)
    cnx.commit()
    print('INFO: created table', table_name)


# Update database configuration if needed
def update_databases():
    print('INFO: updating databases')
    # sql_query = """ALTER TABLE properties
    #                ADD COLUMN latitude float,
    #                ADD COLUMN longitude float"""
    # try:
    #     cursor.execute(sql_query,)
    #     cnx.commit()
    #     print('INFO: added columns latitude, longitude')
    # except Exception:
    #     print('WARNING: columns latitude, longitude already exist. ignore.')
    #
    # sql_query = """ALTER TABLE properties
    #                ADD COLUMN property_type varchar(64)"""
    # try:
    #     cursor.execute(sql_query,)
    #     cnx.commit()
    #     print('INFO: added column property_type')
    # except Exception:
    #     print('WARNING: column property_type already exist. ignore.')


def main():
    create_databases()
    update_databases()


if __name__ == "__main__":
    main()

