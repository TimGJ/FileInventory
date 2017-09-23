# -*- coding: utf-8 -*-
"""
Created on Fri Sep 22 22:37:10 2017

@author: User
"""

import pandas as pd
import argparse
import getpass
import logging


from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import sqlalchemy.exc

from FileInventory import Job, Directory, File

def GetArgs():
    """
    Process command line arguments
    """
    ap = argparse.ArgumentParser(description='Get inventory of files')
    gr = ap.add_mutually_exclusive_group()
    ex = ap.add_argument_group(title='Exotic', description='Here be dragons')
    ap.add_argument('--host',        '-t', help='DB hostname or IP address')
    ap.add_argument('--user',        '-u', help='DB username')
    gr.add_argument('--password',    '-p', help='DB password')
    gr.add_argument('--blank',       '-b', help='Permit blank password', action='store_true')
    ap.add_argument('--schema',      '-s', help='DB schema', default='inventory')
    ap.add_argument('--connector',   '-c', help='DB connector', default='mysql+mysqlconnector')
    ex.add_argument('--sql-debug',   '-q', help='Print details of SQL commands', action='store_true')
    ex.add_argument('--verbose',     '-v', help='Verbosity', action='count')
    
    args = ap.parse_args()
    
    while not (args.blank or args.password): # Prompt for a password if required
        args.password = getpass.getpass('Password: ')

    if not args.verbose: # Is None if not specified
        args.verbose = 0

    return args


if __name__ == '__main__':
    args = GetArgs()
        
    loglevels = [logging.WARNING, logging.INFO, logging.DEBUG]
    level = loglevels[min(args.verbose, len(loglevels)- 1)]
    logging.basicConfig(level = level, format = '%(asctime)s %(message)s')
    
    if args.password:
        connectstr = '{}://{}:{}@{}/{}'.format(args.connector, args.user, 
                      args.password, args.host, args.schema)
    else:
        connectstr = '{}://{}@{}/{}'.format(args.connector, args.user, 
                      args.host, args.schema)
        
    try:
        engine = create_engine(connectstr, pool_recycle=3600, 
                               echo = True if args.sql_debug else False)
    except sqlalchemy.exc.NoSuchModuleError as e:
        logging.critical("Error creating engine: {}".format(e))
    else:
        cnx = engine.connect()
        sql = """select round(size/(1024*1024)) as MB, count(id) as N 
                from file group by round(size/(1024*1024)) 
                order by round(size/(1024*1024))"""
        df = pd.read_sql_query(sql, cnx)
    
    
