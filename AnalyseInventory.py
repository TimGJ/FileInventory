# -*- coding: utf-8 -*-
"""
Created on Fri Sep 22 22:37:10 2017

Some simple Pandas analysis of the inventory data, 
more as a test and a placeholder for future use. 
"""

import pandas as pd
import argparse
import getpass
import logging
import os.path
import numpy as np

from sqlalchemy import create_engine
import sqlalchemy.exc



def GetArgs():
    """
    Process command line arguments
    """
    ap = argparse.ArgumentParser(description='Get inventory of files')
    gr = ap.add_mutually_exclusive_group()
    ex = ap.add_argument_group(title='Exotic', description='Here be dragons')
    ap.add_argument('--host',      '-t', help='DB hostname or IP address')
    ap.add_argument('--user',      '-u', help='DB username')
    gr.add_argument('--password',  '-p', help='DB password')
    gr.add_argument('--blank',     '-b', help='Permit blank password', action='store_true')
    ap.add_argument('--schema',    '-s', help='DB schema', default='inventory')
    ap.add_argument('--connector', '-c', help='DB connector', default='mysql+mysqlconnector')
    ex.add_argument('--sql-debug', '-q', help='Print details of SQL commands', action='store_true')
    ex.add_argument('--verbose',   '-v', help='Verbosity', action='count')
    
    args = ap.parse_args()
    
    while not (args.blank or args.password): # Prompt for a password if required
        args.password = getpass.getpass('Password: ')

    if not args.verbose: # Is None if not specified
        args.verbose = 0

    return args

def GetPathFromFileID(dirid, engine):
    """
    Returns a full path from a directory ID
    """
    
    path = ''
    dirid = int(dirid)
    parent = dirid
    while parent:
        sql = """select name, parent from directory where id = %(id)s limit 1""" 
        (name, parent) = engine.execute(sql, {'id': parent}).fetchone()
        if parent:
            path = os.path.join(name, path)

    sql = """select job.path from job, directory 
            where job.id = directory.job_id and 
            directory.id = %(dirid)s"""
    (root,) = engine.execute(sql, {'dirid': dirid}).fetchone()

    path = os.path.join(root, path)
    return os.path.normpath(path)
    
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
        # Get the DB to return histogram bins for numbers of large
        # (> 1GB) files
        sql = """select round(size/%(size)s) as %(label)s, count(id) as N 
                from file where size > %(size)s
                group by round(size/%(size)s) 
                order by round(size/%(size)s)"""
        df = pd.read_sql_query(sql, cnx, params = {'size': 2**30, 'label': 'GB'})
        df2 = df.set_index('GB')
        df2.plot.bar(title='Large File sizes in GB')

        # Now get Pandas to analyse the amount of data created
        # on a particular day
        
        sql = """select ctime, size from file"""
        df3 = pd.read_sql_query(sql, cnx)
        df3.plot(x='ctime', y='size', style=',', logy=True)
        df4 = df3.set_index('ctime').resample('D').count()
        
        # And work out the day on which the most was created...
        
        pt = df4['size'].idxmax().date()
        sql = """select parent, count(id) as number, sum(size) as bytes  
                from file where ctime between %(pt)s and 
                DATE_ADD(%(pt)s, interval 1 day) 
                group by parent order by parent"""
        df5 = pd.read_sql_query(sql, cnx, params = {'pt':pt})
        
        # Then find out what directories it was created in
        
        df5['path'] =  np.vectorize(GetPathFromFileID)(df5['parent'], engine)
        
        # Now get all the directories where there was more than 1MB created
        # on the day and print them...
        print('Peak day for creation was {:%d %B %Y}'.format(pt))
        print('Locations of files > 1MB created on that are:')
        for _, row in df5[df5['bytes'] > 2**20].iterrows():
            print('{:80} {:6,} files {:8.1f} MB'.format(
                    row['path'], row['number'], row['bytes']/2**20))
        
        
        
