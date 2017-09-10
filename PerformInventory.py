# -*- coding: utf-8 -*-
"""
Spyder Editor

Performs an inventory of files in a directory tree and sticks 
deailts of them in a DB. This is do perform an inventory of
~ 10**8 MP3 files, and it is therefore impossible to use tools such as 
os.walk().

Essentially this replicates the functionality of the standard 
Linux tools find (to find the files), stat (to get the details)
and md5sum (to compute the hash)

Tim Greening-Jackson 30 August 2017
"""

import sys
import os
import os.path
import argparse
import socket
import getpass
import datetime
import logging
import mysql.connector
import mysql.connector.errors

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import sqlalchemy.exc

import FileInventory


def GetArgs():
    """
    Process command line arguments
    """
    ap = argparse.ArgumentParser(description='Get inventory of files')
    gr = ap.add_mutually_exclusive_group()
    ex = ap.add_argument_group(title='Exotic', description='Here be dragons')
    ap.add_argument('--host',        '-t', help='DB hostname or IP address', default='merlin')
    ap.add_argument('--user',        '-u', help='DB username', default='tim')
    gr.add_argument('--password',    '-p', help='DB password')
    gr.add_argument('--blank',       '-b', help='Permit blank password', action='store_true')
    ap.add_argument('--schema',      '-s', help='DB schema', default='inventory')
    ap.add_argument('--description', '-d', help='Job description (may need quotes)')
    ap.add_argument('--connector',   '-c', help='DB connector', default='mysql+mysqlconnector')
    ap.add_argument('--nuke',        '-n', help='Drop DB tables and restart', action='store_true')
    ap.add_argument('--md5sum',      '-m', help='Compute MD5 sum for each file', action='store_true')
    ex.add_argument('--sql-debug',   '-q', help='Print details of SQL commands', action='store_true')
    ex.add_argument('--verbose',     '-v', help='Verbosity', action='count')

    ap.add_argument('dirs', metavar ='directory', help='Directory to scan', nargs='*', default=[os.getcwd()])
    return ap.parse_args()



def ProcessDirectory(session, directory, job_id, compute_md5, parent=None):
    """
    Scans the files in a directory, and sticks them in the 
    database. Note that as a directory potentially contains
    several million records, we commit on a regular basis to
    avoid memory exhaustion.
    
    Parameter root is a flag indicating whether this directory is the
    root of the tree we are searching, so is True for the first call,
    but False for any recursions.
    """
    try:
        if os.path.isdir(directory):
            logging.info("Processing directory {}".format(directory))
    
            d = FileInventory.Directory(name = directory, job_id = job_id, 
                                        parent = parent, 
                                        serial = next(FileInventory.Directory.Bates))
            session.add(d)
            session.commit() # Do a commit here as sometimes the DB gets behind itself and we get a FK integrity error
            try:
                with os.scandir(directory) as di:
                    for entry in di:
                        if entry.is_dir():
                            ProcessDirectory(session, os.path.join(directory, entry.name), 
                                             job_id, compute_md5, d.id)
                        if entry.is_file():
                            logging.debug('Processing file {}'.format(entry.name))
                            try:
                                st = entry.stat()
                            except FileNotFoundError:
                                logging.warning("No such file or directory {}".format(entry.name))
                            else:
                                f = FileInventory.File(serial = next(FileInventory.File.Bates), parent = d.id, 
                                         name = entry.name.encode('utf8')[-FileInventory.File.MaxFileNameLength:],
                                         atime = datetime.datetime.fromtimestamp(st.st_atime), 
                                         mtime = datetime.datetime.fromtimestamp(st.st_mtime), 
                                         ctime = datetime.datetime.fromtimestamp(st.st_ctime), 
                                         mode = st.st_mode, uid = st.st_uid, gid = st.st_gid,
                                         size = st.st_size)
                                if compute_md5:
                                    f.md5sum = FileInventory.MD5(os.path.join(directory, entry.name))
                                try:
                                    session.add(f)
                                    session.commit()
                                except mysql.connector.errors.Error as e:
                                    logging.error("Error committing file {}: {}".format(f.name, e))
                                    session.rollback()

            except PermissionError as e:
                logging.error("Can't read directory {}: {}".format(directory, e))
        else:
            logging.warning("{} is not a directory.".format(directory))
    except KeyboardInterrupt:
        # We check for keyboard interrupt (Ctrl-C) not only to handle such situations
        # gracefully but also becuase if we haven't committed this can cause table
        # locks which (in extremis) mean we might have to restart the database
        logging.error("Job interrupted by user!")
        session.commit()
        session.close()
        sys.exit(0)
    session.commit()
    
if __name__ == '__main__':
    args = GetArgs()
    while not (args.blank or args.password): # Prompt for a password if required
        args.password = getpass.getpass()

    if not args.verbose: # Is None if not specified
        args.verbose = 0
        
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
        engine = create_engine(connectstr, echo = True if args.sql_debug else False)
    except sqlalchemy.exc.NoSuchModuleError as e:
        logging.critical("Error creating engine: {}".format(e))
    else:
        Session = sessionmaker(bind=engine)
        try:
            if args.nuke:
                logging.info("Dropping existing tables")
                FileInventory.File.__table__.drop(engine, checkfirst = True)
                FileInventory.Directory.__table__.drop(engine, checkfirst = True)
                FileInventory.Job.__table__.drop(engine, checkfirst = True)
            FileInventory.Base.metadata.create_all(engine, checkfirst = True)
        except sqlalchemy.exc.ProgrammingError as e:
            logging.critical("Error creating tables: {}".format(e))
        else:
            logging.info("Creating session")
            session = Session()
            if args.description:
                args.description = args.description[:FileInventory.Job.MaxCommentLength]
            job = FileInventory.Job(host=socket.gethostname(), owner=args.user, 
                      comment=args.description,
                      md5sum = True if args.md5sum else False)
            session.add(job)
            session.commit()
            for d in args.dirs:
                ProcessDirectory(session, d, job.id, args.md5sum)                    
            job.ended = datetime.datetime.now()
            session.commit()
            logging.info("Closing session")
            session.close()
