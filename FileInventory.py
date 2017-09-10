#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Aug 27 16:49:41 2017

Classes to do an inventory of files on a disk. 

Designed to be used for HUGE directories which would
contain too many entries to use something like e.g. os.walk
which returns lists of the entire contents. 

@author: tim
"""


import os
import os.path
import argparse
import itertools
import datetime
import hashlib
import logging
import re
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, BigInteger, String, DateTime, Boolean, ForeignKey
from sqlalchemy.orm import validates

Base = declarative_base()

class Job(Base):
    
    MaxOwnerNameLength =  20
    MaxHostNameLength  =  20
    MaxCommentLength   =  80
    
    __tablename__ = 'job'
    id      = Column(Integer, primary_key = True)
    started = Column(DateTime, default=datetime.datetime.now)
    ended   = Column(DateTime)
    owner   = Column(String(MaxOwnerNameLength))
    host    = Column(String(MaxHostNameLength))
    comment = Column(String(MaxCommentLength))
    md5sum  = Column(Boolean, nullable = True)

class Directory(Base):
    
    Bates = itertools.count(1)
    MaxDirNameLength = 255
    NameRe           = re.compile("[^-\w\s_\./-]")    
    __tablename__ = 'directory'
    id      = Column(Integer, primary_key=True)
    job_id  = Column(Integer, ForeignKey('job.id', ondelete='CASCADE'), nullable=False)
    serial  = Column(Integer)
    parent  = Column(Integer, ForeignKey('directory.id', ondelete='CASCADE'), nullable=True, index=True)
    name    = Column(String(MaxDirNameLength), index=True)
    ctime   = Column(DateTime)
    mtime   = Column(DateTime)
    atime   = Column(DateTime)
    mode    = Column(Integer)
    size    = Column(BigInteger)
    uid     = Column(Integer)
    gid     = Column(Integer)
    
    @validates('name')
    def ValidateName(self, key, value):
        """
        It is possible for files to be created with strange characters in
        their names. This breaks MySQL varchar() quite horribly, so strip
        nonsense characters out.
        """
        if type(value) is bytes:
            name = value.decode('utf-8')
        else:
            name = str(value)
        return Directory.NameRe.sub('?', name)

    def __repr__(self):
        return "ID={} Parent={} Name={}".format(self.id, self.parent, self.name)
    
    def __lt__(self, other):
        return self.name < other.name

class File(Base):
    
    Bates = itertools.count(1)
    MaxFileNameLength =   255 # I've never seen one this long, fnarr, frnarr, but it is possible
    MD5SumLength      =    32
    DefaultMD5Chunk   = 1<<24 # 16 MiB
    NameRe            = re.compile("[^\w\s_\.-]")
    
    __tablename__ = 'file'
    id      = Column(Integer, primary_key = True)
    serial  = Column(Integer)
    parent  = Column(Integer, ForeignKey('directory.id', ondelete='CASCADE'), nullable=False)
    name    = Column(String(MaxFileNameLength), index=True)
    ctime   = Column(DateTime)
    mtime   = Column(DateTime)
    atime   = Column(DateTime)
    mode    = Column(Integer)
    size    = Column(BigInteger)
    uid     = Column(Integer)
    gid     = Column(Integer)
    md5sum  = Column(String(MD5SumLength), index=True)
    
    @validates('name')
    def ValidateName(self, key, value):
        """
        It is possible for files to be created with strange characters in
        their names. This breaks MySQL varchar() quite horribly, so strip
        nonsense characters out.
        """
        name = value.decode('utf-8')
        return File.NameRe.sub('?', name)
    
    def __repr__(self):
        return "ID={} Parent={} Name={}".format(self.id, self.parent, self.name)
    
    def __lt__(self, other):
        return self.name < other.name


def MD5(filename, block_size=File.DefaultMD5Chunk):
    """
    Compute the MD5 of a file. Ordinarily we would just slurp the
    file in to memory and compute its sum, but as we might encounter
    huge files (e.g. several GB), this might be too much to fit in
    available memory. So break the file in to manageable chunks.
    """
    try:
        with open(filename, 'rb') as f:
            md5 = hashlib.md5()
            while True:
                data = f.read(block_size)
                if not data:
                    break
                md5.update(data)
            return md5.hexdigest()    
    except FileNotFoundError as e:
        logging.error('Can\'t open {} for reading: {}'.format(filename, e))

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
    ex.add_argument('--chunk-size',  '-g', help='Chunk size for computing MD5', default = File.DefaultMD5Chunk)
    ex.add_argument('--commit-rec',  '-r', help='Max records after which to commit', default = File.MaxCommitRecords)
    ex.add_argument('--verbose',     '-v', help='Verbosity', action='count')

    ap.add_argument('dirs', metavar ='directory', help='Directory to scan', nargs='*', default=[os.getcwd()])
    return ap.parse_args()

