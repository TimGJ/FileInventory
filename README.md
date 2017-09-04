# File Inventory
Python3/SQLAlchemy scripts to do inventory of (potentially) massive
directories.
## Background
I have several (ca 10<sup>8</sup>) mostly MP3 files in a handful of
directories, with some directories containing > 10<sup>6</sup> files.
I also don't know how many of the files are duplicates
(potentially many). 

I therefore need to perform some sort of inventory on
particular directory trees. Ordinarily I could do this
using something like `os.walk()`, but, given the
potentially unknown number of files in a particular
directory this would possibly fill memory. (The directories
in question are too big for the standard Linux `ls`, `mv`
etc, and I already have to do exotic nonsense like
```
$ find foo/ -type f -print0 | xargs -0 mv -t bar/
```
and the like. 

The FileInventory software uses Python3 and SQLAlchemy to
recurse down a particular directory structure and `stat` each 
readable file it encounters therein, optionally performing
an MD5 on each file as it goes. 

(Note that the MD5 option slows things down greatly, as each 
file has to be read in to memory in its entirity, rather than merely
`stat`ed. This isn't a problem on small e.g. MP3 files, but can be on 
large e.g. MySQLDump or ISO files)

## Prerequisites

The software requires Python 3.x, the SQLAlchemy
libraries and the appropriate DB connector e.g.
`mysql-connector` for MySQL/MariaDB or `pgsyco2` for 
Postgres.

It should be platform independent, but was developed in
Linux for deployment on Linux. Obvisouly under Windows 
things like UID, GID and filemode may not yield sensible
values. 

It is a command-line tool, designed to be left running
unattended in the background for potentially days or 
weeks on large, slow system.

## Syntax

Taken from the inbuilt help:

```
tim@merlin:/metatron/Projects/FileInventory$ python
PerformInventory.py -h
usage: PerformInventory.py [-h] [--host HOST] [--user USER]
                           [--password PASSWORD | --blank]
[--schema SCHEMA]
                           [--description DESCRIPTION]
[--connector CONNECTOR]
                           [--nuke] [--md5sum] [--chunk-size
CHUNK_SIZE]
                           [--commit-rec COMMIT_REC]
[--verbose]
                           [directory [directory ...]]

Get inventory of files

positional arguments:
  directory             Directory to scan

optional arguments:
  -h, --help            show this help message and exit
  --host HOST, -t HOST  DB hostname or IP address
  --user USER, -u USER  DB username
  --password PASSWORD, -p PASSWORD
                        DB password
  --blank, -b           Permit blank password
  --schema SCHEMA, -s SCHEMA
                        DB schema
  --description DESCRIPTION, -d DESCRIPTION
                        Job description (may need quotes)
  --connector CONNECTOR, -c CONNECTOR
                        DB connector
  --nuke, -n            Drop DB tables and restart
  --md5sum, -m          Compute MD5 sum for each file

Exotic:
  Here be dragons

  --chunk-size CHUNK_SIZE, -g CHUNK_SIZE
                        Chunk size for computing MD5
  --commit-rec COMMIT_REC, -r COMMIT_REC
                        Max records after which to commit
  --verbose, -v         Verbosity
```
