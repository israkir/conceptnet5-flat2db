Description
===========

This project contains the scripts for relational database creation for [ConceptNet5](http://conceptnet5.media.mit.edu/), if you want to analyze its knowledge graph offline. 

Originally it is written for PostgreSQL, however you can tweak the `dbschema.py` script to adopt it to any other database engine.

An additional schema diagram `schema.png` is added to the package, so you can check the relations between tables visually.


Dependencies
============

1. `python-sqlalchemy` package for `dbschema.py`.
2. `python-psycopg2` package for `flatparser.py`.


Instructions
============

1. Fill the username, password, host, database name information in both `dbschema.py` and `flatparser.py` scripts.

2. To create database tables:

    $ python dbschema.py

3. Put ConceptNet5 flat files in to a directory; e.g. '/home/data/'

4. Starting parsing the data files and add data to database tables:

    $ python flatparser.py /home/data/

Also:

1. I would recommend to run `flatparser.py` as a background process using `nohup`:

    $ nohup python flatparser.py > log.out &

2. To see stdout:
    
    $ tail -f log.out


Acknowledgments
===============

Thanks to [jason2506](https://github.com/jason2506) for helping on `dbschema.py` ;)
