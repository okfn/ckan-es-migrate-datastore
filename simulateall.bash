#! /bin/bash

DATASTORE_LOAD=true python migrate.py config.py -s --segments 0123 &> migrate-q1.log &
DATASTORE_LOAD=true python migrate.py config.py -s --segments 4567 &> migrate-q2.log &
DATASTORE_LOAD=true python migrate.py config.py -s --segments 89ab &> migrate-q3.log &
DATASTORE_LOAD=true python migrate.py config.py -s --segments cdef &> migrate-q4.log &