#! /bin/bash

DATASTORE_LOAD=true python migrate.py config.py --segments 0123 &> migrate-q1.log &
DATASTORE_LOAD=true python migrate.py config.py --segments 4567 &> migrate-q2.log &
DATASTORE_LOAD=true python migrate.py config.py --segments 89ab &> migrate-q3.log &
DATASTORE_LOAD=true python migrate.py config.py --segments cdef &> migrate-q4.log &