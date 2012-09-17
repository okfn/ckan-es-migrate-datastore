# Migration script

Migrate [elasticsearch](http://www.elasticsearch.org) to the [CKAN](http://ckan.org/) datastore. This script depends on the new [CKAN datastore libary](https://github.com/okfn/ckan/tree/2733-feature-datastore) to load the data into CKAN. 

The script is configured with command line parameters (for frequently changing preferences) and the config file. Make yourself familiar with the settings and try running the script in simulation mode first. 

# Good to know

* Simulation means that nothing is written to the db.
* Use the `segments` option to run parts of the migration in parallel (see `runall.bash` and `simulateall.bash`).
* The migration can be run without an installed CKAN. Just copy the `db.py` from `\ckanext\datastore` to the same directory that the `migration.py` is in.
* Use `tail -f <logfile>` to see what's happening if you pipe the output to a log file.


## Requirements
* new CKAN Datastore (at least the `db.py`)
* `dateutil` (install with `pip install python-dateutil`)

## In order to clean the database (in case something goes wrong), use this command to delete all tables.

    select 'drop table "' || tablename || '" cascade;' 
    from pg_tables where schemaname = 'public';