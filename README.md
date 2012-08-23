# Migration script

Migrate [elasticsearch](http://www.elasticsearch.org) to the [CKAN](http://ckan.org/) datastore. This script depends on the new [CKAN datastore libary](https://github.com/okfn/ckan/tree/2733-feature-datastore) to load the data into CKAN. 

The script is configured through 
command line parameters (for frequently changing preferences) and the config file. 
Make yourself familiar with the settings and try running the script in simulation 
mode first. 

## Requirements
* new CKAN Datastore
* `dateutil` (install with `pip install python-dateutil`)

## In order to clean the database (in case something goes wrong), use this command to delete all tables.

    select 'drop table "' || tablename || '" cascade;' from pg_tables where schemaname = 'public';