# Migration script

Migrate [elasticsearch](http://www.elasticsearch.org) to the [CKAN](http://ckan.org/) datastore. This script depends on the new [CKAN datastore libary](https://github.com/okfn/ckan/tree/2733-feature-datastore) to load the data into CKAN. 

The script is configured with command line parameters (for frequently changing preferences) and the config file. Make yourself familiar with the settings and try running the script in simulation mode first. 

## How to migrate from the Elasticsearch datastore to the new datastore

_Note:_ This step by step introduction shows a possible migration. Some steps may differ in your setup.

* Create a PostgreSQL database for the datastore and a read only user as described in [the docs](http://docs.ckan.org/en/latest/datastore.html).
* Either have CKAN installed and the migration script in `ckanext/migration/` or copy the `db.py` from `\ckanext\datastore` to the same directory that the `migration.py` is in. The second option allows you to run the migration without having CKAN on the same server or even installed.
* Make sure you have all requirements installed.
* Adapt the `config.py` to your needs.
* Make yourself familiar with the command line options. The option `-h` will show possible command line options and some explanations. 
* Make sure that the settings are correct

Commands:

    # simulate the migration of one resource
    $ python migrate.py -s --max 1 config.py
    
    # If no errors occur, try writing to the db
    $ python migrate.py -s --max 1 config.py
    
    # Okay, cool. Let's clear the database and start the real migration. 
    # Go into the datastore db and clear the table that has been created.
    $ psql …
    
    # You can run the migration in parallel to speed things up. Let's try the simulation first.
    ./simulateall.bash
    
* Start the migration

Commands:

    # If you want to run the migration in parallel (change the file to your needs!)
    ./runall.bash
    
    # Serial execution
    $ python migrate.py config.py

* Monitor the progress of the migration. Use `tail -f <logfile>` to see what's happening if you pipe the output to a log file.
* Enjoy the new datastore

## Good to know

* Simulation means that nothing is written to the db.
* Use the `segments` option to run parts of the migration in parallel (see `runall.bash` and `simulateall.bash`).


## Requirements
* new CKAN Datastore (at least the `db.py`)
* `dateutil` (install with `pip install python-dateutil`)
* `sqlalchemy`

## In order to clean the database (in case something goes wrong), use this command to delete all tables.

    select 'drop table "' || tablename || '" cascade;' 
    from pg_tables where schemaname = 'public';