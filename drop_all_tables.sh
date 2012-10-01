#! /bin/sh

# invoke with username and database name as arguments
# for example ./drop_all_tables.sh postgres datastore

sudo -u $1 psql -d $2 -c"select 'drop table \"' || tablename || '\" cascade;' from pg_tables where schemaname = 'public';" -t > drop_all.sql
sudo -u $1 psql -d $2 -f drop_all.sql