## ======================================
## Configuration

# url to elastic search
es_url = 'http://localhost:9200/'

# name of the es index
index = 'ckan-www.ckan.net'

# url to the datastore db
postgres_url = 'postgresql://ckanuser:pass@localhost/datastore'

# mapping from es types to pg types
type_mapping = {
	'string': 'text',
	'long': 'int4',
	'double': 'float8',
	'date': 'timestamp', # probably 17/04/2012 13:28:05 -> 2005-03-01
	'int': 'int4',
	'': 'tsvector',
	'nested': '_json'
}

# size for a part of the record during scrolling
chunk_size = 100

# maximum number of records to process, None if no limit
max_records = 5

# use a dumped mapping file, if available
use_dump = False
