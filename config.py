## ======================================
## Configuration


config = {
    # url to elastic search
    'es_url': 'http://localhost:9200/',

    # name of the es index
    'index': 'ckan-www.ckan.net',

    # url to the datastore db
    'postgres_url': 'postgresql://ckanuser:pass@localhost/datastore',

    # mapping from es types to pg types
    'type_mapping': {
        'string': 'text',
        'long': 'int8',
        'double': 'float8',
        'date': 'timestamp',
        'int': 'int4',
        'nested': 'json'
    },

    # size for a part of the record during scrolling
    'chunk_size': 100,

    # use a dumped mapping file, if available or create it
    'use_dump': False,

    # ignore errors when saving data to the datastore and log the incident
    'ignore_exceptions': False
}