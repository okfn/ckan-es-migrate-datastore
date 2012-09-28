## ======================================
## Configuration


config = {
    # url to elastic search
    'es_url': 'http://localhost:9200/',

    # name of the es index
    'index': 'ckan-www.ckan.net',

    # url to the datastore db
    'postgres_url': 'postgresql://ckanuser:pass@localhost/datastore',

    # url to the ckan meta data catalog
    'ckan_postgres_url': 'postgresql://ckanuser:pass@localhost/ckan',

    # mapping from es types to pg types
    'type_mapping': {
        'string': 'text',
        'long': 'int8',
        'double': 'float8',
        'date': 'timestamp',
        'integer': 'int4',
        'nested': 'json',
        'geo_point': '_float8'
    },

    # size for a part of the record during scrolling
    'chunk_size': 100,

    # use a dumped mapping file, if available or create it
    'use_dump': True,

    # ignore errors when saving data to the datastore and log the incident
    'ignore_exceptions': False,

    # filter resource ids by those available in the meta-data catalogue
    'filter_resource_ids': False
}