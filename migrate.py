import ckanext.datastore.db as db
import urllib2
import urllib
import json
from pprint import pprint as pp
import logging
from time import gmtime, strftime

## Configuration
es_url = 'http://localhost:9200/'
node = 'ckan-www.ckan.net'
postgres_url = 'postgresql://ckanuser:pass@localhost/datastore'
type_mapping = {
	'string': 'text',
	'long': 'int4',
	'double': 'float8',
	'date': 'timestamp', # probably 17/04/2012 13:28:05 -> 2005-03-01
	'int': 'int4',
	'': 'tsvector',
	'nested': '_json'
}
chunk_size = 50

# TODO start at a certain record (in case something fails)

logging.basicConfig(level=logging.DEBUG)

class Migrate(object):
	def __init__(self):
		self.mapiter = self._mapping_iterator()

	def run(self):
		# jump over n entries
		for _ in range(3):
			r, p = self.mapiter.next()
		for _ in range(1):
			r, p = self.mapiter.next()
			self._process_resource(r, p)
		
		#for resource_id, properties in self.mapiter:
		#	print resource_id
			#self._process_resource(resource_id, properties)

	def _mapping_iterator(self):
		# TODO: Use dump
		mapping_url = es_url + node + '/' + '_mapping'

		logging.info("Mapping url: {url}".format(url=mapping_url))

		mapping = self._request(mapping_url)

		for key, value in mapping['ckan-www.ckan.net'].items():
			yield key, value

	def _scan_iterator(self, resource_id, fields):
		resource_url = es_url + "/" + node + '/' + resource_id + '/_search'

		get = {
			'search_type': 'scan',
			'scroll': '2m', # lifetime 
			'size': chunk_size
		}
		post = {
			'query': {
				"match_all" : {}
			}
		}

		resource_url = resource_url + '?' + urllib.urlencode(get)

		logging.info("Processing resource: {resource}".format(resource=resource_id))
		logging.info("Scan url: {url}".format(url=resource_url))

		scroll = self._request(resource_url, post)

		scroll_id = scroll['_scroll_id']
		total = scroll['hits']['total']
		
		logging.info("Initial scroll id: {id}".format(id=scroll_id))

		logging.info("Found {total} records".format(total=total))

		count = True
		while count:
			records_chunk, scroll_id, count = self._process_chunk(es_url, scroll_id, fields)
			yield records_chunk

	def _process_resource(self, resource_id, properties):
		fields = self._extract_fields(properties['properties'])

		records = []
		for records_chunk in self._scan_iterator(resource_id, fields):
			records += records_chunk

		data_dict = {'resource_id': resource_id, 'fields': fields, 'records': records}

		#pp(data_dict)

	def _process_chunk(self, url, scroll_id, fields):
		resource_url = url + '_search/scroll?scroll=2m'
		post = scroll_id
		
		logging.info("Processing chunk with schroll id: {id}".format(id=scroll_id))

		results = self._request(resource_url, post)

		#pp(results)
		count = len(results['hits']['hits'])
		records = self._extract_records(results['hits']['hits'], fields)

		return records, scroll_id, count

	def _extract_fields(self, properties):
		#pp(properties)
		fields = []
		for p_id, value in properties.items():
			p_type = value['type']
			fields.append({'id': p_id, 'type': type_mapping[p_type]})
		return fields

	def _extract_records(self, hits, fields):
		#pp(hits)
		records = []
		for hit in hits:
			record = {}
			for key, value in hit['_source'].items():
				record[key] = value
			records.append(record)
		return records

	def _request(self, url, query = None):
		'''Perform a request on ElasticSearch endpoint.
		:param url: a url for the request
		:param query: a dictionary specifying the elasticsearch query as per
		elastic search spec or other data for the post request
		'''
		if query and type(query) == dict:
			query = json.dumps(query)
		req = urllib2.Request(url, query)

		try:
			out = urllib2.urlopen(req).read()
		except Exception, inst:
			logging.error('%s: %s' % (inst.url, inst.read()))
			raise
		return json.loads(out)

	def _save(self, data_dict):
		context = {}
		data_dict['connection_url'] =  postgres_url
		result = db.create(context, data_dict)

# expected format
"""data_dict = {
	'resource_id': '123',
	'fields': [{'id': 'book', 'type': 'text'},
			   {'id': 'author', 'type': 'text'}],
	'records': [{'book': 'annakarenina', 'author': 'tolstoy'},
				{'book': 'warandpeace', 'name': 'tolstoy'}]
}"""

if __name__ == '__main__':
	m = Migrate()
	m.run()