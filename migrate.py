#!/usr/bin/env python
'''
A basic script to migrate from elasticsearch to the ckan datastore.

For command line usage do:
	python migrate.py -h
'''

import urllib2, urllib
import json
from time import gmtime, strftime
from dateutil.parser import *
from datetime import *
import os, imp
import logging
from pprint import pprint as pp
import hashlib

import ckanext.datastore.db as db

logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger('migrate')
logger.setLevel(logging.INFO)

class Migrate(object):
	def __init__(self, config = {}, **args):
		self.__dict__.update(config.config)

		self.__dict__.update(args)

		self.url = self.es_url + self.index

		self.active_resource_id = None # only for logging

	def run(self):
		'''
		Migrates resources from elastic search to the datastore. 
		A start resource id can be defined so that in case of an error the migration
		can continue from a certain point without redoing the previous migrations. 
		'''
		self.mapiter = self._mapping_iterator()
		
		started_at = 0
		for i, (resource_id, properties) in enumerate(self.mapiter):
			if self.start_id and resource_id != self.start_id:
				logger.debug("Jumping over {resid}".format(resid=resource_id))
				continue
			elif self.start_id and resource_id == self.start_id:
				started_at = i
				self.start_id = None

			if self.max_records and i - started_at >= self.max_records:
				break
			
			logger.info("Processing resource nr {nr} with id {resid}".format(nr=i, resid=resource_id))
			self.active_resource_id = resource_id
			data_dict = self._process_resource(resource_id, properties)

	def _process_resource(self, resource_id, properties):
		fields = self._extract_fields(properties['properties'])

		records = []
		for records_chunk in self._scan_iterator(resource_id, fields):
			data_dict = {'resource_id': resource_id, 'fields': fields, 'records': records_chunk}
			if not self.simulate:
				self._save(data_dict)

		return data_dict

	def _process_chunk(self, scroll_id, fields):
		'''
		Processes one chunk = one part of the resource during scrolling
		'''
		resource_url = self.es_url + '_search/scroll?scroll=10m'
		post = scroll_id
		
		logger.debug("Processing chunk with schroll id: {id}".format(id=scroll_id))

		results = self._request(resource_url, post)

		#pp(results)
		count = len(results['hits']['hits'])
		records = self._extract_records(results['hits']['hits'], fields)
		new_scroll_id = results['_scroll_id']

		return records, new_scroll_id, count

	def _extract_fields(self, properties):
		#pp(properties)
		fields = []
		for p_id, value in properties.items():
			field = {'id': self._validate_field_name(p_id)}
			field['type'] = self.type_mapping[value['type']]
			if value.has_key('format'):
				field['format'] = p_format = value['format']
			fields.append(field)
		#pp(fields)
		return fields

	def _extract_records(self, hits, fields):
		records = []
		for hit in hits:
			record = {}
			for key, value in hit['_source'].items():
				key = self._validate_field_name(key)

				# ignore fields that are not in the mapping
				field = [x for x in fields if x['id'] == key]
				if len(field) == 1:
					field = field[0]
					if field['type'] == 'text':
						value = unicode(value)
					elif field['type'] == 'timestamp':
						logger.debug("Found a date: {date}".format(date=value))
						value = str(parse(value)) # field['format']
					record[key] = value
				else:
					logger.warn("Found a field that is not in the mapping: {fieldname} in {resource}".format(fieldname=key, resource=self.active_resource_id))
			records.append(record)
		return records

	def _mapping_iterator(self):
		'''
		Iterator for resources from the mapping. The mapping can be dumped in order to 
		use the same mapping for multiple runs. 
		'''
		mapping = None

		path = os.path.join(os.path.dirname(__file__), 'dump_' + self.index + '.json')
		if os.path.exists(path) and self.use_dump:
			logger.info("Use dumped mapping: {path}".format(path=path))
			with open(path, 'r') as dump:
				mapping = json.loads(dump.read())
		
		if not mapping:
			mapping_url = self.url + '/' + '_mapping'
			logger.info("Mapping url: {url}".format(url=mapping_url))
			mapping = self._request(mapping_url)

			if self.use_dump:
				logger.info("Dumped mapping to: {path}".format(path=path))
				with open(path, 'w+') as dump:
					dump.write(json.dumps(mapping))

		for key, value in mapping['ckan-www.ckan.net'].items():
			yield key, value

	def _scan_iterator(self, resource_id, fields):
		'''
		Iterator for scan searches
		'''
		resource_url = self.url + '/' + resource_id + '/_search'

		get = {
			'search_type': 'scan',
			'scroll': '10m', # lifetime 
			'size': self.chunk_size
		}
		post = {
			'query': {
				"match_all" : {}
			}
		}

		resource_url = resource_url + '?' + urllib.urlencode(get)

		logger.debug("Processing resource: {resource}".format(resource=resource_id))
		logger.debug("Scan url: {url}".format(url=resource_url))

		scroll = self._request(resource_url, post)

		scroll_id = scroll['_scroll_id']
		total = scroll['hits']['total']
		
		logger.debug("Initial scroll id: {id}".format(id=scroll_id))

		logger.info("Found {total} records".format(total=total))

		count = True
		while count:
			records_chunk, scroll_id, count = self._process_chunk(scroll_id, fields)
			yield records_chunk

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
			logger.error('%s: %s' % (inst.url, inst.read()))
			raise
		return json.loads(out)

	def _validate_field_name(self, name):
		'''
		tries to clean the field name
		'''
		
		# strip ", whitespaces and _
		name = name.strip().strip('"').strip("_").strip()

		# replace all " with '
		name = name.replace('"', "'")

		# truncate because of maximum field size of 63 in pg
		if len(name) >= 63:
			name = name[:56] + hashlib.md5(name).hexdigest()[:6]
		
		return name

	def _save(self, data_dict):
		context = {}
		data_dict['connection_url'] =  self.postgres_url
		result = db.create(context, data_dict)


## ======================================
## Command line interface

if __name__ == '__main__':
	import argparse

	parser = argparse.ArgumentParser(
		description='Migrate from elasticsearch to the CKAN datastore', 
		epilog='"He reached out and pressed an invitingly large red button on a nearby panel. The panel lit up with the words Please do not press this button again."')
	
	parser.add_argument('config', metavar='CONFIG', type=file, 
	                   help='configuration file')

	parser.add_argument('--max', metavar='N', type=int, default=None, dest='max_records',
	                   help='maximum number of records to process (default is unlimited)')
	parser.add_argument('--start', metavar='START-RES-ID', type=str, default=None, dest='start',
	                   help='resource id to start with (default is the beginning)')
	parser.add_argument('-s', '--simulate', action='store_true', dest='simulate', default=False, 
	                   help="don't store anything in the database")

	args = parser.parse_args()

	config = imp.load_source('config', args.config.name)

	m = Migrate(config, start_id=args.start, simulate=args.simulate, max_records=args.max_records)
	m.run()
