#!/usr/bin/python

from optparse import OptionParser
from pymongo import MongoClient

def main():
	parser = OptionParser()
	parser.add_option('-i', '--infile', dest='in_file', action='store', help='iceplant input file')
	parser.add_option('-o', '--outfile', dest='out_file', action='store', help='iceplant output file')
	parser.add_option('-s', '--server', dest='server', action='store', help='MongoDB server', default='localhost')
	parser.add_option('-p', '--port', dest='port', type='int', action='store', help='MongoDB port', default='27017')
	parser.add_option('-d', '--db', dest='db', action='store', help='database name', default='uniprot')

	option, arg = parser.parse_args()

	client = MongoClient(option.server, option.port)
	db = client[option.db]
	collection = db['uniprot_kb']

	map_file(option.in_file, option.out_file, collection)

def map_file(in_file, out_file, col):
	with open(in_file) as i, open(out_file, 'w') as o:
		for line in i:
			data = line.strip('\n').split('\t')
			if len(data) != 2:
				raise DataError('Expected 2 columns, read {0}.'.format(len(data)))

			if data[1] == '':
				data.append('')
			elif data[1].startswith('UPI'):
				query = col.find_one({'UniParc': data[1]})
				if query:
					data.append(to_semi_sep(query['GO']))
			elif data[1].startswith('UniRef50'):
				query = col.find_one({'UniRef50': data[1]})
				if query:
					data.append(to_semi_sep(query['GO']))
			else:
				query = col.find_one({'UniProtKB-ID': data[1]})
				if query:
					data.append(to_semi_sep(query['GO']))

			o.write(to_tab_sep(data) + '\n')

def to_semi_sep(l):
	if isinstance(l, list):
		return '; '.join(l)
	return l

def to_tab_sep(l):
	if isinstance(l, list):
		return '\t'.join(l)
	return l

class DataError(Exception):
	pass

if __name__ == '__main__':
	main()