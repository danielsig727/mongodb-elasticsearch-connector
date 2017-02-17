#!/usr/bin/env python

import argparse
from elasticsearch import Elasticsearch
import logging
import os
from pymongo import MongoClient
import pymongo
import bson
import datetime


def convert_ObjectId(doc):
    if isinstance(doc, dict):
        iter = doc.iteritems()
    elif isinstance(doc, list):
        iter = enumerate(doc)
    else:
        raise RuntimeError()

    for (k, v) in iter:
        if isinstance(v, bson.objectid.ObjectId):
            doc[k] = str(v)
        elif isinstance(v, list) or isinstance(v, dict):
            doc[k] = convert_ObjectId(v)

    return doc


def sanitize_document(document, fields=[]):
    """
    Removes fields from a MongoDB document.

    Args:
        document: The MongoDB document to sanitize.
        fields: An array of fields to check against. Each field is treated as a substring
                        of keys within the MongoDB document.

    Returns:
        A sanitized version of the document.
    """
    for field in document.keys():
        if field in fields:
            document.pop(field, None)

    return convert_ObjectId(document)


def send_to_elasticsearch(document, docid, index, doc_type='mongodb', client=None):
    """
    Sends the document to Elasticsearch.
    """

    logging.debug('Indexing document: %s', docid)
    client.index(index=index, doc_type=doc_type, id=docid, body=document)


def process_collection(database, collection, index, blacklist=[], db_client=None, es_client=None, desc_field=None):
    """
    Iterates over a collection in MongoDB. Each document in the collection is sanitized and sent
    to Elasticsearch.
    """
    es_doc_type = collection.lower()
    if desc_field:
        # find the latest in es
        res = es_client.search(index=index, doc_type=es_doc_type,
                               body={"query": {"match_all": {}},
                                     "size": 1,
                                     "sort": {desc_field: "desc"}})
        timestamp = res['hits']['hits'][0]['sort'][0]
        timestamp = datetime.datetime.fromtimestamp(timestamp/1000.0)
        query = {desc_field: {"$gt": timestamp}}
    else:
        query = {}

    logging.info('query: %s', str(query))
    collection_data = db_client[database][collection]

    qresult = collection_data.find(query, no_cursor_timeout=True)
    if desc_field:
        qresult = qresult.sort(desc_field, pymongo.DESCENDING)

    logging.info('Processing %0d documents', qresult.count())
    # return

    first_doc = None
    for entry in qresult:
        docid = str(entry['_id'])
        doc = sanitize_document(entry, blacklist)
        if not first_doc:
            first_doc = doc
        send_to_elasticsearch(doc, docid, index, doc_type=es_doc_type, client=es_client)
        # break

    qresult.close()
    return first_doc


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Python script to export MongoDB collections to Elasticsearch.')
    parser.add_argument('-v', '--verbose', help='Increase output verbosity', action='store_true')
    parser.add_argument('-d', '--debug', help='Increase output verbosity for debugging', action='store_true')
    parser.add_argument('--mongo_host', help='Address of the MongoDB host.')
    parser.add_argument('--database',
                        default=os.environ.get('MONGO_DATABASE'),
                        help='MongoDB database to use.')
    parser.add_argument('--collection',
                        default=os.environ.get('MONGO_COLLECTION'),
                        help='MongoDB collection to export.')
    parser.add_argument('--elasticsearch_host',
                        default=os.environ.get('ES_HOST', 'localhost'),
                        help='Address of the Elasticsearch host.')
    parser.add_argument('--elasticsearch_port',
                        default=os.environ.get('ES_PORT', 9200), type=int,
                        help='Port to connect to Elasticsearch on.')

    parser.add_argument('--desc_field',
                        default=None,
                        help='field to sort descent (should be time)')
    parser.add_argument('--index',
                        default=os.environ.get('ES_INDEX'),
                        help='Index in Elasticsearch to store the collection\'s data.')

    fields = os.environ.get('MONGO_BLACKLIST', [])
    if fields:
        fields = fields.split(',')
    parser.add_argument('--blacklist', default=fields, nargs="+",
                        help='Fields to sanitize out of the MongoDB entries.')
    args = parser.parse_args()

    if args.debug:
        level = logging.DEBUG
    elif args.verbose:
        level = logging.INFO
    else:
        level = logging.WARN
    logging.basicConfig(format='%(asctime)s - %(name)s:%(levelname)s - %(message)s', level=level)

    index = args.index or 'mongodb-{}-{}'.format(args.database, args.collection)

    logging.warn('Starting export of %s.%s to %s', args.database, args.collection, index)

    index = index.lower()
    es_client = Elasticsearch([{'host': args.elasticsearch_host, 'port': args.elasticsearch_port}])
    # ignore 400 cause by IndexAlreadyExistsException when creating an index
    es_client.indices.create(index=index, ignore=400)

    db_client = MongoClient(args.mongo_host)

    # def ascii_encode_dict(data):
    #     ascii_encode = lambda x: x.encode('ascii') if isinstance(x, unicode) else x
    #     return dict(map(ascii_encode, pair) for pair in data.items())

    first_doc = process_collection(args.database, args.collection, index,
                                   blacklist=args.blacklist,
                                   db_client=db_client, es_client=es_client,
                                   desc_field=args.desc_field)

    db_client.close()

    logging.warn('Finished export')

    # print 'first_doc={}'.format(str(first_doc))
    # if args.desc_field:
    #     # time = first_doc[args.desc_field].strftime('%Y-%m-%d %H:%M:%S.%fZ')
    #     time = int((first_doc[args.desc_field]-datetime.datetime.utcfromtimestamp(0)).total_seconds()*1000)
    #     print '{"%s": { "$gt": { "$date": %d }}}' % (args.desc_field, time)
