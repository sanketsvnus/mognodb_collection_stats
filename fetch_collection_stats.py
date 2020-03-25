import logging
import sys
import argparse
import pymongo
from pymongo import MongoClient
import os
from pprint import pprint
from functools import wraps
import time
from pymongo import errors
from datetime import datetime, timedelta
import copy

# logger configuration
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(name)s %(levelname)s %(message)s')
logger = logging.getLogger(os.path.basename(sys.argv[0]))

# To run 'collstats' on these mongodb internal databases/collections,
# typically user needs "__system" privilege. In many prod environments
# no users would have this privilege, so ignoring it right now
mongodb_internal_databases = ['admin', 'local']
mongodb_internal_collections = ['system.sessions']


metrics_to_be_fetched = ['collection_docs_count', 'collection_size_bytes', 'index_size_bytes']


# General function which can be used to reattempt mongodb functions involving mongodb operations.
def auto_reattempt(func):
    @wraps(func)
    def wrapped_func(*l_args, **l_kwargs):
        num_of_retries = 5
        attempt_number = 1
        while num_of_retries:
            try:
                # print(args[0])
                return func(*l_args, **l_kwargs)
            except (errors.AutoReconnect, errors.ConnectionFailure, errors.ExecutionTimeout,
                    errors.NetworkTimeout, errors.ServerSelectionTimeoutError,
                    errors.WTimeoutError, errors.WriteConcernError, errors.OperationFailure) as atmf:
                logger.exception('Error occurred while tagging RpClientId to shard. details: %s. AttemptNumber: %s ',
                                 atmf.details, attempt_number)
                num_of_retries -= 1
                attempt_number += 1
                time.sleep((attempt_number * 10) / 1000)

    return wrapped_func


@auto_reattempt
def get_collection_stats(l_db, l_collection):
    """
    Run CollStats for provided db and collection. Return collstats response.
    :param l_db: database name
    :param l_collection: collection name
    :return: collstats response
    """
    collection_stats = {'collStats': l_collection}
    l_collection_stats = mongo_client[l_db].command(collection_stats)
    # pprint(type(response))
    return l_collection_stats


def fetch_metrics_frm_collection_stats(l_db, l_collection, l_collection_stats, l_docs):
    '''
    This function iterates over metrics to be fetched. It prepares json/ mongodb document for each metric.
    Idea is to create one document per requested metric from collstats response.
    Overall metric doc structure is
    doc['db'] = database name
    doc['coll'] = collection name
    doc["metric_name"] can be
      1. collection_docs_count representing docs count for provided collection
      2. collection_size_bytes representing size of collection fetched from collstats in bytes
      3. index_size_bytes_+index_name representing size of individual index in bytes fetched from collstats
    Creating one document for each index.
    doc['metric_value'] value of relavant metric

    - Overall, creating one doc for each metric. Individual index size, collection size or collection doc count
    can be one metric.
    - Keeping field name helps while retrieving  documents. Especially metric_name can be indexed.
    - If same docs needs to be inserted in ELK , less number of fields can improve performance

    :param l_db: database name
    :param l_collection: collection name
    :param l_collection_stats: collstat response
    :param l_docs: mongodb_metric_documents to be saved for further reporting.
    :return:
    '''
    for metric in metrics_to_be_fetched:
        doc = {}
        doc['db'] = l_db
        doc['coll'] = l_collection

        # doc["lt_mdfy"] = datetime.utcnow()
        if metric in ['collection_docs_count', 'collection_size_bytes']:
            doc["metric_name"] = metric
            if metric == 'collection_docs_count':
                doc["metric_value"] = l_collection_stats['count']
            if metric == 'collection_size_bytes':
                doc["metric_value"] = l_collection_stats['size']
            l_docs.append(doc)
        if metric in ['index_size_bytes']:

            for index_name in l_collection_stats['indexSizes']:
                index_metric_doc = {}
                index_metric_doc = copy.deepcopy(doc)
                index_metric_doc["metric_name"] = metric + '_' + index_name
                index_metric_doc["metric_value"] = l_collection_stats['indexSizes'][index_name]
                l_docs.append(index_metric_doc)

    return l_docs


@auto_reattempt
def batch_insert_docs(docs):
    '''
    Idea is if number of metric documents increases more than batch size, save them in mongodb.
    insert_many is more efficient than insert_one
    :param docs: metric documents to be published
    :return: metric documents
    '''
    logger.debug('length of metric docs {}'.format(len(docs)))
    if len(docs) >= 100:
        result = mongo_client[mongodb_mgmt_database][mongodb_mgmt_collection].insert_many(docs, ordered=False)
        docs = []
        logger.info("Number of Documents Inserted {} ".format(len(result.inserted_ids)))
    return docs


def gather_collection_stats():
    '''
    # Overall Script does below at high level
        # 1. Iterate over each database.
        # 2. Iterate over each collection with in database.
        # 3. Run collstats command for each collection.
        # 4. Fetch requested information out of collection_stats response.
        # 5. Store them in mgmt database with daily collection.
    :return:
    '''
    docs = []

    for db in mongo_client.list_database_names():
        # logger.debug(db)
        if db not in mongodb_internal_databases:
            for collection in mongo_client[db].list_collection_names():
                # logger.debug(collection)
                if collection not in mongodb_internal_collections:
                    collection_stats = get_collection_stats(db, collection)
                    logger.debug('database:{} collection: {} count: {} size: {} indexSize: {}'.format(
                        db, collection, collection_stats['count'], collection_stats['size'],
                        collection_stats['indexSizes']
                    ))
                    docs = fetch_metrics_frm_collection_stats(db, collection, collection_stats, docs)
                    docs = batch_insert_docs(docs)

    ## Flush remaining docs
    result = mongo_client[mongodb_mgmt_database][mongodb_mgmt_collection].insert_many(docs, ordered=False)
    docs = []
    logger.info("Number of Documents Inserted {} ".format(len(result.inserted_ids)))


#
# main
#


logger.info(sys.argv[:])
parser = argparse.ArgumentParser(description="Mongodb Fetch Collection Counts")

required_names = parser.add_argument_group('required arguments')
optional_names = parser.add_argument_group('optional arguments')
required_names.add_argument("-m", "--mognodbUriToBeScannedForCollectionStats",
                            help='MongoDB connection uri e.g. mongodb://dbadmin:xyz@shpmgs'
                                 '-mongo-01.renaissance-golabs.com:27017' \
                                 '/?connectTimeoutMS=300000 for sharded cluster where RpClientId is needed to be tagged'
                            , required=True)
actions_parser = parser.add_argument_group('actions')

actions_parser.add_argument("--gather_collection_stats", dest='action',
                            action='store_const', const=gather_collection_stats,
                            help='Gather collection counts for all collections with in provided mongodb cluster ')
args = parser.parse_args()

# based on the argument passed, this will call the "const" function from the parser config
if args.action is None:
    parser.parse_args(['-h'])

logger.info("Connecting to Sharded Cluster  " + args.mognodbUriToBeScannedForCollectionStats)
try:
    mongo_client = MongoClient(args.mognodbUriToBeScannedForCollectionStats)
    now = datetime.now()
    mongodb_mgmt_database = 'mgmt'
    mongodb_mgmt_collection = 'collection_stats_' + str(now.year) + '_' + str(now.month) + '_' + str(now.day)
    args.action()
except Exception as err:
    logger.exception('Error Connecting to Sharded Cluster' + str(err))
    raise
finally:
    mongo_client.close()
