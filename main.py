import json
from elasticsearch import Elasticsearch
import tqdm
from elasticsearch.helpers import streaming_bulk
import time
import pandas as pd


def extract(file):

    '''Split to:
       sample_db - used for generating dynamic mapping
       db - used for bulk loading'''

    f = open(file)
    db = json.loads(f.read())
    sample_db = dict(list(db.items())[0:2])

    return db, sample_db

def schema_validator(db, orient_choice):

    '''Refer to documentation, specially for information about string format - orient.
       This function should not run with big files or if you already have correct orientation.
     '''
    # https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.to_json.html
    
    dataframe_db = pd.read_json(db, orient=orient_choice)
    return dataframe_db

def index(db={}, index_name=''):

    '''If you want to supply your own mapping in appropriate format to elasticsearch you can skip calling index() function.
       This function generates mapping

    '''

    start  = time.time()
    for id, body in db.items():
        es.index(index=index_name, id=id, doc_type='doc', body=body)
    print("--- %s seconds ---" % (time.time() - start))

    mapping = es.indices.get_mapping(index=index_name)
    mapping = mapping[index_name]

    es.indices.close(index=index_name)
    es.indices.delete(index=index_name)
    print("Index deleted. Mapping saved")

    return mapping

def generate_actions(db):

    '''Yield each document to streaming_bulk loop'''

    for id, body in db.items():
            actions = dict(body)
            yield actions


def bulk_index(db={}, index_name= "", mapping={}):

    '''Create index. Bulk Load.'''

    es.indices.create(index=index_name, body=mapping)
    number_of_docs = len(db)
    progress = tqdm.tqdm(unit=" documents", total=number_of_docs)
    successes = 0
    for ok, action in streaming_bulk(client=es, index=index_name, actions=generate_actions(db),):
        progress.update(1)
        successes += ok

    print("Indexed %d/%d documents" % (successes, number_of_docs))

if __name__ == '__main__':

    '''Create Elasticsearch connection. Remember to start elasticsearch first!'''

    es = Elasticsearch([{'host': 'localhost', 'port': 9200}])

    '''Specify name of a file to load.
       Specify index name for elasticsearch
       Specify orientation of data for loading into pandas. You may want to change orientation depending on your
       key - item scheme.
    '''

    file = 'test_file.json'
    index_name = 'bulk_index'
    orient_choice = 'index'

    '''Extract JSON file from path. Return Sample for dynamic mapping in ES'''
    db, db_sample = extract(file)

    '''Helper function, if you need to read-in data into ES based on different string format.'''
    dataframe = schema_validator(db_sample, orient_choice=orient_choice)

    mapping = index(db, index_name)
    bulk_index(db, index_name, mapping)

    print('Done. Delete %%%', index_name, 'index %%% if you want to rerun script with same parameters.')
