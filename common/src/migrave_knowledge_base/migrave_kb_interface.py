'''Module defining interfaces for knowledge base interactions
specific to domestic applications.
'''
import numpy as np
import pandas as pd
import time 

from migrave_knowledge_base.db_utils import DBUtils
from typing import Dict, Sequence

class MigraveKBInterface(object):
    '''Defines an interface for performing knowledge base operations common
    for an application in migrave project.
    '''
    def __init__(self):
        mongo_db_client = DBUtils.get_db_client()

        self.database_name = 'migrave_game_performance_database'
        self.collection_name = 'performance_records'
        

        if not self.database_name in mongo_db_client.list_database_names():
            database = mongo_db_client[self.database_name]
            print('Migrave database is not created. The database will be created ...')
        else:
            database = mongo_db_client[self.collection_name]
            print('Successfully accessed Migrave database')
        

    def store_performance_record(self, performance_record: Dict) -> bool:
        '''Stores a performance record in the database
        Keyword arguments:
        @param performance_record -- dictionary with performance record data
        '''

        performance_record_df = pd.json_normalize(performance_record, sep='_')
        performance_record = performance_record_df.to_dict(orient='records')[0]
        performance_record['timestamp'] = time.time()

        mongo_db_client = DBUtils.get_db_client()
        database = mongo_db_client[self.database_name]
        collection = database[self.collection_name]
        result = collection.insert_one(performance_record)

        if result.acknowledged:
            print('Stored entry: \n {} \n with the id: \n {}'.format(performance_record, result.inserted_id))
        else:
            print('Could not store the entry: \n {}'.format(performance_record))
        return result.acknowledged

    def get_performance_records(self, person_name: str = '', game_id: str = '') -> Sequence[Dict]:
        '''Returns all the performance records for a given person name and game id. 
        If 'collection_name' or 'game_id' is not specified, they are ignored while finding 
        the performance records. If neither 'collection_name' nor 'game_id' is
        specified, all the possible documents are returned.
        '''
        performance_records = DBUtils.get_specific_docs(db_name=self.database_name, 
                                        collection_name=self.collection_name, 
                                        person_name=person_name, 
                                        game_id=game_id)
        return [self.convert_performance_record(record) for record in performance_records]

    def get_newest_performance_record(self) -> Dict:
        '''Returns the latest performance record from the database.
        '''
        performance_record = DBUtils.get_newest_doc(db_name=self.database_name, 
                                                    collection_name=self.collection_name)
        return self.convert_performance_record(performance_record)

    def get_oldest_performance_record(self) -> Dict:
        '''Returns the oldest performance record from the database.
        '''
        performance_record = DBUtils.get_oldest_doc(db_name=self.database_name, 
                                                    collection_name=self.collection_name)
        return self.convert_performance_record(performance_record)

    def get_all_perfomance_records(self) -> Sequence[Dict]:
        '''Returns all performance records from the database.
        '''
        performance_record = DBUtils.get_all_docs(db_name=self.database_name,
                                                  collection_name=self.collection_name)
        return [self.convert_performance_record(record) for record in performance_record]

    def clear_performance_records(self):
        '''Clears the database from the performance records.
        '''
        DBUtils.clear_db(db_name=self.database_name)

    def convert_performance_record(self, performance_record: Dict) -> Dict:
        '''Converts flatened dictionary into the nested dictionary.
        '''
        record = {}
        record['time'] = {'secs': performance_record['time_secs'], 
                          'nsecs': performance_record['time_nsecs']}
        record['person'] = {'name': performance_record['person_name'],
                            'age': performance_record['person_age'],
                            'gender': performance_record['person_gender'],
                            'mother_tongue': performance_record['person_mother_tongue']}
        record['game_activity'] = {'game_id': performance_record['game_activity_game_id'],
                                   'game_activity_id': performance_record['game_activity_game_activity_id'],
                                   'difficulty_level': performance_record['game_activity_difficulty_level']}
        record['answer_correctness'] = performance_record['answer_correctness']
        return record

# if __name__ == '__main__':
#     performance = {}
#     performance['time'] = {'secs': 1000, 'nsecs': 2000}
#     performance['person'] = {'name': 'John Smith',
#                             'age': 12,
#                             'gender': 'male',
#                             'mother_tongue': 'russian'}
#     performance['game_activity'] = {'game_id': 1,
#                                     'game_activity_id': 2,
#                                     'difficulty_level': 4}
#     performance['answer_correctness'] = 1


#     migrave_db = MigraveKBInterface()
#     migrave_db.clear_performance_records()

#     result = migrave_db.store_performance_record(performance)

#     performance['game_activity']['game_id'] = 3
    
#     result = migrave_db.store_performance_record(performance)

#     performance['person']['name'] = 'Lilia Carpenter'
#     performance['person']['age'] = 13
#     performance['person']['gender'] = 'female'
#     performance['person']['mother_tongue'] = 'german'
#     performance['game_activity']['game_id'] = 2

#     result = migrave_db.store_performance_record(performance)

#     performance['game_activity']['game_id'] = 1
    
#     result = migrave_db.store_performance_record(performance)
    
#     performance['person']['name'] = 'Daniel Capman'
#     performance['person']['age'] = 12
#     performance['person']['gender'] = 'male'
#     performance['person']['mother_tongue'] = 'spain'
#     performance['game_activity']['game_id'] = 3

#     result = migrave_db.store_performance_record(performance)

#     performance['game_activity']['game_id'] = 2
    
#     result = migrave_db.store_performance_record(performance)
    
#     print('Get newest record: \n', migrave_db.get_newest_performance_record(), '\n')
#     print('Get oldest record: \n', migrave_db.get_oldest_performance_record(), '\n')
#     print('Get all performance records: \n', migrave_db.get_all_perfomance_records(), '\n')
#     print('Get person performance records: \n', migrave_db.get_performance_records(person_name='Daniel Capman'), '\n')
#     print('Get game performance recods: \n', migrave_db.get_performance_records(game_id=1))
#     print('Get person and game performance record: \n' ,migrave_db.get_performance_records(person_name='Lilia Carpenter', game_id=2))
#     print('Get wrong person: \n', migrave_db.get_performance_records(person_name='Lilia Ca', game_id=2))
#     print('Get wrong game: \n', migrave_db.get_performance_records(person_name='Lilia Carpenter', game_id=10))
#     print('Get everything: \n', migrave_db.get_performance_records())