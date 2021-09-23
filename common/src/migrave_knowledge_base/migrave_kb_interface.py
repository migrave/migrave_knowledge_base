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
    for a application in migrave project.
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
        

    def store_performance_record(self, performance_record: Dict):
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

        print('Stored entry: \n {} \n with the id: \n {}'.format(performance_record, result.inserted_id))
        return result

    def get_performance_record(self, ) -> Dict:
        pass
        

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
        performance_record = DBUtils.get_all_docs(db_name=self.database_name,
                                                  collection_name=self.collection_name)
        return [self.convert_performance_record(record) for record in performance_record]

    def clear_performance_records(self):
        DBUtils.clear_db(db_name=self.database_name)

    def convert_performance_record(self, performance_record: Dict) -> Dict:
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

if __name__ == '__main__':
    performance = {}
    performance['time'] = {'secs': 1000, 'nsecs': 2000}
    performance['person'] = {'name': 'John Smith',
                            'age': 12,
                            'gender': 'male',
                            'mother_tongue': 'russian'}
    performance['game_activity'] = {'game_id': '1',
                                    'game_activity_id': '2',
                                    'difficulty_level': '4'}
    performance['answer_correctness'] = '1'


    migrave_db = MigraveKBInterface()
    migrave_db.clear_performance_records()
    result = migrave_db.store_performance_record(performance)

    performance['person']['name'] = 'Lilia Carpenter'
    performance['person']['age'] = '13'
    performance['person']['gender'] = 'female'
    performance['person']['mother_tongue'] = 'german'
    performance['game_activity']['game_id'] = '2'

    result = migrave_db.store_performance_record(performance)
    
    performance['person']['name'] = 'Daniel Capman'
    performance['person']['age'] = '12'
    performance['person']['gender'] = 'male'
    performance['person']['mother_tongue'] = 'spain'
    performance['game_activity']['game_id'] = '3'

    result = migrave_db.store_performance_record(performance)
    
    print(migrave_db.get_newest_performance_record())
    print(migrave_db.get_oldest_performance_record())
    print(migrave_db.get_all_perfomance_records())
