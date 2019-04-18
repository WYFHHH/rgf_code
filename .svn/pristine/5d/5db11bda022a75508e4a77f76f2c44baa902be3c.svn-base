
# 批量insert（针对不同的股票代码）

from pymongo import MongoClient

def delete(collecition_name):
    url = 'mongodb://root:rgf12345@192.168.0.119:27017'
    db = 'jydb'
    client = MongoClient(url)
    database = client[db]  # select a database
    # mycol = database["hk_adjfactornew"]
    mycol = database[collecition_name]
    mycol.drop()

