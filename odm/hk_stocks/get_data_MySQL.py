import pandas as pd
import numpy as np
import pymysql
import datetime

def get_datas_MySQL(collection_name):
    # 连接MySQL
    con = pymysql.connect(host='192.168.0.119', user='jydb', passwd='jydb', database='jydb')
    if collection_name == 'qt_hkdailyquote':
        now_time = datetime.datetime.now()
        # 一周前的日期
        past_time = now_time + datetime.timedelta(days=-5)   # refresh from 3 days ago
        past_time = past_time.strftime("%Y%m%d")
        sql = "SELECT * FROM jydb." + collection_name + ' where TradingDay >= ' + past_time
        data = pd.read_sql(sql, con)
        # 将日期格式转换
        def stamp2int(date):
            newdate = int(date.strftime('%Y%m%d'))
            return newdate
        data['TradingDay'] = data['TradingDay'].apply(stamp2int)

        # 增加一列主键
        keys = data['InnerCode'] * 1e8 + data['TradingDay']
        keys = keys.apply(int)
        keys = pd.DataFrame(keys, columns=['key'])
        data = data.join(keys)

        data = np.array(data)
        return data

    else:
        sql = "SELECT * FROM " +  collection_name
        if collection_name == 'hk_adjfactornew':
            data = pd.read_sql(sql, con)
            def stamp2int(date):
                newdate = int(date.strftime('%Y%m%d'))
                return newdate
            data['ExDiviDate'] = data['ExDiviDate'].apply(stamp2int)

            # 增加一列主键
            keys = data['InnerCode'] * 1e8 + data['ExDiviDate']
            keys = keys.apply(int)
            keys = pd.DataFrame(keys, columns=['key'])
            data = data.join(keys)

            data = np.array(data)
            return data

        else:
            data = pd.read_sql(sql, con)
            data = np.array(data)
            return data