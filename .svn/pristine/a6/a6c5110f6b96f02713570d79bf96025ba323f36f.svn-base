# 写程序时Mongodb数据库最新日期更新到2019/4/1
# 从MySQL中获得hk_secumain、hk_adjustedfactor的最新数据，如有不同，删除原来的，并将新的版本插入到Mongodb里，
# 从MySQL中获得hkdailyquote当日的数据

from odm.hk_stocks.get_info_Mongodb import *
from odm.hk_stocks.delete_colllection import *
from odm.hk_stocks.insert_data_Mongodb import *


len_hk_secumain = len(get_field('SecuCode','hk_secumain'))
len_hk_adjustedfactor = len(get_field('InnerCode','hk_adjustedfactor'))

# 从MySQL调用某一列的数据，获得其长度，
mysql_len_hk_secumain = len(get_datas_MySQL('hk_secumain'))
mysql_len_hk_adjustedfactor = len(get_datas_MySQL('hk_adjfactornew'))

def refresh_not_daily():
    if len_hk_secumain != mysql_len_hk_secumain:
        delete('hk_secumain') #删除Mongodb中原来的collection
        # 填入最新的数据
        insert2Mongodb('hk_secumain')
        print('Sucessfully refresh hk_secumain')
    if len_hk_adjustedfactor != mysql_len_hk_adjustedfactor:
        delete('hk_adjustedfactor') #删除Mongodb中原来的collection
        # 填入最新的数据
        insert2Mongodb('hk_adjustedfactor')
        print('Sucessfully refresh hk_adjustedfactor')

def refresh_daily():
    insert2Mongodb('qt_hkdailyquote')
    print('Sucessfully refresh hkdailyquote')