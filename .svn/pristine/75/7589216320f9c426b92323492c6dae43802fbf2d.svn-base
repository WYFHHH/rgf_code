from pymodm.connection import connect
from pymongo.write_concern import WriteConcern
from pymodm import MongoModel, fields
from tqdm import tqdm
from odm.hk_stocks.get_info_Mongodb import *
from odm.hk_stocks.get_data_MySQL import *
import time

class hk_secumain(MongoModel):
    ID = fields.CharField(primary_key=True)
    InnerCode = fields.IntegerField()
    CompanyCode = fields.IntegerField()
    SecuCode = fields.CharField()
    ChiName = fields.CharField(blank=True)
    ChiNameAbbr = fields.CharField(blank=True)
    EngName = fields.CharField(blank=True)
    EngNameAbbr = fields.CharField(blank=True)
    SecuAbbr = fields.CharField(blank=True)
    ChiSpelling = fields.CharField(blank=True)
    SecuMarket = fields.IntegerField(blank=True)
    SecuCategory = fields.IntegerField(blank=True)
    ListedDate = fields.CharField(blank=True)      # fields.DateTimeField()
    ListedSector = fields.IntegerField(blank=True)
    ListedState = fields.IntegerField(blank=True)
    XGRQ = fields.DateTimeField(blank=True)
    JSID = fields.IntegerField(blank=True)
    DelistingDate = fields.CharField(blank=True)   #fields.DateTimeField()
    ISIN = fields.CharField(blank=True)
    FormerName = fields.CharField(blank=True)
    TradingUnit = fields.FloatField(blank=True)
    TraCurrUnit = fields.CharField(blank=True)   #fields.IntegerField()
    InsertTime = fields.CharField(blank=True)    #fields.DateTimeField()
    class Meta:
        write_concern = WriteConcern(j=True)
        connection_alias = 'initial_update'
        final = True

class hk_adjustedfactor(MongoModel):
    ID = fields.CharField(blank=True)
    InnerCode = fields.IntegerField(blank=True)
    ExDiviDate = fields.IntegerField(blank=True)
    UpdateTime = fields.DateTimeField(blank=True)
    AdjustFactor = fields.FloatField(blank=True)
    AdjustConst = fields.FloatField(blank=True)
    InformationMine = fields.CharField(blank=True)
    key = fields.IntegerField(primary_key=True)
    class Meta:
        write_concern = WriteConcern(j=True)
        connection_alias = 'initial_update_adjustedinfo'
        final = True

class hkdailyquote(MongoModel):
    ID = fields.CharField(blank=True)
    TradingDay = fields.IntegerField(blank=True)
    InnerCode = fields.IntegerField(blank=True)
    OpenPrice = fields.FloatField(blank=True)
    HighPrice = fields.FloatField(blank=True)
    LowPrice = fields.FloatField(blank=True)
    ClosePrice = fields.FloatField(blank=True)
    TurnoverVolume = fields.FloatField(blank=True)
    TurnoverValue = fields.FloatField(blank=True)
    key = fields.IntegerField(primary_key=True)
    class Meta:
        write_concern = WriteConcern(j=True)
        connection_alias = 'hkdailyquote'
        final = True


# 定义行情数据collection类


def insert2Mongodb(collection_name):
    if collection_name == 'hk_secumain':
        connect("mongodb://root:rgf12345@192.168.0.119:27017/jydb?authSource=admin", alias="initial_update")
        arr_hk_secumain = get_datas_MySQL('hk_secumain')
        for i in tqdm(range(len(arr_hk_secumain))):
            hk_secumain(arr_hk_secumain[i, 0], arr_hk_secumain[i, 1], arr_hk_secumain[i, 2], arr_hk_secumain[i, 3],
                    arr_hk_secumain[i, 4], arr_hk_secumain[i, 5], arr_hk_secumain[i, 6], arr_hk_secumain[i, 7],
                    arr_hk_secumain[i, 8], arr_hk_secumain[i, 9], arr_hk_secumain[i, 10], arr_hk_secumain[i, 11],
                    arr_hk_secumain[i, 12], arr_hk_secumain[i, 13], arr_hk_secumain[i, 14], arr_hk_secumain[i, 15],
                    arr_hk_secumain[i, 16], arr_hk_secumain[i, 17], arr_hk_secumain[i, 18], arr_hk_secumain[i, 19],
                    arr_hk_secumain[i, 20], arr_hk_secumain[i, 21], arr_hk_secumain[i, 22]).save()

    elif collection_name == 'hk_adjustedfactor':
        connect("mongodb://root:rgf12345@192.168.0.119:27017/jydb?authSource=admin", alias="initial_update_adjustedinfo")
        arr_hk_adjustedfactor = get_datas_MySQL('hk_adjfactornew')
        for i in tqdm(range(len(arr_hk_adjustedfactor))):
            hk_adjustedfactor(arr_hk_adjustedfactor[i, 0], arr_hk_adjustedfactor[i, 1], arr_hk_adjustedfactor[i, 2], arr_hk_adjustedfactor[i, 5],
                        arr_hk_adjustedfactor[i, 3], arr_hk_adjustedfactor[i, 4], arr_hk_adjustedfactor[i, 11], arr_hk_adjustedfactor[i, 14]).save()

    elif collection_name == 'qt_hkdailyquote':
        connect("mongodb://root:rgf12345@192.168.0.119:27017/jydb?authSource=admin", alias="hkdailyquote")
        arr_hkdailyquote = get_datas_MySQL('qt_hkdailyquote')
        if len(arr_hkdailyquote) == 0:
            print('非交易日：'+ time.strftime("%Y%m%d", time.localtime(time.time())))
            pass
        else:
            for i in tqdm(range(len(arr_hkdailyquote))):
                hkdailyquote(arr_hkdailyquote[i, 0], arr_hkdailyquote[i, 1], arr_hkdailyquote[i, 2], arr_hkdailyquote[i, 9],
                             arr_hkdailyquote[i, 3], arr_hkdailyquote[i, 4], arr_hkdailyquote[i, 5], arr_hkdailyquote[i, 7],
                             arr_hkdailyquote[i, 6], arr_hkdailyquote[i, 32]).save()