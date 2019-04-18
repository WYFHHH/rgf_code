# 从Mongodb里面读取已有collection的长度

from pymodm.queryset import QuerySet
from pymodm.connection import connect
from pymongo.write_concern import WriteConcern
from pymodm import MongoModel, fields
from pymodm.manager import Manager

# 创建查询
class QuerySet_secumain(QuerySet):
    def get_SecuCode(self, SecuCode):
        return list(self.only(SecuCode))

Manager_hk_secumain = Manager.from_queryset(QuerySet_secumain)

class hk_secumain(MongoModel):
    ID = fields.CharField(primary_key=True)
    InnerCode = fields.IntegerField()
    CompanyCode = fields.IntegerField()
    SecuCode = fields.CharField()
    ChiName = fields.CharField(blank=True)
    ChiNameAbbr = fields.CharField(blank=True)
    EngName = fields.CharField()
    EngNameAbbr = fields.CharField()
    SecuAbbr = fields.CharField()
    ChiSpelling = fields.CharField()
    SecuMarket = fields.IntegerField()
    SecuCategory = fields.IntegerField()
    ListedDate = fields.CharField()  # fields.DateTimeField()
    ListedSector = fields.IntegerField()
    ListedState = fields.IntegerField()
    XGRQ = fields.DateTimeField()
    JSID = fields.IntegerField()
    DelistingDate = fields.CharField()  # fields.DateTimeField()
    ISIN = fields.CharField()
    FormerName = fields.CharField()
    TradingUnit = fields.FloatField()
    TraCurrUnit = fields.CharField()  # fields.IntegerField()
    InsertTime = fields.CharField()  # fields.DateTimeField()
    objects = Manager_hk_secumain()

    class Meta:
        write_concern = WriteConcern(j=True)
        connection_alias = 'initial_update'
        final = True

class QuerySet_adjustedfactor(QuerySet):
    def get_SecuCode(self, SecuCode):
        return list(self.only(SecuCode))

Manager_hk_adjustedfactor = Manager.from_queryset(QuerySet_adjustedfactor)


class hk_adjustedfactor(MongoModel):
    ID = fields.CharField(primary_key=True)
    InnerCode = fields.IntegerField()
    ExDiviDate = fields.DateTimeField()
    UpdateTime = fields.DateTimeField()
    AdjustFactor = fields.FloatField()
    AdjustConst = fields.FloatField()
    InformationMine = fields.CharField()
    objects = Manager_hk_adjustedfactor()
    class Meta:
        write_concern = WriteConcern(j=True)
        connection_alias = 'initial_update_adjustedinfo'
        final = True

def get_field(SecuCode, collection_name):
    if collection_name == 'hk_secumain':
        print('Connect the Mongodb...')
        connect("mongodb://root:rgf12345@192.168.0.119:27017/jydb?authSource=admin", alias="initial_update")

        SecuCodes_class = hk_secumain.objects.get_SecuCode(SecuCode)
        SecuCodes = []
        for secu in SecuCodes_class:
            SecuCodes.append(eval('secu.'+ SecuCode))
        print('Download Completed.')
        return SecuCodes

    elif collection_name == 'hk_adjustedfactor':
        print('Connect the Mongodb...')
        connect("mongodb://root:rgf12345@192.168.0.119:27017/jydb?authSource=admin", alias="initial_update_adjustedinfo")
        SecuCodes_class = hk_adjustedfactor.objects.get_SecuCode(SecuCode)
        SecuCodes = []
        for secu in SecuCodes_class:
            SecuCodes.append(eval('secu.' + SecuCode))
        print('Download Completed.')
        return SecuCodes
    # return pd.DataFrame(SecuCodes)
