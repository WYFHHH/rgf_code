import numpy as np
import os
import pymysql
from pymodm.connection import connect
from pymongo.write_concern import WriteConcern
from pymodm import MongoModel, fields
import pandas as pd
from tqdm import tqdm
from odm_new.filldata_new import *
from odm_new.filldata_origin import *
from odm_new.secu2inner_new import *

# HSI export
# 连接Mongodb
connect("mongodb://root:rgf12345@192.168.0.119:27017/jydb?authSource=admin", alias="hsi_import")

def hsi_import():
    df = pd.read_csv("E:/20190324/hsi.csv").drop(columns='COUNT')

    df['Date'] = 10000 * df['Date'].str[:4].apply(int) + \
                                         100 * df['Date'].str[5:7].apply(int) + \
                                         df['Date'].str[8:10].apply(int)

    arr_hsi = df.values
    class hsi(MongoModel):
        Date = fields.IntegerField(primary_key=True)
        HIGH = fields.FloatField()
        CLOSE = fields.FloatField()
        LOW = fields.FloatField()
        OPEN = fields.FloatField()
        VOLUME = fields.IntegerField()
        class Meta:
            write_concern = WriteConcern(j=True)
            connection_alias = 'hsi_import'
            final = True
    for i in tqdm(range(len(arr_hsi))):
        hsi(arr_hsi[i, 0], arr_hsi[i, 1], arr_hsi[i, 2], arr_hsi[i, 3],arr_hsi[i, 4], arr_hsi[i, 5]).save()

# hsi_import()    #没有更新功能


# 从MySQL读取交易日数据
con = pymysql.connect(host='192.168.0.119', user='jydb', passwd='jydb', database='jydb')
sql_select = 'SELECT TradingDate FROM jydb.qt_tradingdaynew where \
TradingDate >= 20100101 and SecuMarket = 72 and IfTradingDay = 1 and TradingDate <= 20191231;'
tradingdays = pd.read_sql(sql_select, con)   #Time_stamp类型
con.close()

tradingdays_list = []
for i in range(len(tradingdays)):
    tradingdays_list.append(int(str(tradingdays['TradingDate'][i])[0:4]+
                            str(tradingdays['TradingDate'][i])[5:7]+
                            str(tradingdays['TradingDate'][i])[8:11]))   #所有交易日的list(取前10个字符)

######################################
filePath = "E:/20190324/semi"
file_list = os.listdir(filePath)

# 定义一个针对semi的class

class hk_derivative(MongoModel):

    Income_Statement_Source_Date = fields.IntegerField()
    Instrument = fields.CharField()
    Financial_Period_Absolute = fields.CharField()
    Period_End_Date = fields.CharField()
    Net_Income_Before_Extraordinary_Items = fields.FloatField()
    Working_Capital = fields.FloatField()
    EBIT_TTM = fields.FloatField()
    ROE_Common_Equity_TTM = fields.FloatField()
    Total_Fixed_Assets_Net = fields.FloatField()
    Total_Fixed_Assets_Net_1 = fields.FloatField()
    key = fields.IntegerField(primary_key=True)
    InnerCode = fields.IntegerField()
    class Meta:
        write_concern = WriteConcern(j=True)
        connection_alias = 'hk_derivative'
        final = True

# 连接Mongodb
connect("mongodb://root:rgf12345@192.168.0.119:27017/jydb?authSource=admin", alias="hk_derivative")

# 退市股票代码
lost_stock = []

for file in tqdm(file_list):
#     file = file_list[0]  # 先做第一个文件
    filled_data = filedata(filePath, file,tradingdays_list)
    # 对于退市的股票，filled_data长度为1，输出到指定文件夹
    if len(filled_data)==1:    # 查看2663.HK.csv有没有在lost_list里面，不在则是正确
        lost_stock.append(filled_data[0])
        continue
    else:
    # 对于新得到的filled_data判断有没有缺失列，如果有就补齐
        # 由齐全数据得到的columns
        columns = ['Income Statement Source Date', 'Instrument',
           'Financial Period Absolute', 'Period End Date',
           'Net Income Before Extraordinary Items', 'Working Capital', 'EBIT, TTM',
           'ROE Common Equity %, TTM', 'Total Fixed Assets, Net',
           'Total Fixed Assets, Net.1']

        for col in columns:
            if col in filled_data.columns:
                continue
            else:
                filled_data[col] = np.nan
        filled_data = filled_data[columns]

        # 给补充好的表加一列主键
        innercode = SecuCode2InnerCode('0' + file[0:4])
        part1 = int(innercode)*1e8
        keys = []
        for date in filled_data['Income Statement Source Date']:
            # key = int(int(date[0:4])*1e4) + int(int(date[5:7])*1e2) + int(int(date[8:10])) + SecuCode
            key = date + part1
            keys.append(key)
        keys = pd.DataFrame(keys, columns = ['key'])
        filled_data = filled_data.join(keys)

        # 对instrument进行统一，并加上一列innercode
        filled_data['Instrument'] = '0' + file[0:4]
        filled_data['InnerCode'] = innercode

        arr_filled_data = filled_data.values   # 转换为array




        # 导入Mongodb
        for i in range(len(arr_filled_data)):
            hk_derivative(arr_filled_data[i, 0], arr_filled_data[i, 1], arr_filled_data[i, 2], arr_filled_data[i, 3], arr_filled_data[i, 4],
                        arr_filled_data[i, 5], arr_filled_data[i, 6], arr_filled_data[i, 7],arr_filled_data[i, 8], arr_filled_data[i, 9],
                          arr_filled_data[i, 10],arr_filled_data[i, 11]).save()



########################
# initial
filePath = "E:/20190324/is_bs_cf_fy"
file_list = os.listdir(filePath)

# 定义一个针对initial数据的class
class hk_initial(MongoModel):
    Income_Statement_Source_Date = fields.IntegerField()
    Instrument = fields.CharField()
    Financial_Period_Absolute = fields.CharField()
    Period_End_Date = fields.CharField()
    Building___Gross = fields.FloatField()
    Land_Improvements___Gross = fields.FloatField()
    Machinery_Equipment___Gross = fields.FloatField()
    Other_Property_Plant_Equipment___Gross = fields.FloatField()
    Property_Plant_And_Equipment_Total___Gross = fields.FloatField()
    Accumulated_Depreciation_Total = fields.FloatField()
    Property_Plant_Equipment_Total___Net = fields.FloatField()
    Goodwill_Net = fields.FloatField()
    Intangibles_Net = fields.FloatField()
    LT_Investment___Affiliate_Companies = fields.FloatField()
    Long_Term_Investments = fields.FloatField()
    Deferred_Income_Tax___LT_Asset = fields.FloatField()
    Other_Long_Term_Assets_Total = fields.FloatField()
    Total_Assets_Reported = fields.FloatField()
    Accounts_Payable = fields.FloatField()
    Payable___Accrued = fields.FloatField()
    Current_Port__of_LT_Debt_Capital_Leases = fields.FloatField()
    Income_Taxes_Payable = fields.FloatField()
    Other_Current_liabilities_Total = fields.FloatField()
    Long_Term_Debt = fields.FloatField()
    Total_Long_Term_Debt = fields.FloatField()
    Total_Debt = fields.FloatField()
    Deferred_Income_Tax___LT_Liability = fields.FloatField()
    Deferred_Income_Tax___LT_Liability_1 = fields.FloatField()
    Minority_Interest = fields.FloatField()
    Pension_Benefits___Underfunded = fields.FloatField()
    Other_Liabilities_Total = fields.FloatField()
    Total_Liabilities = fields.FloatField()
    Common_Stock = fields.FloatField()
    Common_Stock_Total = fields.FloatField()
    Additional_Paid_In_Capital = fields.FloatField()
    Retained_Earnings_Accumulated_Deficit = fields.FloatField()
    Unrealized_Gain_Loss = fields.FloatField()
    Translation_Adjustment = fields.FloatField()
    Other_Equity = fields.FloatField()
    Other_Comprehensive_Income = fields.FloatField()
    Other_Equity_Total = fields.FloatField()
    Total_Equity = fields.FloatField()
    Total_Liabilities_And_Shareholders = fields.FloatField()
    Total_Common_Shares_Outstanding = fields.FloatField()
    Cash = fields.FloatField()
    Cash_and_Equivalents = fields.FloatField()
    Short_Term_Investments = fields.FloatField()
    Cash_and_Short_Term_Investments = fields.FloatField()
    Accounts_Receivable___Trade_Gross = fields.FloatField()
    Provision_for_Doubtful_Accounts = fields.FloatField()
    Accounts_Receivable___Trade_Net = fields.FloatField()
    Notes_Receivable___Short_Term = fields.FloatField()
    Receivables___Other = fields.FloatField()
    Total_Receivables_Net = fields.FloatField()
    Inventories___Finished_Goods = fields.FloatField()
    Inventories___Work_In_Progress = fields.FloatField()
    Total_Inventory = fields.FloatField()
    Prepaid_Expenses = fields.FloatField()
    Discontinued_Operations___Current_Asset = fields.FloatField()
    Other_Current_Assets = fields.FloatField()
    Other_Current_Assets_Total = fields.FloatField()
    Total_Current_Assets = fields.FloatField()
    LT_Investments___Other = fields.FloatField()
    Note_Receivable___Long_Term = fields.FloatField()
    Other_Long_Term_Assets = fields.FloatField()
    Notes_Payable_Short_Term_Debt = fields.FloatField()
    Customer_Advances = fields.FloatField()
    Security_Deposits = fields.FloatField()
    Other_Payables = fields.FloatField()
    Discontinued_Operations___Curr_Liability = fields.FloatField()
    Other_Current_Liabilities = fields.FloatField()
    Total_Current_Liabilities = fields.FloatField()
    Other_Long_Term_Liabilities = fields.FloatField()
    Net_Income_Starting_Line_Cumulative = fields.FloatField()
    Depreciation_CF_Cumulative = fields.FloatField()
    Depreciation___Depletion_Cumulative = fields.FloatField()
    Deferred_Taxes_CF = fields.FloatField()
    Unusual_Items_CF = fields.FloatField()
    Equity_in_Net_Earnings_Loss_CF = fields.FloatField()
    Other_Non_Cash_Items = fields.FloatField()
    Non_Cash_Items = fields.FloatField()
    Other_Assets_Liabilities_Net_CF = fields.FloatField()
    Other_Operating_Cash_Flow = fields.FloatField()
    Changes_in_Working_Capital_Cumulative = fields.FloatField()
    Cash_from_Operating_Activities_Cumulative = fields.FloatField()
    Purchase_of_Fixed_Assets_Cumulative = fields.FloatField()
    Purchase_Acquisition_of_Intangibles = fields.FloatField()
    Capital_Expenditures_Cumulative = fields.FloatField()
    Acquisition_of_Business = fields.FloatField()
    Sale_of_Business = fields.FloatField()
    Sale_of_Fixed_Assets = fields.FloatField()
    Sale_Maturity_of_Investment = fields.FloatField()
    Investment_Net_CF = fields.FloatField()
    Purchase_of_Investments = fields.FloatField()
    Other_Investing_Cash_Flow = fields.FloatField()
    Other_Investing_Cash_Flow_Items_Total_Cumulative = fields.FloatField()
    Cash_from_Investing_Activities_Cumulative = fields.FloatField()
    Other_Financing_Cash_Flow = fields.FloatField()
    Financing_Cash_Flow_Items = fields.FloatField()
    Cash_Dividends_Paid___Common = fields.FloatField()
    Total_Cash_Dividends_Paid_Cumulative = fields.FloatField()
    Repurchase_Retirement_of_Common = fields.FloatField()
    Common_Stock_Issued_Retired_Net = fields.FloatField()
    Issuance_Retirement_of_Stock_Net_Cumulative = fields.FloatField()
    Long_Term_Debt_Issued_Reduced_Net = fields.FloatField()
    Total_Debt_Issued = fields.FloatField()
    Total_Debt_Reduction = fields.FloatField()
    Issuance_Retirement_of_Debt_Net_Cumulative = fields.FloatField()
    Cash_from_Financing_Activities_Cumulative = fields.FloatField()
    Net_Change_in_Cash_Cumulative = fields.FloatField()
    Net_Cash___Beginning_Balance = fields.FloatField()
    Net_Cash___Ending_Balance = fields.FloatField()
    Cash_Interest_Paid_Supplemental_Cumulative = fields.FloatField()
    Cash_Taxes_Paid = fields.FloatField()
    Labor_And_Related_Expense = fields.FloatField()
    Net_Income_Before_Taxes = fields.FloatField()
    Provision_for_Income_Taxes = fields.FloatField()
    Net_Income_After_Taxes = fields.FloatField()
    Minority_Interest_1 = fields.FloatField()
    Net_Income_Before_Extraordinary_Items = fields.FloatField()
    Discontinued_Operations = fields.FloatField()
    Total_Extraordinary_Items = fields.FloatField()
    Net_Income_Incl_Extra_Before_Distributions = fields.FloatField()
    Income_Avail_to_Cmn_Shareholders_Excl_Extra = fields.FloatField()
    Income_Avail_to_Cmn_Shareholders_Incl_Extra = fields.FloatField()
    Basic_Weighted_Average_Shares = fields.FloatField()
    Basic_EPS_Excluding_Extraordinary_Items = fields.FloatField()
    Basic_EPS_Including_Extraordinary_Items = fields.FloatField()
    Diluted_Net_Income = fields.FloatField()
    Diluted_Weighted_Average_Shares = fields.FloatField()
    Diluted_EPS_Excluding_Extraordinary_Items = fields.FloatField()
    Diluted_EPS_Including_Extraordinary_Items = fields.FloatField()
    DPS___Common_Stock_Primary_Issue = fields.FloatField()
    Net_Sales = fields.FloatField()
    Revenue = fields.FloatField()
    Total_Revenue = fields.FloatField()
    Cost_of_Revenue = fields.FloatField()
    Cost_of_Revenue_Total = fields.FloatField()
    Gross_Profit = fields.FloatField()
    Selling_General_Administrative_Expense_Total = fields.FloatField()
    Depreciation_Operating = fields.FloatField()
    Depreciation_And_Amortization = fields.FloatField()
    Investment_Income___Operating = fields.FloatField()
    Interest_Investment_Income___Operating = fields.FloatField()
    Interest_Expense_Income_Net_Operating_Ttl = fields.FloatField()
    Loss_Gain_on_Sale_of_Assets___Operating = fields.FloatField()
    Other_Unusual_Expense_Income = fields.FloatField()
    Unusual_Expense_Income = fields.FloatField()
    Other_Operating_Expense = fields.FloatField()
    Other_Net___Operating = fields.FloatField()
    Other_Operating_Expenses_Total = fields.FloatField()
    Total_Operating_Expense = fields.FloatField()
    Operating_Income = fields.FloatField()
    Interest_Expense___Non_Operating = fields.FloatField()
    Interest_Capitalized___Non_Operating = fields.FloatField()
    Interest_Expense_Net_Non_Operating = fields.FloatField()
    Investment_Income___Non_Operating = fields.FloatField()
    Interest_Invest_Income___Non_Operating = fields.FloatField()
    Interest_Income_Expense_Net_Non_Operating_Total = fields.FloatField()
    Other_Non_Operating_Income_Expense = fields.FloatField()
    Other_Net___Non_Operating = fields.FloatField()
    Total_Extraordinary_Items_1 = fields.FloatField()
    key = fields.IntegerField(primary_key=True)
    InnerCode = fields.IntegerField()
    class Meta:
        write_concern = WriteConcern(j=True)
        connection_alias = 'hk_initial'
        final = True

# 连接Mongodb
connect("mongodb://root:rgf12345@192.168.0.119:27017/jydb?authSource=admin", alias="hk_initial")

# 退市股票代码
lost_stock_initial = []

for file in tqdm(file_list):
    #     file = file_list[0]  # 先做第一个文件
    filled_data = filedata(filePath, file,tradingdays_list)
    # 对于退市的股票，filled_data长度为1，输出到指定文件夹
    if len(filled_data) == 1:
        lost_stock_initial.append(filled_data[0])
        continue
    else:
        # 对于新得到的filled_data判断有没有缺失列，如果有就补齐
        # 由齐全数据得到的columns
        columns = ['Income Statement Source Date',
             'Instrument',
             'Financial Period Absolute',
             'Period End Date',
             'Building - Gross',
             'Land/Improvements - Gross',
             'Machinery/Equipment - Gross',
             'Other Property/Plant/Equipment - Gross',
             'Property, Plant And Equipment, Total - Gross',
             'Accumulated Depreciation, Total',
             'Property/Plant/Equipment, Total - Net',
             'Goodwill, Net',
             'Intangibles, Net',
             'LT Investment - Affiliate Companies',
             'Long Term Investments',
             'Deferred Income Tax - LT Asset',
             'Other Long Term Assets, Total',
             'Total Assets, Reported',
             'Accounts Payable',
             'Payable / Accrued',
             'Current Port. of LT Debt/Capital Leases',
             'Income Taxes Payable',
             'Other Current liabilities, Total',
             'Long Term Debt',
             'Total Long Term Debt',
             'Total Debt',
             'Deferred Income Tax - LT Liability',
             'Deferred Income Tax - LT Liability.1',
             'Minority Interest',
             'Pension Benefits - Underfunded',
             'Other Liabilities, Total',
             'Total Liabilities',
             'Common Stock',
             'Common Stock, Total',
             'Additional Paid-In Capital',
             'Retained Earnings (Accumulated Deficit)',
             'Unrealized Gain (Loss)',
             'Translation Adjustment',
             'Other Equity',
             'Other Comprehensive Income',
             'Other Equity, Total',
             'Total Equity',
             "Total Liabilities And Shareholders' Equity",
             'Total Common Shares Outstanding',
             'Cash',
             'Cash and Equivalents',
             'Short Term Investments',
             'Cash and Short Term Investments',
             'Accounts Receivable - Trade, Gross',
             'Provision for Doubtful Accounts',
             'Accounts Receivable - Trade, Net',
             'Notes Receivable - Short Term',
             'Receivables - Other',
             'Total Receivables, Net',
             'Inventories - Finished Goods',
             'Inventories - Work In Progress',
             'Total Inventory',
             'Prepaid Expenses',
             'Discontinued Operations - Current Asset',
             'Other Current Assets',
             'Other Current Assets, Total',
             'Total Current Assets',
             'LT Investments - Other',
             'Note Receivable - Long Term',
             'Other Long Term Assets',
             'Notes Payable/Short Term Debt',
             'Customer Advances',
             'Security Deposits',
             'Other Payables',
             'Discontinued Operations - Curr Liability',
             'Other Current Liabilities',
             'Total Current Liabilities',
             'Other Long Term Liabilities',
             'Net Income/Starting Line, Cumulative',
             'Depreciation (CF), Cumulative',
             'Depreciation / Depletion, Cumulative',
             'Deferred Taxes (CF)',
             'Unusual Items (CF)',
             'Equity in Net Earnings (Loss) (CF)',
             'Other Non-Cash Items',
             'Non-Cash Items',
             'Other Assets & Liabilities, Net (CF)',
             'Other Operating Cash Flow',
             'Changes in Working Capital, Cumulative',
             'Cash from Operating Activities, Cumulative',
             'Purchase of Fixed Assets, Cumulative',
             'Purchase/Acquisition of Intangibles',
             'Capital Expenditures, Cumulative',
             'Acquisition of Business',
             'Sale of Business',
             'Sale of Fixed Assets',
             'Sale/Maturity of Investment',
             'Investment, Net (CF)',
             'Purchase of Investments',
             'Other Investing Cash Flow',
             'Other Investing Cash Flow Items, Total, Cumulative',
             'Cash from Investing Activities, Cumulative',
             'Other Financing Cash Flow',
             'Financing Cash Flow Items',
             'Cash Dividends Paid - Common',
             'Total Cash Dividends Paid, Cumulative',
             'Repurchase/Retirement of Common',
             'Common Stock Issued (Retired), Net',
             'Issuance (Retirement) of Stock, Net, Cumulative',
             'Long Term Debt Issued (Reduced), Net',
             'Total Debt Issued',
             'Total Debt Reduction',
             'Issuance (Retirement) of Debt, Net, Cumulative',
             'Cash from Financing Activities, Cumulative',
             'Net Change in Cash, Cumulative',
             'Net Cash - Beginning Balance',
             'Net Cash - Ending Balance',
             'Cash Interest Paid, Supplemental, Cumulative',
             'Cash Taxes Paid',
             'Labor And Related Expense',
             'Net Income Before Taxes',
             'Provision for Income Taxes',
             'Net Income After Taxes',
             'Minority Interest.1',
             'Net Income Before Extraordinary Items',
             'Discontinued Operations',
             'Total Extraordinary Items',
             'Net Income Incl Extra Before Distributions',
             'Income Avail to Cmn Shareholders Excl Extra',
             'Income Avail to Cmn Shareholders Incl Extra',
             'Basic Weighted Average Shares',
             'Basic EPS Excluding Extraordinary Items',
             'Basic EPS Including Extraordinary Items',
             'Diluted Net Income',
             'Diluted Weighted Average Shares',
             'Diluted EPS Excluding Extraordinary Items',
             'Diluted EPS Including Extraordinary Items',
             'DPS - Common Stock Primary Issue',
             'Net Sales',
             'Revenue',
             'Total Revenue',
             'Cost of Revenue',
             'Cost of Revenue, Total',
             'Gross Profit',
             'Selling/General/Administrative Expense, Total',
             'Depreciation, Operating',
             'Depreciation And Amortization',
             'Investment Income - Operating',
             'Interest/Investment Income - Operating',
             'Interest Expense (Income), Net-Operating, Ttl',
             'Loss (Gain) on Sale of Assets - Operating',
             'Other Unusual Expense (Income)',
             'Unusual Expense (Income)',
             'Other Operating Expense',
             'Other, Net - Operating',
             'Other Operating Expenses, Total',
             'Total Operating Expense',
             'Operating Income',
             'Interest Expense - Non-Operating',
             'Interest Capitalized - Non-Operating',
             'Interest Expense, Net Non-Operating',
             'Investment Income - Non-Operating',
             'Interest/Invest Income - Non-Operating',
             'Interest Income (Expense), Net-Non-Operating, Total',
             'Other Non-Operating Income (Expense)',
             'Other, Net - Non-Operating',
             'Total Extraordinary Items.1']

        for col in columns:
            if col in filled_data.columns:
                continue
            else:
                filled_data[col] = np.nan
        filled_data = filled_data[columns]

            # 给补充好的表加一列主键
        innercode = SecuCode2InnerCode('0' + file[0:4])
        part1 = int(innercode)*1e8
        keys = []
        for date in filled_data['Income Statement Source Date']:
            # key = int(int(date[0:4]) * 1e4) + int(int(date[5:7]) * 1e2) + int(int(date[8:10])) + SecuCode
            key = date + part1
            keys.append(key)
        keys = pd.DataFrame(keys, columns=['key'])
        filled_data = filled_data.join(keys)

        # 对instrument进行统一，并加上一列innercode
        filled_data['Instrument'] = '0' + file[0:4]
        filled_data['InnerCode'] = innercode

        arr_filled_data = filled_data.values  # 转换为array

        # 导入Mongodb
        for i in range(len(arr_filled_data)):
            hk_initial(arr_filled_data[i, 0],
                        arr_filled_data[i, 1],
                        arr_filled_data[i, 2],
                        arr_filled_data[i, 3],
                        arr_filled_data[i, 4],
                        arr_filled_data[i, 5],
                        arr_filled_data[i, 6],
                        arr_filled_data[i, 7],
                        arr_filled_data[i, 8],
                        arr_filled_data[i, 9],
                        arr_filled_data[i, 10],
                        arr_filled_data[i, 11],
                        arr_filled_data[i, 12],
                        arr_filled_data[i, 13],
                        arr_filled_data[i, 14],
                        arr_filled_data[i, 15],
                        arr_filled_data[i, 16],
                        arr_filled_data[i, 17],
                        arr_filled_data[i, 18],
                        arr_filled_data[i, 19],
                        arr_filled_data[i, 20],
                        arr_filled_data[i, 21],
                        arr_filled_data[i, 22],
                        arr_filled_data[i, 23],
                        arr_filled_data[i, 24],
                        arr_filled_data[i, 25],
                        arr_filled_data[i, 26],
                        arr_filled_data[i, 27],
                        arr_filled_data[i, 28],
                        arr_filled_data[i, 29],
                        arr_filled_data[i, 30],
                        arr_filled_data[i, 31],
                        arr_filled_data[i, 32],
                        arr_filled_data[i, 33],
                        arr_filled_data[i, 34],
                        arr_filled_data[i, 35],
                        arr_filled_data[i, 36],
                        arr_filled_data[i, 37],
                        arr_filled_data[i, 38],
                        arr_filled_data[i, 39],
                        arr_filled_data[i, 40],
                        arr_filled_data[i, 41],
                        arr_filled_data[i, 42],
                        arr_filled_data[i, 43],
                        arr_filled_data[i, 44],
                        arr_filled_data[i, 45],
                        arr_filled_data[i, 46],
                        arr_filled_data[i, 47],
                        arr_filled_data[i, 48],
                        arr_filled_data[i, 49],
                        arr_filled_data[i, 50],
                        arr_filled_data[i, 51],
                        arr_filled_data[i, 52],
                        arr_filled_data[i, 53],
                        arr_filled_data[i, 54],
                        arr_filled_data[i, 55],
                        arr_filled_data[i, 56],
                        arr_filled_data[i, 57],
                        arr_filled_data[i, 58],
                        arr_filled_data[i, 59],
                        arr_filled_data[i, 60],
                        arr_filled_data[i, 61],
                        arr_filled_data[i, 62],
                        arr_filled_data[i, 63],
                        arr_filled_data[i, 64],
                        arr_filled_data[i, 65],
                        arr_filled_data[i, 66],
                        arr_filled_data[i, 67],
                        arr_filled_data[i, 68],
                        arr_filled_data[i, 69],
                        arr_filled_data[i, 70],
                        arr_filled_data[i, 71],
                        arr_filled_data[i, 72],
                        arr_filled_data[i, 73],
                        arr_filled_data[i, 74],
                        arr_filled_data[i, 75],
                        arr_filled_data[i, 76],
                        arr_filled_data[i, 77],
                        arr_filled_data[i, 78],
                        arr_filled_data[i, 79],
                        arr_filled_data[i, 80],
                        arr_filled_data[i, 81],
                        arr_filled_data[i, 82],
                        arr_filled_data[i, 83],
                        arr_filled_data[i, 84],
                        arr_filled_data[i, 85],
                        arr_filled_data[i, 86],
                        arr_filled_data[i, 87],
                        arr_filled_data[i, 88],
                        arr_filled_data[i, 89],
                        arr_filled_data[i, 90],
                        arr_filled_data[i, 91],
                        arr_filled_data[i, 92],
                        arr_filled_data[i, 93],
                        arr_filled_data[i, 94],
                        arr_filled_data[i, 95],
                        arr_filled_data[i, 96],
                        arr_filled_data[i, 97],
                        arr_filled_data[i, 98],
                        arr_filled_data[i, 99],
                        arr_filled_data[i, 100],
                        arr_filled_data[i, 101],
                        arr_filled_data[i, 102],
                        arr_filled_data[i, 103],
                        arr_filled_data[i, 104],
                        arr_filled_data[i, 105],
                        arr_filled_data[i, 106],
                        arr_filled_data[i, 107],
                        arr_filled_data[i, 108],
                        arr_filled_data[i, 109],
                        arr_filled_data[i, 110],
                        arr_filled_data[i, 111],
                        arr_filled_data[i, 112],
                        arr_filled_data[i, 113],
                        arr_filled_data[i, 114],
                        arr_filled_data[i, 115],
                        arr_filled_data[i, 116],
                        arr_filled_data[i, 117],
                        arr_filled_data[i, 118],
                        arr_filled_data[i, 119],
                        arr_filled_data[i, 120],
                        arr_filled_data[i, 121],
                        arr_filled_data[i, 122],
                        arr_filled_data[i, 123],
                        arr_filled_data[i, 124],
                        arr_filled_data[i, 125],
                        arr_filled_data[i, 126],
                        arr_filled_data[i, 127],
                        arr_filled_data[i, 128],
                        arr_filled_data[i, 129],
                        arr_filled_data[i, 130],
                        arr_filled_data[i, 131],
                        arr_filled_data[i, 132],
                        arr_filled_data[i, 133],
                        arr_filled_data[i, 134],
                        arr_filled_data[i, 135],
                        arr_filled_data[i, 136],
                        arr_filled_data[i, 137],
                        arr_filled_data[i, 138],
                        arr_filled_data[i, 139],
                        arr_filled_data[i, 140],
                        arr_filled_data[i, 141],
                        arr_filled_data[i, 142],
                        arr_filled_data[i, 143],
                        arr_filled_data[i, 144],
                        arr_filled_data[i, 145],
                        arr_filled_data[i, 146],
                        arr_filled_data[i, 147],
                        arr_filled_data[i, 148],
                        arr_filled_data[i, 149],
                        arr_filled_data[i, 150],
                        arr_filled_data[i, 151],
                        arr_filled_data[i, 152],
                        arr_filled_data[i, 153],
                        arr_filled_data[i, 154],
                        arr_filled_data[i, 155],
                        arr_filled_data[i, 156],
                        arr_filled_data[i, 157],
                        arr_filled_data[i, 158],
                        arr_filled_data[i, 159],
                        arr_filled_data[i, 160],
                        arr_filled_data[i, 161],
                       arr_filled_data[i, 162],
                       arr_filled_data[i, 163],).save()


########################
# daily

filePath = "E:/20190324/daily_all"
file_list = os.listdir(filePath)

# 定义一个针对daily数据的class

class hk_daily(MongoModel):
    Calc_Date = fields.IntegerField()
    Instrument = fields.CharField()
    Enterprise_Value_Daily_Time_Series = fields.FloatField()
    Outstanding_Shares = fields.FloatField()
    Company_Market_Cap = fields.FloatField()
    Price_To_Book_Value_Per_Share_Daily_Time_Series_Ratio = fields.FloatField()
    P_E_Daily_Time_Series_Ratio = fields.FloatField()
    Company_Shares_ = fields.FloatField()
    Total_Debt_To_Enterprise_Value_Daily_Time_Series_Ratio = fields.FloatField()
    Enterprise_Value_To_Sales_Daily_Time_Series_Ratio = fields.FloatField()
    Net_Debt_To_Enterprise_Value_Daily_Time_Series_Ratio = fields.FloatField()
    Price_To_Sales_Per_Share_Daily_Time_Series_Ratio = fields.FloatField()
    Enterprise_Value_To_EBIT_Daily_Time_Series_Ratio = fields.FloatField()
    Price_To_Cash_Flow_Per_Share_Daily_Time_Series_Ratio = fields.FloatField()
    Net_Debt_To_EBITDA_Daily_Time_Series_Ratio = fields.FloatField()
    Total_Debt_To_EBITDA_Daily_Time_Series_Ratio = fields.FloatField()
    Enterprise_Value_To_EBITDA_Daily_Time_Series_Ratio = fields.FloatField()
    Enterprise_Value_To_Operating_Cash_Flow_Daily_Time_Series_Ratio = fields.FloatField()
    Price_To_Tangible_Book_Value_Per_Share_Daily_Time_Series_Ratio = fields.FloatField()
    Forward_P_E_Daily_Time_Series_Ratio = fields.FloatField()
    Forward_P_E_G_Daily_Time_Series_Ratio = fields.FloatField()
    Forward_Enterprise_Value_To_EBIT_Daily_Time_Series_Ratio = fields.FloatField()
    Forward_Enterprise_Value_To_EBITDA_Daily_Time_Series_Ratio = fields.FloatField()
    Forward_Enterprise_Value_To_Sales_Daily_Time_Series_Ratio = fields.FloatField()
    Forward_Net_Debt_To_EBITDA_Daily_Time_Series_Ratio = fields.FloatField()
    Forward_Price_To_Book_Value_Per_Share_Daily_Time_Series_Ratio = fields.FloatField()
    Forward_Price_To_Cash_Flow_Per_Share_Daily_Time_Series_Ratio = fields.FloatField()
    Forward_Price_To_Sales_Per_Share_Daily_Time_Series_Ratio = fields.FloatField()
    Forward_Total_Debt_To_EBITDA_Daily_Time_Series_Ratio = fields.FloatField()
    Forward_Enterprise_Value_To_Operating_Cash_Flow_Daily_Time_Series_Ratio = fields.FloatField()
    Company_Market_Cap_1 = fields.FloatField()
    Issue_Market_Cap = fields.FloatField()
    key = fields.IntegerField(primary_key=True)
    InnerCode = fields.IntegerField()
    class Meta:
        write_concern = WriteConcern(j=True)
        connection_alias = 'hk_daily'
        final = True

# 连接Mongodb
connect("mongodb://root:rgf12345@192.168.0.119:27017/jydb?authSource=admin", alias="hk_daily")

# 退市股票代码
lost_stock_daily = []

for file in tqdm(file_list):

    filled_data = filedata2(filePath, file, tradingdays_list)

    # 对于退市的股票，filled_data长度为1，输出到指定文件夹
    if len(filled_data)==1:    # 查看2663.HK.csv有没有在lost_list里面，不在则是正确
        lost_stock_daily.append(filled_data[0])
        continue
    else:
    # 对于新得到的filled_data判断有没有缺失列，如果有就补齐
        # 由齐全数据得到的columns
        columns = ['Calc Date',
             'Instrument',
             'Enterprise Value (Daily Time Series)',
             'Outstanding Shares',
             'Company Market Cap',
             'Price To Book Value Per Share (Daily Time Series Ratio)',
             'P/E (Daily Time Series Ratio)',
             'Company Shares ',
             'Total Debt To Enterprise Value (Daily Time Series Ratio)',
             'Enterprise Value To Sales (Daily Time Series Ratio)',
             'Net Debt To Enterprise Value (Daily Time Series Ratio)',
             'Price To Sales Per Share (Daily Time Series Ratio)',
             'Enterprise Value To EBIT (Daily Time Series Ratio)',
             'Price To Cash Flow Per Share (Daily Time Series Ratio)',
             'Net Debt To EBITDA (Daily Time Series Ratio)',
             'Total Debt To EBITDA (Daily Time Series Ratio)',
             'Enterprise Value To EBITDA (Daily Time Series Ratio)',
             'Enterprise Value To Operating Cash Flow (Daily Time Series Ratio)',
             'Price To Tangible Book Value Per Share (Daily Time Series Ratio)',
             'Forward P/E (Daily Time Series Ratio)',
             'Forward P/E/G (Daily Time Series Ratio)',
             'Forward Enterprise Value To EBIT (Daily Time Series Ratio)',
             'Forward Enterprise Value To EBITDA (Daily Time Series Ratio)',
             'Forward Enterprise Value To Sales (Daily Time Series Ratio)',
             'Forward Net Debt To EBITDA (Daily Time Series Ratio)',
             'Forward Price To Book Value Per Share (Daily Time Series Ratio)',
             'Forward Price To Cash Flow Per Share (Daily Time Series Ratio)',
             'Forward Price To Sales Per Share (Daily Time Series Ratio)',
             'Forward Total Debt To EBITDA (Daily Time Series Ratio)',
             'Forward Enterprise Value To Operating Cash Flow (Daily Time Series Ratio)',
             'Company Market Cap.1',
             'Issue Market Cap']

        for col in columns:
            if col in filled_data.columns:
                continue
            else:
                filled_data[col] = np.nan
        filled_data = filled_data[columns]

        # 给补充好的表加一列主键
        innercode = SecuCode2InnerCode('0' + file[0:4])
        part1 = int(innercode)*1e8
        keys = []
        for date in filled_data['Calc Date']:
            # key = int(int(date[0:4])*1e4) + int(int(date[5:7])*1e2) + int(int(date[8:10])) + SecuCode
            key = date + part1
            keys.append(key)
        keys = pd.DataFrame(keys, columns = ['key'])
        filled_data = filled_data.join(keys)
        # 对instrument进行统一，并加上一列innercode
        filled_data['Instrument'] = '0' + file[0:4]
        filled_data['InnerCode'] = innercode

        arr_filled_data = filled_data.values   # 转换为array

        # 导入Mongodb
        for i in range(len(arr_filled_data)):
            hk_daily(arr_filled_data[i, 0], arr_filled_data[i, 1], arr_filled_data[i, 2], arr_filled_data[i, 3], arr_filled_data[i, 4],
                     arr_filled_data[i, 5], arr_filled_data[i, 6], arr_filled_data[i, 7], arr_filled_data[i, 8], arr_filled_data[i, 9],
                     arr_filled_data[i, 10], arr_filled_data[i, 11], arr_filled_data[i, 12], arr_filled_data[i, 13],arr_filled_data[i, 14],
                     arr_filled_data[i, 15], arr_filled_data[i, 16], arr_filled_data[i, 17], arr_filled_data[i, 18],arr_filled_data[i, 19],
                     arr_filled_data[i, 20], arr_filled_data[i, 21], arr_filled_data[i, 22], arr_filled_data[i, 23],arr_filled_data[i, 24],
                     arr_filled_data[i, 25], arr_filled_data[i, 26], arr_filled_data[i, 27], arr_filled_data[i, 28],arr_filled_data[i, 29],
                     arr_filled_data[i, 30], arr_filled_data[i, 31], arr_filled_data[i, 32], arr_filled_data[i, 33]).save()