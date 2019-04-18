# 读取Mongodb的数据
from odm_new.backtest01.getfield import *
from odm_new.backtest01.selectdata import *
from tqdm import tqdm
import pandas as pd

SecuCode = get_field('SecuCode')
InnerCode = get_field('InnerCode')
SecuCategory = get_field('SecuCategory')
# len(SecuCode)==len(InnerCode)
pd_SecuCode = pd.DataFrame(SecuCode, columns=['SecuCode'])
pd_InnerCode = pd.DataFrame(InnerCode, columns=['InnerCode'])
pd_SecuCategory = pd.DataFrame(SecuCategory, columns=['SecuCategory'])
pd_InnerCode = pd_InnerCode.join(pd_SecuCode)
pd_InnerCode = pd_InnerCode.join(pd_SecuCategory)
# # 只留下SecuCategory为3、4、51、53的行
# pd_InnerCode = pd_InnerCode[(pd_InnerCode['SecuCategory']==3)|(pd_InnerCode['SecuCategory']==4)|
#                    (pd_InnerCode['SecuCategory']==51)|(pd_InnerCode['SecuCategory']==53)]

def SecuCode2InnerCode(SecuCode):
    bol = pd_InnerCode['SecuCode']==SecuCode
    InnerCode = pd_InnerCode['InnerCode'][bol]
    return InnerCode.iloc[0]

# refresh the stock pool
# SecuCode = list(Secu_inner.index)

# # 对SecuCode进行循环，针对每一只股票，首先获取其InnerCode，然后以此为索引从Mongodb调用数据
# symbol_data = {}
# problem_code = []
#
# for s in tqdm(SecuCode[100:110]):
#     Inner = Secu_inner.loc[s][0]
#     if Inner in list(Secu_inner['InnerCode']):    # 只下载股票池中的股票
#         # loc是用index和columns当中的值进行索引
#         # iloc是不理会index和columns当中的值的，永远都是用从0开始的下标进行索引
#         SelectResults = select(int(Inner))
#         if len(SelectResults) > 0:     # 如果股票池中股票的数据为空集
#             symbol_data[Inner] = select(int(Inner))
#         else:        # 有些Inner查不到数据
#             problem_code.append(s)
#             continue
#     else:
#         problem_code.append(s)
#         continue