import pandas as pd

# 针对处daily外的财务数据填充函数

def filedata(filePath, file,tradingdays_list):
    df = pd.read_csv(filePath + '/' + file)     #注意这里df是乱序的
    # 首先判断这是不是一只退市的/新上市的股票
    if df.shape[0] == 1 and df.shape[1] == 1:    # 行和列都为1
        return df['Instrument']
    elif 'Income Statement Source Date' not in df.columns:
        return df['Instrument']
    else:
        # aa = df.columns.values.tolist()  # 返回所有的列名
        df.sort_values(by=['Income Statement Source Date', 'Period End Date'], ascending = True, inplace = True)
        unique_dates = df['Income Statement Source Date'].unique()

        dates_reserve = pd.DataFrame()
        for i in range(len(unique_dates)):
            unique_date = unique_dates[i]  # 找到本次需要筛选的日期
            dates_needs_filter = df[df['Income Statement Source Date'] == unique_date]
            if len(dates_needs_filter) < 2:
                dates_reserve = dates_reserve.append(dates_needs_filter, ignore_index=True)
                continue
            else:
                date_reserve = dates_needs_filter.iloc[len(dates_needs_filter)-1]
                dates_reserve = dates_reserve.append(date_reserve, ignore_index=True)
            # 调整日期格式
        dates_reserve['Income Statement Source Date'] = 10000 * dates_reserve['Income Statement Source Date'].str[:4].apply(int) + \
                                             100 * dates_reserve['Income Statement Source Date'].str[5:7].apply(int) + \
                                                        dates_reserve['Income Statement Source Date'].str[8:10].apply(int)
        dates_reserve['Period End Date'] = 10000 * dates_reserve['Period End Date'].str[:4].apply(int) + \
                                                        100 * dates_reserve['Period End Date'].str[5:7].apply(int) + \
                                                        dates_reserve['Period End Date'].str[8:10].apply(int)


        # merge两张表
        tradingdays = pd.DataFrame({'Income Statement Source Date':tradingdays_list})
        nan_data =  pd.merge(tradingdays, dates_reserve, how='left')
        indexcol = nan_data['Instrument'].notnull()    #非nan的地方为True
        valuelist = []
        for i in range(len(indexcol)):      # 获得所有非nan值在原表的索引
            if indexcol[i] == True:
                valuelist.append(i)
        # 填充nan
        fill_data = nan_data.fillna(method='ffill')
        return fill_data