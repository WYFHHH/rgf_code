import pandas as pd

# 针对daily 财务数据的数据整理函数

def filedata2(filePath, file, tradingdays_list):
    # 首先判断这是不是一只退市的/新上市的股票
    df = pd.read_csv(filePath + '/' + file)
    if df.shape[0] == 1 and df.shape[1] == 1:    # 行和列都为1
        return [file]
    elif 'Calc Date' not in df.columns:
        return [file]
    else:

        date = pd.read_csv(filePath + '/' + file, usecols = [1] ).dropna()  # 有效时间
        length = len(date)  # 有效长度
        colnums = list(df.columns)   # 所有列名
        hold_colnums = []
        for colnum in colnums:
            if colnum[0:9] == 'Calc Date':
                continue
            else:
                hold_colnums.append(colnum)
        # 保留除Instrument和日期外的所有列
        df_part = df[hold_colnums]
        # 截取有效日期的长度
        df_new = df_part.iloc[0:length]
        df_new = df_new.join(date)
        df = df_new
        df.sort_values(by=['Calc Date'], ascending = True, inplace = True)
        unique_dates = df['Calc Date'].unique()

        dates_reserve = pd.DataFrame()
        for i in range(len(unique_dates)):
            unique_date = unique_dates[i]  # 找到本次需要筛选的日期
            dates_needs_filter = df[df['Calc Date'] == unique_date]
            if len(dates_needs_filter) < 2:
                dates_reserve = dates_reserve.append(dates_needs_filter, ignore_index=True)
                continue
            else:
                date_reserve = dates_needs_filter.iloc[len(dates_needs_filter)-1]
                dates_reserve = dates_reserve.append(date_reserve, ignore_index=True)
            # 调整日期格式
        dates_reserve['Calc Date'] = 10000 * dates_reserve['Calc Date'].str[:4].apply(int) + \
                                             100 * dates_reserve['Calc Date'].str[5:7].apply(int) + \
                                                        dates_reserve['Calc Date'].str[8:10].apply(int)
        # merge两张表
        tradingdays = pd.DataFrame({'Calc Date':tradingdays_list})
        nan_data =  pd.merge(tradingdays, dates_reserve, how='left')
        nan_data.sort_values(by=['Calc Date'], ascending=True, inplace=True)
        # 填充nan
        fill_data = nan_data.fillna(method='ffill')
        return fill_data