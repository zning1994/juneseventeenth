import akshare as ak
import pandas as pd
import datetime
import time
import _thread
from sqlalchemy import create_engine

contractCodeList = list()
SLEEP_TIME=5
tradingDayFlag = None
tradingTimeFlag = None
morn_start = time.strptime("09:25:00", "%H:%M:%S")
morn_end = time.strptime("11:35:00", "%H:%M:%S")
noon_start = time.strptime("12:55:00", "%H:%M:%S")
noon_end = time.strptime("15:05:00", "%H:%M:%S")
host = '127.0.0.1'
port = 3306
db = 'youguess'
user = 'youguess'
password = 'youguess'

def getOptionInfo(threadcount, contractCodeList):
    option_sina_sse_info_df = pd.DataFrame()
    # 遍历所有合约获取当前所有合约信息
    for i in contractCodeList:
        # 获取实时所有信息
        option_sina_sse_spot_price_df_temp = ak.option_sina_sse_spot_price(code=i)
        option_sina_sse_greeks_df_temp = ak.option_sina_sse_greeks(code=i)

        # 合并df并去重
        option_sina_sse_info_temp = option_sina_sse_spot_price_df_temp.append(option_sina_sse_greeks_df_temp)
        option_sina_sse_info_temp = option_sina_sse_info_temp[option_sina_sse_info_temp.duplicated() == False]

        # 原表没有期权合约代码 加入一行
        option_sina_sse_info_temp.loc[option_sina_sse_info_temp.index.size] = ['期权合约代码', i]

        if (option_sina_sse_info_df.empty):
            option_sina_sse_info_df = option_sina_sse_info_temp
        else:
            option_sina_sse_info_df = option_sina_sse_info_df.reset_index(drop=True)
            option_sina_sse_info_temp = option_sina_sse_info_temp.reset_index(drop=True)
            option_sina_sse_info_df = pd.concat([option_sina_sse_info_df, option_sina_sse_info_temp.drop(['字段'], axis=1)],
                                                axis=1, join='inner')

        # print("[" + str(datetime.datetime.today()) + "] ENDED Thread"+str(threadcount)+" Contract ID " + i + " Batch Task.")
        time.sleep(0.13)

    option_sina_sse_info_df = option_sina_sse_info_df.set_index('字段').rename_axis(None).T
    option_sina_sse_info_df.rename(columns={'申买量一 ': '申买量一'}, inplace=True)
    print("[" + str(datetime.datetime.today()) + "] ENDED Thread"+str(threadcount)+" Option Info Dataframe Transpose.")
    option_sina_sse_info_df.to_sql(name="result", con=engine, if_exists='append',index=False)
    # option_sina_sse_info_df.to_csv('out_'+str(int(time.time()))+'.csv', encoding='utf-8')
    # print("[" + str(datetime.datetime.today()) + "] ENDED Thread"+str(threadcount)+" Option Info Dataframe Export to CSV.")
    # return option_sina_sse_info_df

def getContractID():
    # 遍历沪市50ETF和300ETF当前所有挂出合约
    option_sina_sse_list_50ETF = ak.option_sina_sse_list(symbol="50ETF", exchange="null")
    for i in option_sina_sse_list_50ETF:
        option_sina_sse_codes_tuple = ak.option_sina_sse_codes(trade_date=i, underlying="510050")
        for j in option_sina_sse_codes_tuple:
            for k in j:
                contractCodeList.append(k)

    option_sina_sse_list_300ETF = ak.option_sina_sse_list(symbol="300ETF", exchange="null")
    for i in option_sina_sse_list_300ETF:
        option_sina_sse_codes_tuple = ak.option_sina_sse_codes(trade_date=i, underlying="510300")
        for j in option_sina_sse_codes_tuple:
            for k in j:
                contractCodeList.append(k)

    print("[" + str(datetime.datetime.today()) + "] ENDED GET Contract Code List. The Contracts have as follow: " + str(contractCodeList))

if __name__ == '__main__':

    engine = create_engine(str(r"mysql+pymysql://%s:" + '%s' + "@%s/%s?charset=utf8mb4") % (user, password, host, db))

    # 获取新浪财经的股票交易日历数据 单次返回从 1990-12-19 到 2020-12-31 之间的股票交易日历数据
    tool_trade_date_hist_sina_df = ak.tool_trade_date_hist_sina()

    # 判断当天是否为交易日 datetime.date.today()获取今天日期
    while True:
        # True为非交易日，False为交易日
        # if (tool_trade_date_hist_sina_df[tool_trade_date_hist_sina_df['trade_date'] == "2021-04-16"].empty):
        if (tool_trade_date_hist_sina_df[tool_trade_date_hist_sina_df['trade_date'] == str(datetime.date.today())].empty):
            if (tradingDayFlag != False):
                print("[" + str(datetime.datetime.today()) + "] Today is off day. Have fun!")
            tradingDayFlag = False
            time.sleep(SLEEP_TIME)
            continue
        else:
            if (tradingDayFlag != True):
                print("[" + str(datetime.datetime.today()) + "] Today is trading day. Let's do it!")
                getContractID()
            tradingDayFlag = True
            i=0
            while True:
                now = time.strptime(time.strftime("%H:%M:%S", time.localtime()), "%H:%M:%S")
                if ((now >= morn_start and now <= morn_end) or (now >= noon_start and now <= noon_end)):
                    tradingTimeFlag = True
                    # 创建线程
                    try:
                       _thread.start_new_thread( getOptionInfo, (++i, contractCodeList, ) )
                    except:
                       print ("Error: Cannot start thread.")
                    i=i+1
                    time.sleep(60)
                elif(now >= morn_end and now <= noon_start):
                    if(tradingTimeFlag != False):
                        print("[" + str(datetime.datetime.today()) + "] Trading day break, have a good rest.")
                    tradingTimeFlag = False
                    continue
                else:
                    if(tradingTimeFlag != False):
                        print("[" + str(datetime.datetime.today()) + "] Trading day end or not begin, have fun.")
                    tradingTimeFlag = False
                    break