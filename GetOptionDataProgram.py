import akshare as ak
import pandas as pd
import datetime
import time
import _thread
from sqlalchemy import create_engine
import smtplib
from email.mime.text import MIMEText
from qcloudsms_py import SmsSingleSender
from qcloudsms_py.httpclient import HTTPError

from passfile import host
from passfile import port
from passfile import db_options
from passfile import user
from passfile import password

from passfile import mail_host
from passfile import mail_user
from passfile import mail_pass
from passfile import sender

from passfile import appid
from passfile import appkey
from passfile import phone_numbers
from passfile import template_id
from passfile import sms_sign

contractCodeList = list()
SLEEP_TIME=5
tradingDayFlag = None
tradingTimeFlag = None
morn_start = time.strptime("09:25:00", "%H:%M:%S")
morn_end = time.strptime("11:46:00", "%H:%M:%S")
noon_start = time.strptime("12:55:00", "%H:%M:%S")
noon_end = time.strptime("15:16:00", "%H:%M:%S")

def sendSMSviaQcloud(sysname,health):
    result=""
    ssender = SmsSingleSender(appid, appkey)
    params = []  # 当模板没有参数时，`params = []`
    params.append(str(datetime.datetime.today().strftime('%H:%M')))
    params.append(sysname)
    params.append(health)
    try:
      result = ssender.send_with_param(86, phone_numbers[0], template_id, params, sign=sms_sign, extend="", ext="")
    except HTTPError as e:
      print("[" + str(datetime.datetime.today().strftime('%Y-%m-%d %H:%M')) + "] ERROR ",e)
    except Exception as e:
      print("[" + str(datetime.datetime.today().strftime('%Y-%m-%d %H:%M')) + "] ERROR ",e)

    print("[" + str(datetime.datetime.today().strftime('%Y-%m-%d %H:%M')) + "] INFO "+str(result))

def sendemail(content, subject, to):
    message = MIMEText(content, "plain", "utf-8")
    message['From'] = "JuneSeventeenth Finance <noreply@finance.zning.net.cn>" # 发送者
    message['To'] = to # 接收者
    message['Subject'] = subject

    try:
        smtpObj = smtplib.SMTP()
        smtpObj.connect(mail_host,25)
        smtpObj.login(mail_user,mail_pass)
        smtpObj.sendmail(sender,to,message.as_string())
        smtpObj.quit()
        print("[" + str(datetime.datetime.today().strftime('%Y-%m-%d %H:%M')) + "] Send mail success.")
    except smtplib.SMTPException as e:
        print("[" + str(datetime.datetime.today().strftime('%Y-%m-%d %H:%M')) + "] ERROR ",e)

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

        # print("[" + str(datetime.datetime.today().strftime('%Y-%m-%d %H:%M')) + "] ENDED Thread"+str(threadcount)+" Contract ID " + i + " Batch Task.")
        time.sleep(0.13)

    option_sina_sse_info_df = option_sina_sse_info_df.set_index('字段').rename_axis(None).T
    option_sina_sse_info_df.rename(columns={'申买量一 ': '申买量一'}, inplace=True)
    option_sina_sse_info_df.to_sql(name="result", con=engine, if_exists='append',index=False)
    print("[" + str(datetime.datetime.today().strftime('%Y-%m-%d %H:%M')) + "] ENDED Thread"+str(threadcount)+" Option Info Had into Database.")
    # option_sina_sse_info_df.to_csv('out_'+str(int(time.time()))+'.csv', encoding='utf-8')
    # print("[" + str(datetime.datetime.today().strftime('%Y-%m-%d %H:%M')) + "] ENDED Thread"+str(threadcount)+" Option Info Dataframe Export to CSV.")
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

    print("[" + str(datetime.datetime.today().strftime('%Y-%m-%d %H:%M')) + "] ENDED GET Contract Code List. The Contracts have as follow: " + str(contractCodeList))


if __name__ == '__main__':

    engine = create_engine(str(r"mysql+pymysql://%s:" + '%s' + "@%s/%s?charset=utf8mb4") % (user, password, host, db_options))

    # 获取新浪财经的股票交易日历数据 单次返回从 1990-12-19 到 2020-12-31 之间的股票交易日历数据
    tool_trade_date_hist_sina_df = ak.tool_trade_date_hist_sina()

    # 判断当天是否为交易日 datetime.date.today()获取今天日期
    while True:
        # True为非交易日，False为交易日
        # if (tool_trade_date_hist_sina_df[tool_trade_date_hist_sina_df['trade_date'] == "2021-04-16"].empty):
        if (tool_trade_date_hist_sina_df[tool_trade_date_hist_sina_df['trade_date'] == str(datetime.date.today())].empty):
            if (tradingDayFlag != False):
                print("[" + str(datetime.datetime.today().strftime('%Y-%m-%d %H:%M')) + "] Today is off day. Have fun!")
                # sendemail("今日非交易日，休息一下吧。", "[" + str(datetime.datetime.today().strftime('%Y-%m-%d %H:%M')) + "] 今日非交易日，休息一下吧。", "zhn038@163.com")
            tradingDayFlag = False
            time.sleep(SLEEP_TIME)
            continue
        else:
            if (tradingDayFlag != True):
                print("[" + str(datetime.datetime.today().strftime('%Y-%m-%d %H:%M')) + "] Today is trading day. Let's do it!")
                # sendemail("今日交易日，大干一场吧！", "[" + str(datetime.datetime.today().strftime('%Y-%m-%d %H:%M')) + "] 今日交易日，大干一场吧！", "zhn038@163.com")
            tradingDayFlag = True
            i=0
            while True:
                now = time.strptime(time.strftime("%H:%M:%S", time.localtime()), "%H:%M:%S")
                if ((now >= morn_start and now <= morn_end) or (now >= noon_start and now <= noon_end)):
                    if (tradingTimeFlag != True):
                        sendemail("早盘开始，期权数据采集运行正常。", "[" + str(datetime.datetime.today().strftime('%Y-%m-%d %H:%M')) + "] 早盘开始，期权数据采集运行正常。", "zhn038@163.com")
                        sendSMSviaQcloud("期权数据采集", "正常.今日交易日.")
                        getContractID()
                    tradingTimeFlag = True
                    # 创建线程
                    try:
                       _thread.start_new_thread( getOptionInfo, (++i, contractCodeList, ) )
                    except:
                       print ("Error: Cannot start thread.")
                       sendemail("期权数据采集异常：线程无法启动。", "[" + str(datetime.datetime.today().strftime('%Y-%m-%d %H:%M')) + "] ETF数据采集异常：线程无法启动。","zhn038@163.com")
                       sendSMSviaQcloud("期权数据采集", "线程异常无法启动")
                    i=i+1
                    time.sleep(60)
                elif(now >= morn_end and now <= noon_start):
                    if(tradingTimeFlag != False):
                        print("[" + str(datetime.datetime.today().strftime('%Y-%m-%d %H:%M')) + "] Trading day break, have a good rest.")
                        sendemail("早盘结束，期权数据采集运行正常。", "[" + str(datetime.datetime.today().strftime('%Y-%m-%d %H:%M')) + "] 早盘结束，期权数据采集运行正常。", "zhn038@163.com")
                        sendSMSviaQcloud("期权数据采集", "正常.早盘结束.")
                    tradingTimeFlag = False
                    continue
                else:
                    if(tradingTimeFlag != False):
                        print("[" + str(datetime.datetime.today().strftime('%Y-%m-%d %H:%M')) + "] Trading day end or not begin, have fun.")
                        sendemail("午盘结束或早盘未开始，期权数据采集运行正常。", "[" + str(datetime.datetime.today().strftime('%Y-%m-%d %H:%M')) + "] 午盘结束或早盘未开始，期权数据采集运行正常。", "zhn038@163.com")
                        sendSMSviaQcloud("期权数据采集", "正常.午盘或全天结束.")
                    tradingTimeFlag = False
                    break