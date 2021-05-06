import datetime
import time
import smtplib
from email.mime.text import MIMEText
from qcloudsms_py import SmsSingleSender
from qcloudsms_py.httpclient import HTTPError

from passfile import mail_host
from passfile import mail_user
from passfile import mail_pass
from passfile import sender

from passfile import appid
from passfile import appkey
from passfile import phone_numbers
from passfile import template_id
from passfile import sms_sign

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