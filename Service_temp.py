import win32serviceutil
import win32service
import win32event
import servicemanager
import socket

import time
import schedule #Скачать

import smtplib
import email.mime.application as Application
import email.mime.multipart as Multipart
import email.mime.text as Text

from datetime import datetime
from dateutil.relativedelta import relativedelta

import pandas as pd #Скачать
import numpy as np #Скачать
from statsmodels.tsa.ar_model import AutoReg #Скачать
from scipy.signal import find_peaks #Скачать
import json

from sqlalchemy.engine import create_engine #Скачать
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
import cx_Oracle #Скачать

#Настройка log файла
file_log = "C:\\Users\\Prediction\\Documents\\ControlOfTemperature\\mylog.log"
import logging
logging.basicConfig(
    level=logging.INFO,
    filename = file_log,
    format = "%(asctime)s - [%(levelname)s] - %(funcName)s: %(lineno)d - %(message)s",
    datefmt='%Y-%m-%d %H:%M:%S',
    )
logging.getLogger("sqlalchemy.engine").setLevel(logging.INFO)

conf_file = 'C:\\Users\\Prediction\\Documents\\ControlOfTemperature\\predict_conf.json'
with open(conf_file, "r", encoding='utf-8-sig') as read_file:
    data = json.load(read_file)
Server = data['Server']

DIALECT = Server['DIALECT']
SQL_DRIVER = Server['SQL_DRIVER']
USERNAME = Server['USERNAME']
PASSWORD = Server['PASSWORD']
HOST = Server['HOST']
PORT = Server['PORT']
SERVICE = Server['SERVICE']

today = 0
period = Server['period']
param_max = Server['param_max']
param_min = Server['param_min']
day_of_predict = Server['day_of_predict']
critical_temp = Server['critical_temp']

cx_Oracle.init_oracle_client(lib_dir=r"C:\Users\Prediction\Documents\ControlOfTemperature\instantclient_21_6")

class AppServerSvc (win32serviceutil.ServiceFramework):
    _svc_name_ = "Prediction"
    _svc_display_name_ = "Prediction"
    _svc_description_ = "Predicting the temperature under the mixers"

    def __init__(self,args):
        win32serviceutil.ServiceFramework.__init__(self,args)
        self.hWaitStop = win32event.CreateEvent(None,0,0,None)
        socket.setdefaulttimeout(60)

    def SvcStop(self):
        self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
        win32event.SetEvent(self.hWaitStop)

    def SvcDoRun(self):
        servicemanager.LogMsg(servicemanager.EVENTLOG_INFORMATION_TYPE,
                              servicemanager.PYS_SERVICE_STARTED,
                              (self._svc_name_,''))
        self.main()

    def main(self):
        def predict():
            #Подключаемся к БД
            engine = create_engine(DIALECT + '+' + SQL_DRIVER + '://' + USERNAME + ':' + PASSWORD +'@' + HOST + ':' + str(PORT) + '/?service_name=' + SERVICE, echo=True)

            #Рассылка о прогнозе
            def send_mail_error():
                to_addres = ['Maksim.Gildenberg@rusal.com']
                try:
                    smtpObj = smtplib.SMTP('sibmail.rusal.com', 587)
                except Exception as e:
                    print(e)
                    smtpObj = smtplib.SMTP_SSL('sibmail.rusal.com', 465)
                #type(smtpObj) 
                smtpObj.ehlo()
                smtpObj.starttls()
                smtpObj.login('ЛОГИН', "ПАРОЛЬ")
                
                msg = Multipart.MIMEMultipart()
                msg['Subject'] = 'Сбой службы Прогнозирование температуры подин миксеров'
                msg['From'] = 'MES Support'
                msg['To'] = ", ".join(to_addres)
                
                filename=file_log
                fp=open(filename,'rb')
                att = Application.MIMEApplication(fp.read(),_subtype="xlsx")
                fp.close()
                att.add_header('Content-Disposition','attachment',filename='Прогнозирование температуры подин миксеров.log')
                msg.attach(att)

                body = '<b style="color: darkred">ВНИМАНИЕ! Произошёл сбой службы Прогнозирование температуры подин миксеров</b>'
                att = Text.MIMEText(body)
                att.add_header('Content-Type','text/html')
                msg.attach(att)
                smtpObj.sendmail('Maksim.Gildenberg@rusal.com', to_addres, msg.as_string())

                smtpObj.quit()

            #Выгружаем
            def list_of_data():
                sql_gage = "select id_tag from xtech.tag where id_el in (select id_el from xcommon.eltree where id_eltreetype = 9) and name not like '%Pech%'" # and id_equip = 410511
                list_of_gage = list(map(str,pd.read_sql_query(sql_gage, engine)['id_tag'].values))

                return list_of_gage

            #Выгружаем последнюю дату фактических значений для каждой точки температуры
            def list_last_date(list_of_gage):
                list_date = dict()
                for num_gage in list_of_gage:
                    sql_date = "select max(datevalue) as datevalue from xtech.tagvalueaggregated where id_tag = "+num_gage
                    last_date = pd.read_sql_query(sql_date, engine).loc[0,'datevalue']
                    if last_date is not None:
                        list_date[num_gage] = last_date.strftime('%Y-%m-%d %H:%M:%S')
                    else:
                        list_date[num_gage] = datetime(2020,2,18).strftime('%Y-%m-%d %H:%M:%S')

                return list_date

            #Фильтрация данных
            def filter_peaks(full_X):     
                full_X['isextrememax'] = np.nan
                full_X['isextrememin'] = np.nan
                data_temp = full_X['valuehour'].to_list()
                
                num = np.arange(0,len(data_temp))
                peak = find_peaks(data_temp,height=0,threshold=0)
                peak_pos_max = (num[peak[0]]).tolist()

                y2 = np.array(data_temp)*-1
                minima = find_peaks(y2)
                peak_pos_min = (num[minima[0]]).tolist()

                for j in peak_pos_max:           
                    full_X.iloc[j, full_X.columns.get_loc('isextrememax')] = 1
                    full_X.iloc[j, full_X.columns.get_loc('isextrememin')] = 0
                for j in peak_pos_min:           
                    full_X.iloc[j, full_X.columns.get_loc('isextrememin')] = 1
                    full_X.iloc[j, full_X.columns.get_loc('isextrememax')] = 0
                    
                return full_X.dropna()

            #Выгружаем актуальные данные по точкам температуры
            def input_new_data(list_of_gage, list_date):
                Data = pd.DataFrame()
                for num_gage in list_of_gage:
                    sql_gage = "SELECT id_tag, value, datestamp FROM xtech.tagvalue_ar " \
                    "WHERE id_tag = "+num_gage+" and (datestamp between to_date('"+list_date[num_gage]+"','yy-mm-dd hh24:mi:ss') " \
                    "and to_date('"+today+"','yy-mm-dd')) and VALUE between 1 and 500 ORDER BY id_tag, datestamp"
                    Data_New = pd.read_sql_query(sql_gage, engine)
                    if Data_New.empty:
                        continue
                    #Фильтрация по часам
                    day_prev = 0
                    gage = []
                    date_and_time = []
                    value = []
                    for j in range(Data_New.shape[0]):
                        date_beg = str(Data_New.iloc[j, Data_New.columns.get_loc('datestamp')]).split(' ')[1].split(':')[0]
                        if (date_beg != day_prev):
                            day_prev = date_beg
                            date_and_time.append(Data_New.iloc[j, Data_New.columns.get_loc('datestamp')])
                            value.append(Data_New.iloc[j, Data_New.columns.get_loc('value')])
                            gage.append(Data_New.iloc[j, Data_New.columns.get_loc('id_tag')])
                    Data_New = pd.DataFrame(data={'id_tag':gage,'datevalue':date_and_time,'valuehour':value})
                    #Фильтрация выбросов
                    full_X = Data_New['valuehour'].to_list()
                    for j in range(len(full_X)):
                        if (j % (period-1) == 0):
                            mid = np.round(np.median(full_X[j:j+period]),1)
                        if full_X[j] > param_max*mid or full_X[j] < mid/param_min:
                            full_X[j] = full_X[j-1]

                    count_del = 0
                    temp_X = full_X[-1]
                    for j in range(len(full_X)-2,0,-1):
                        if temp_X*param_max < full_X[j]:
                            if count_del == 0:
                                temp_j = j
                            count_del += 1
                            if count_del == 48:
                                full_X[:temp_j+1] = [np.nan] * len(full_X[:temp_j+1])
                                break
                        else:
                            temp_X = full_X[j]
                            count_del = 0

                    Data_New['valuehour'] = full_X
                    Data_New = filter_peaks(Data_New)
                    if Data_New.empty:
                        continue
                    Data = Data.append(Data_New, ignore_index=True)
                        
                return Data

            #Прогноз
            def predict_data(data, day_of_predict):
                try:
                    model = AutoReg(data, lags=1, trend='c', seasonal=True, period=day_of_predict).fit()
                    predictions = np.round(abs(model.predict(start=len(data), end=len(data)+day_of_predict-1,dynamic=False)),1)

                    return predictions
                except:
                    return list()

            #Выгружаем все отфильтрованные данные
            def input_predict_data(list_of_gage, list_date):
                global day_of_predict

                Predict_Data = pd.DataFrame()
                for num_gage in list_of_gage:
                    sql_gage = "SELECT id_tag, valuehour, datevalue FROM xtech.tagvalueaggregated " \
                    "WHERE id_tag = "+num_gage+" and isextrememax = 1 ORDER BY id_tag, datevalue"
                    Data_New = pd.read_sql_query(sql_gage, engine)
                    if Data_New.empty:
                        continue

                    height = Data_New['valuehour'].values
                    #Старый алгоритм удаления отключения мест отключения миксера, работает неидеально
                    # count_del = 0
                    # temp_X = height[-1]
                    # for j in range(len(height)-2,0,-1):
                    #     if temp_X*param_max < height[j]:
                    #         if count_del == 0:
                    #             temp_j = j
                    #         count_del += 1
                    #         if count_del == 24:
                    #             height[:temp_j+1] = [np.nan] * len(height[:temp_j+1])
                    #             break
                    #     else:
                    #         temp_X = height[j]
                    #         count_del = 0
                    # height = [x for x in height if ~np.isnan(x)]

                    prediction = predict_data(height, day_of_predict)
                    if len(prediction) == 0:
                        continue

                    date_and_time = []
                    x_hour = datetime.strptime(list_date[num_gage],'%Y-%m-%d %H:%M:%S')
                    for j in range(day_of_predict):
                        x_hour += relativedelta(days=1)
                        date_and_time.append(x_hour.strftime('%Y-%m-%d %H:%M:%S'))
                    
                    Data_New = pd.DataFrame(data={'id_tag':num_gage,'datevalueprediction':date_and_time,'valueprediction':prediction})
                    Predict_Data = Predict_Data.append(Data_New, ignore_index=True)

                return Predict_Data

            #Загрузка данных в таблицу 1 - Отфильтрованные данные
            def output_fact_data(Data_New):
                for j in range(Data_New.shape[0]):
                    id_tag = str(Data_New.iloc[j, Data_New.columns.get_loc('id_tag')])
                    date = Data_New.iloc[j, Data_New.columns.get_loc('datevalue')].strftime('%Y-%m-%d %H:%M:%S')
                    value = str(Data_New.iloc[j, Data_New.columns.get_loc('valuehour')])
                    loc_max = str(Data_New.iloc[j, Data_New.columns.get_loc('isextrememax')])
                    loc_min = str(Data_New.iloc[j, Data_New.columns.get_loc('isextrememin')])
                    sql_insert = "INSERT INTO XTECH.TAGVALUEAGGREGATED(ID_TAG,DATEVALUE,VALUEHOUR,ISEXTREMEMAX,ISEXTREMEMIN) VALUES("+id_tag+",TIMESTAMP '"+date+"',"+value+","+loc_max+","+loc_min+")"
                    engine.execute(sql_insert)

            #Загрузка данных в таблицу 2 - Спрогнозированные данные
            def output_predict_data(Data_New, list_date):
                new_date = max(list_date.values())
                sql_insert_date = "INSERT INTO XTECH.PREDICTION(ID_PREDICTIONTYPE,DATEPREDICTION) VALUES(1, to_date('"+new_date+"','yy-mm-dd hh24:mi:ss'))"
                engine.execute(sql_insert_date)
                sql_insert_date = "select max(id_prediction) as id_prediction from xtech.prediction"
                insert_date = pd.read_sql_query(sql_insert_date, engine)
                id_prediction = str(insert_date['id_prediction'][0])
                for j in range(Data_New.shape[0]):
                    id_tag = str(Data_New.iloc[j, Data_New.columns.get_loc('id_tag')])
                    date = Data_New.iloc[j, Data_New.columns.get_loc('datevalueprediction')]
                    value = str(Data_New.iloc[j, Data_New.columns.get_loc('valueprediction')])
                    sql_insert = "INSERT INTO XTECH.TAGVALUEPREDICTION(ID_TAG,ID_PREDICTION,DATEVALUEPREDICTION,VALUEPREDICTION) VALUES("+id_tag+","+id_prediction+",TIMESTAMP '"+date+"',"+value+")"
                    engine.execute(sql_insert)
            
            try:
                global day_of_predict, critical_temp
                today = datetime.now().strftime('%Y-%m-%d')
                day_of_predict = Server['day_of_predict']
                critical_temp = Server['critical_temp']
                #Номера тегов точек температур
                list_of_gage = list_of_data()
                #Последняя дата точек температуры
                list_date = list_last_date(list_of_gage)

                Data_New = input_new_data(list_of_gage, list_date)
                logging.info('Выгружены новые данные по точкам температуры')

                if not Data_New.empty:
                    output_fact_data(Data_New)
                    logging.info('Загружены отфильтрованные новые данные')
                
                list_date = list_last_date(list_of_gage)
                Predict_Data = input_predict_data(list_of_gage, list_date)
                logging.info('Выгружены все спрогнозированные данные')

                output_predict_data(Predict_Data, list_date)
                logging.info('Загружены спрогнозированные данные')
            except:
                send_mail_error()
        
        def clear_log():
            open(file_log, 'w').close()

        schedule.every().day.at("00:00").do(predict)
        schedule.every().saturday.at("00:00").do(clear_log)

        while True:
            schedule.run_pending()
            time.sleep(1)

if __name__ == '__main__':
    win32serviceutil.HandleCommandLine(AppServerSvc)