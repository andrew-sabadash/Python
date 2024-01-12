import telegram
from datetime import datetime, timedelta
import pandas as pd
import psycopg2
import numpy as np
import io
from io import StringIO
import requests
import seaborn as sns
import matplotlib.pyplot as plt
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.models import Variable
my_token = Variable.get('bot_token') # токен бота
bot = telegram.Bot(token=my_token) # получаем доступ
chat_id = 1234064601
 # Параметры соединения
connection = Variable.get('connect',deserialize_json=True)

# Для поиска аномалий применим метод межквартильного размаха
def anomaly(df,metric,alpha = 1.5):
    df_current = df.loc[df['ts'] == df['ts'].max()]
    current_metric = df_current[metric].iloc[0]
    df_trimmed = df.loc[(df['ts'] >= df['ts'].max()-timedelta(minutes = 120))&(df['ts']<df['ts'].max())]  # Выбираю период N = 8
    quartile25 = df_trimmed[metric].quantile(q = 0.25)
    quartile75 = df_trimmed[metric].quantile(q = 0.75)
    IQR = quartile75 - quartile25
    if current_metric < quartile25 - alpha * IQR or\
       current_metric > quartile75 + alpha * IQR:
        alert = 1
    else:
        alert = 0
    return alert, current_metric
# Дефолтные параметры, которые прокидываются в таски
default_args = {
    'owner': 'a.sabadash',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 6, 21),
    }
# Интервал запуска DAG
schedule_interval = '15 * * * *'

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def my_alerts_script():
    
    @task
    def extract():
        query = "
            SELECT
            date_bin('15 minutes',time) as ts
            ,count(distinct user_id) as users
            FROM actions
            WHERE ts >= today() and ts < date_bin('15 minutes',now())
            GROUP BY ts
            ORDER BY ts"
        cursor = connection.cursor()    
        cursor.execute(query)         
        df = cursor.fetchall()        
        columns = []                     
        for head in cursor.description:
        columns.append(head[0])
        return df
    
    @task
    def users_alert(df):
        metric = 'users'
        alert, current_metric = anomaly(df,metric)
        if alert:
            msg = f'Аномальное значение метрики {metric}: {current_metric}'
            sns.set(rc={'figure.figsize': (16, 10)}, style ='whitegrid')
            ax = sns.lineplot(x = 'ts', y = metric, data = df)
            ax.set(xlabel='time') # задаем имя оси Х
            ax.set(ylabel = metric) # задаем имя оси У
            ax.set_title(f'{metric}')
            plot_object = io.BytesIO()
            ax.figure.savefig(plot_object)
            plot_object.seek(0)
            plot_object.name = f'{metric}.png'
            plt.close()
            bot.sendMessage(chat_id=chat_id, text = msg) # Отсылаем сообщение в мессенджер
            bot.sendPhoto(chat_id=chat_id, photo=plot_object) # Отсылаем диграмму в мессенджер
     
  
    df = extract()
    users_alert = users_alert(df)
my_alerts_script = my_alerts_script()