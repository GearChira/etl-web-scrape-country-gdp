import requests
from bs4 import BeautifulSoup
import sqlite3
import numpy as np
import pandas as pd
from datetime import datetime

url = 'https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29#Table'
csv_path = 'Countries_by_GDP.csv'
db_path = 'World_Economies.db'
table_name = 'Countries_by_GDP'
table_attribs = ['Country','GDP_USD_millions']
sql_conn = sqlite3.connect(db_path)
log_file = 'etl_project_log.txt'

def extract(target_url, table_attributes):
    url_text = requests.get(target_url).text
    html_parse = BeautifulSoup(url_text,'html.parser')
    df = pd.DataFrame(columns=table_attributes)
    html_tables = html_parse.find_all('tbody')
    html_rows = html_tables[2].find_all('tr')

    for row in html_rows:
        col = row.find_all('td')
        if len(col) != 0 and col[0].find('a') is not None and '—' not in col[2]:
            data_dict = {'Country': col[0].a.contents[0],
                        'GDP_USD_millions': col[2].contents[0]}
            df1 = pd.DataFrame(data_dict, index=[0])
            df = pd.concat([df,df1], ignore_index=True)

    return df

def transform(extracted_data):
    extracted_data['GDP_USD_millions'] = round(extracted_data['GDP_USD_millions'].str.replace(',','').astype(float) / 1000,2)
    extracted_data.rename(columns={'GDP_USD_millions': 'GDP_USD_billions'}, inplace=True)
    
    return extracted_data

def load_csv(transformed_data):
    load_to_csv = transformed_data.to_csv(csv_path)
    return load_to_csv

def load_db(transformed_data,sql_connection):
    load_to_db = transformed_data.to_sql(table_name, sql_connection, if_exists='replace', index=False)
    return load_to_db

def run_query(sql_statement, sql_connection):
    print(sql_statement)
    output = pd.read_sql(sql_statement, sql_connection)
    print(output)

def log_progress(message):
    time_format = '%Y-%m-%d %H:%M:%S'
    time_now = datetime.now()
    current_time = time_now.strftime(time_format)
    with open(log_file,'a') as f:
        f.write(current_time + ': ' +  message + '.' + '\n')


log_progress('ETL Process Started')

log_progress('Extraction Started')
extracted_data = extract(url,table_attribs)

log_progress('Extraction Completed')

log_progress('Transformation Started')
transformed_data = transform(extracted_data)

log_progress('Transformation Completed')

log_progress('Loading to CSV Started')
load_csv(transformed_data)

log_progress('Loading to CSV Completed')

log_progress('Loading to SQL DB Started')
load_db(transformed_data,sql_conn)

log_progress('Loading to SQL DB Completed')

log_progress('Query Output from SQL DB')
sql_query_statement = f'SELECT * FROM {table_name} WHERE GDP_USD_billions >= 100;'
run_query(sql_query_statement,sql_conn)

log_progress('Close Query Connection')
sql_conn.close()

log_progress('ETL Process Completed')
