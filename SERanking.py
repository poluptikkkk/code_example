
import requests
import json
import pandas as pd
from datetime import date
from sqlalchemy import create_engine
import pandasql as ps
import pymysql
import time
import numpy as np
from config import token, type_list, order_field, rows_limit, source_list, to_excel, to_sql
from config import database_username, database_password, database_ip, database_host, database_port, database_name, data_table

def mysql_connection():
    conn = create_engine(
        'mysql+pymysql://{0}:{1}@{2}/{3}'.format(database_username, database_password, database_ip, database_name))
    return conn

def mysql_connection2():
    connection = pymysql.connect(
    host = database_host,
    port = database_port,
    user = database_username,
    password = database_password,
    database = database_name,
    cursorclass=pymysql.cursors.DictCursor)
    return connection

def add_to_db(new_load, old_load): # параметры - df
    #old_load['usl'] = old_load['domain'] + old_load['date'].astype(str) 
    #new_load['usl'] = new_load['domain'] + new_load['date'].astype(str)
    query = '''SELECT * 
    FROM new_load LEFT JOIN (SELECT usl from old_load) as old_load
    ON new_load.usl = old_load.usl 
    WHERE old_load.usl IS NULL'''
    to_add_df = ps.sqldf(query, locals())
    del to_add_df['usl']
    return to_add_df   

def get_data(domain, source, page, limit, order_field, type_, token, cols):
    start__getdata_time = time.time()
    df_stack = pd.DataFrame(columns = cols)
    url = 'https://api4.seranking.com/research/{source}/keywords/?domain={domain}&type={type_}&order_field={order_field}&order_type=desc&token={token}&page={page}&limit={limit}'.format(source = source, domain = domain, token = token, type_ = type_, order_field = order_field, page = page, limit = limit)
    response = requests.get(
    url)

    print('got response')
    print("- Response time: %s sec -" % (round(time.time() - start__getdata_time)))
    response_text = response.text
    if 'Invalid domain' not in response_text:
        if 'Rows limit exceeded' not in response_text:
            json_object = json.loads(response_text)
            if len(json_object) != 0:
                #for i in range(len(json_object)):
                #    df_stack= df_stack.append(json_object[i], ignore_index=True)
                #    for j in df_stack.columns:
                #        if j not in cols:
                #            del df_stack[j]
                    
                #    df_stack['upload_date'] = df_stack['upload_date'].apply(lambda x: date.today())
                #    df_stack['domain'] = df_stack['domain'].apply(lambda x: domain)
                #    df_stack['search_engine'] = df_stack['search_engine'].apply(lambda x: se_dict[source])
                #    df_stack['region'] = df_stack['region'].apply(lambda x: regions_dict[source])
                #    df_stack['data_update_date'] = df_stack['data_update_date'].apply(lambda x: date.today().replace(day=1))
                
                
                df_stack = pd.json_normalize(json_object)
                for j in df_stack.columns:
                    if j not in cols:
                        del df_stack[j]
                df_stack.insert(loc=0, column='idx', value = np.nan)
                df_stack.insert(loc=1, column='upload_date', value=date.today())
                df_stack.insert(loc=2, column='data_update_date', value=date.today().replace(day=1))
                df_stack.insert(loc=3, column='domain', value=domain)
                df_stack.insert(loc=4, column='search_engine', value=se_dict[source])
                df_stack.insert(loc=5, column='region', value=regions_dict[source])
                
  
                print("- made df_stack: %s sec -" % (round(time.time() - start__getdata_time)))
            #else:
            #    print('empty data by domain!')
                
    else:
        print('Invalid domain!')
    
    return df_stack

    
regions_dict = {'ru_msk_ya': 'msk', 'ru': 'russia'}
se_dict = {'ru_msk_ya': 'yandex', 'ru': 'google'}
limit = 1000

def main():
    with open("domains.txt", encoding='utf-8') as file:
        domainlist = [row.strip() for row in file]
          
    cols  = ['idx', 'upload_date', 'data_update_date', 'domain','search_engine', 'region', 'keyword', 
             'position', 'prev_pos', 'volume', 'cpc', 'competition', 'url', 'kei', 
             'total_sites', 'traffic', 'traffic_percent', 'price', 'block', 'snippet_num', 
             'snippets_count', 'snippet_title', 'snippet_description', 'snippet_display_url']
    df = pd.DataFrame(columns = cols)
    
    for domain in domainlist:
        #print(domain)
        for source in source_list:
            for type_ in type_list:
                for page in range(1, (rows_limit//limit)+1, 1):
                    print(domain + ' ' + source + ' ' + type_ + ' ' + str(page))
                    start_time = time.time()
                    while True:
                        try:
                            df_stack = get_data(domain=domain, source=source, page=page, limit=limit, order_field=order_field, type_=type_, token=token, cols=cols)
                            break
                        except Exception as e:
                            print(str(e))
                            continue
                    df= pd.concat([df, df_stack])
                    print("- Get data time: %s sec -" % (round(time.time() - start_time)))
                    
    df.replace('N/A', np.nan, inplace = True)
    
    if to_excel == True:
        with pd.ExcelWriter('data.xlsx') as writer:
            df.to_excel(writer, index = False)
            
    if to_sql == True:
        check_start_time = time.time()
        unique_check_query = "SELECT DISTINCT CONCAT (data_update_date, region, domain, search_engine) as usl FROM %s ORDER BY usl desc" % data_table 
        old_load = pd.read_sql(unique_check_query, con = mysql_connection2())
        
        new_load = df
        new_load['usl'] = new_load['data_update_date'].astype(str) + new_load['region'] + new_load['domain'] + new_load['search_engine']
        new_load = new_load.sort_values(by= ['usl'])

        df = add_to_db(new_load = new_load, old_load = old_load)
       
        print("- Data duplication check: %s sec -" % (round(time.time() - check_start_time)))
        
        if (not df.empty):
            conn = mysql_connection()
            df.to_sql(con=conn, name=data_table, if_exists='append', index=False, chunksize=1000)
            conn.dispose()
        else:
            print('----No data to upload----')

    
main()