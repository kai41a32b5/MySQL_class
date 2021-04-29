#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Mar 19 15:48:13 2021

@author: k.liu
"""
import pandas as pd 
from sqlalchemy import create_engine
import json
import os 
#%%
class MySql:
    

    def __init__(self, username, password, ip, database='youtube_data'):
    
        path = 'mysql+pymysql://{}:{}@{}/{}'.format(username, password, ip, database)
        self.engine = create_engine(path)
        self.con = self.engine.connect()
    
    
    def execute(self, query, fetchall=True, n=10):
        result = self.con.execute(query)
        if fetchall:
            try:
                return result.fetchall()
            except:
                return result
        else:
            return self.con.execute(query).fetchmany(n)


    def table_list(self):
        return self.engine.table_names()
    
        
    def table_info(self, table):
        result = self.execute('DESCRIBE '+table+' ;')
        return pd.DataFrame([dict(i) for i in result])
    
    
    def table_length(self, *args):
        dic = {}
        for table in args:
            result= self.execute('SELECT COUNT(*) FROM '+ table +' ;')
            dic[table] = result[0][0]
        return dic
    
    
    def select_table(self, table, columns='*', where=None, fetchall=True, n=100):
        if type(columns) == list:
            col_string = ','.join(columns) 
        else:
            col_string = columns
        query = 'SELECT ' + col_string + ' FROM ' + table 
        if where != None:
            query = query + ' WHERE ' + where
        else: pass   
        if not fetchall:
            query = query + ' LIMIT ' + str(n)
        return self.execute(query + ' ;', fetchall=fetchall, n=n)
                
        
    def to_pandas(self, table, columns='*', where=None, fetchall=True, n=100):
        query = self.select_table(table, columns, where, fetchall, n)
        return pd.DataFrame([dict(i) for i in query])


    def insert_data(self, table, df):
        #t = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
        #df['insertTime'] = t  
        df.to_sql(table, con=self.engine, if_exists='append', chunksize=1000)


    def update_data(self, table, column, df):
        column_str = ','.join(column)
        self.execute('UPDATE '+ table + 
                     ' SET '+ column_str+
                     ' VALUE ')
        
        
    def drop_table(self, table):
        self.execute('drop table '+table +' ;')
        return self.table_list()


        return self.select_table('comments', where='userId = '+userId)
    def insert_folder(self, folder):
        
        def load_data(folder):
            data_list = []
            prefix = folder+'/{}'
            channels = os.listdir(folder)
            channels.remove('.DS_Store')
            for path in [prefix.format(i) for i in channels]:
                folders = os.listdir(path)
                folders.remove('.DS_Store')
                for p in [path+'/{}'.format(i) for i in folders]:
                    os.chdir(p)
                    files = os.listdir(p)
                    for f in files:
                        try:
                            with open(f, 'r') as file:
                                data_list.append(json.load(file))
                        except:
                            pass
            return data_list


        def list_to_df(data_list):
            info_dic = {i:[] for i in data_list[0]['info'][0].keys()}
            comment_dic = {i:[] for i in data_list[0]['comment'][0].keys()}
            l = list(comment_dic.keys())
            l[1] = 'replyId'
            reply_dic = {i:[] for i in l}
            for i in data_list:
                for key in info_dic.keys():
                    info_dic[key].append(i['info'][0][key])
                for c in i['comment']: 
                    for key in comment_dic.keys():
                        comment_dic[key].append(c[key])
                for r in i['reply']:
                    for key in reply_dic.keys():
                        reply_dic[key].append(r[key])
            info_df = pd.DataFrame(info_dic)
            info_df['publishedAt'] = pd.to_datetime(info_df['publishedAt'])
            comment_df = pd.DataFrame(comment_dic)
            comment_df['topComment'] = 1
            reply_df = pd.DataFrame(reply_dic).rename(columns = {'replyId': 'commentID'})
            reply_df['topComment'] = 0
            comment_df = comment_df.append(reply_df)
            comment_df = comment_df.drop(columns='commentID').rename(columns={'publishAt':'publishedAt'})
            comment_df['publishedAt'] = pd.to_datetime(comment_df['publishedAt'])
            
            return info_df, comment_df
        
        video_df, comment_df = list_to_df(load_data(folder))
        self.insert_data('videos', video_df)
        self.insert_data('comments', comment_df)


    def close(self):
        self.con.close()
        

#%%
sql = MySql('remixed', 'jour2048', '140.112.153.55')
#%%
import pickle
os.chdir('/Users/k.liu/Documents/Python/py_sql')
#%%
with open('old_comments.pickle', 'rb') as file:
    comments = pickle.load(file)
with open('old_videos.pickle', 'rb') as file:
    videos = pickle.load(file)
sql.insert_data('comments', comments)
sql.insert_data('videos', videos)
folder = '/Users/k.liu/Documents/Python/yt_covid'
sql.insert_folder(folder)
folder = '/Users/k.liu/Documents/Python/yt_political'
sql.insert_folder(folder)
#%%
comments.columns
