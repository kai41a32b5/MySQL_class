#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Mar 19 15:48:13 2021

@author: k.liu
"""
import pandas as pd 
from sqlalchemy import create_engine, Table, Column, Text, Integer, String, MetaData, DateTime, Boolean
from sqlalchemy.dialects.mysql import insert
import json
import os 
from datetime import datetime as dt

def main():
    
    sql = MySql('remixed', 'jour2048', '140.112.153.55')
    folder = '/Volumes/Data/youtube_data/covid'
    sql.insert_folder(folder)
    folder = '/Volumes/Data/youtube_data/political'
    sql.insert_folder(folder)
    folder = '/Volumes/Data/youtube_data/covid_taipei'
    sql.insert_folder(folder)    
    sql.close()
        

class MySql:
    

    def __init__(self, username, password, ip, database='youtube_data_new'):
    
        path = 'mysql+pymysql://{}:{}@{}/{}'.format(username, password, ip, database)
        self.engine = create_engine(path)
        self.con = self.engine.connect()
        
        self.metadata = MetaData()
        # self.users = Table('users', self.metadata,
        #                    Column('authorId', String(200), primary_key=True),
        #                    Column('authorDisplayName', Text),
        #                    Column('commentCount', Integer))
        
        self.videos = Table('videos', self.metadata,
                       Column('channelId', String(200)),
                       Column('channelTitle', Text),
                       Column('title', Text),
                       Column('publishedAt', DateTime),
                       Column('description', Text),
                       Column('videoId', String(200), primary_key=True),
                       Column('likeCount', Integer),
                       Column('dislikeCount', Integer),
                       Column('commentCount', Integer),
                       Column('viewCount', Integer),
                       Column('etag', String(200)))
        
        self.chats = Table('chats', self.metadata,
                      Column('videoId', String(200)),
                      Column('chatId', String(200)),
                      Column('authorDisplayName', Text),
                      Column('textOriginal', Text),
                      Column('publishedAt', DateTime),
                      Column('elapsedTime', Text),
                      Column('authorId', String(200)))
        
        self.comments = Table('comments', self.metadata,
                         Column('videoId', String(200)),
                         Column('commentId', String(200), primary_key=True),
                         Column('parentId', String(200)),
                         Column('authorDisplayName', Text),
                         Column('authorId', String(200)),
                         Column('textOriginal', Text),
                         Column('likeCount', Integer),
                         Column('publishedAt', DateTime),
                         Column('topComment', Boolean),
                         Column('etag', String(200)))
        
    
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
    

    def select_table(self, table, columns=['*'], where=None, group_by = None, using=None, fetchall=True, n=100):

        try: group_by_str = ','.join(group_by)
        except:group_by_str = None
        col_string = ','.join(columns) 
        query = 'SELECT ' + col_string + ' FROM ' + table 
        
        for i, j in zip([' WHERE ', ' GROUP BY ', ' USING '],[where, group_by_str, using]):
            if j != None:
                query = query + i + j

        if not fetchall:
            query = query + ' LIMIT ' + str(n)
        result =  self.execute(query + ' ;', fetchall=fetchall, n=n)
        return pd.DataFrame([dict(i) for i in result])

    def inner_join(self, columns, table_l, table_r, on, fetchall=True, n=100):

        on_str = table_l+'.'+on+' = '+table_r+'.'+on
        query = 'SELECT ' + ','.join(columns) + ' FROM '+ table_l + ' INNER JOIN ' + table_r + ' ON ' + on_str     
        result = self.execute(query + ' ;', fetchall=fetchall, n=n)
        return pd.DataFrame([dict(i) for i in result])
    
    def left_join(self, columns, table_l, table_r, on, fetchall=True, n=100):

        on_str = table_l+'.'+on+' = '+table_r+'.'+on
        query = 'SELECT ' + ','.join(columns) + ' FROM '+ table_l + ' LEFT JOIN ' + table_r + ' on ' + on_str
        result = self.execute(query + ' ;', fetchall=fetchall, n=n)
        return pd.DataFrame([dict(i) for i in result])
        
    def right_join(self, columns, table_l, table_r, on, fetchall=True, n=100):

        on_str = table_l+'.'+on+' = '+table_r+'.'+on
        query = 'SELECT ' + ','.join(columns) + ' FROM '+ table_l + ' RIGHT JOIN ' + table_r + ' USING ' + on_str
        result = self.execute(query + ' ;', fetchall=fetchall, n=n)
        return pd.DataFrame([dict(i) for i in result])
    
    #def insert_json():
        
    
    def insert(self, table, df):
        
        self.metadata.create_all(self.engine)    
        tab = {'chats': self.chats, 'videos':self.videos, 'comments': self.comments}
        ins = tab[table].insert()
        self.con.execute(ins, df.to_dict('records'))


    def upsert(self, table, df):
        
        self.metadata.create_all(self.engine)
        tab = {'chats': self.chats, 'videos':self.videos, 'comments': self.comments}
        for i in df.to_dict('records'):
            ins = insert(tab[table]).values(i).on_duplicate_key_update(i)
            self.con.execute(ins)


    def drop_table(self, *args):
        for i in args:
            self.execute('drop table '+ i +' ;')
        return self.table_list()


    def insert_folder(self, folder):
        
        def get_path(folder):
            path_list = []
            prefix = folder+'/{}'
            listdir = os.listdir(folder)
            channels = []
        
            for i in listdir:
                if len(i.split('.')) == 1:
                    channels.append(i)
            #print(channels)
            for path in [prefix.format(i) for i in channels]:
                folders = os.listdir(path)
        
                try:
                    folders.remove('.DS_Store')
                except:pass
                for file in [path+'/{}'.format(i) for i in folders]:
                    if file[-4:] == 'json':
                        path_list.append(file)
            return path_list
        
        def parse(json_path):
            #video table
            with open(json_path, 'rb') as f:
                data = json.load(f)
            item_keys = ['id', 'etag']
            info_snippet_keys = ['publishedAt', 'channelId', 'title', 'description', 'channelTitle']
            stat_keys = ['viewCount', 'likeCount', 'dislikeCount', 'commentCount']
            comment_snippet_keys = ['videoId', 'textOriginal', 'authorDisplayName', 'likeCount', 'publishedAt']
            comment_keys = ['commentId', 'parentId', 'etag'] + comment_snippet_keys + ['authorId', 'topComment']
            video_keys = ['videoId', 'etag'] + info_snippet_keys + stat_keys
            chat_author_key = ['channelId', 'name']
            chat_item_keys = ['id', 'elapsedTime', 'datetime', 'message']
            chat_keys = ['videoId', 'authorId', 'authorDisplayName', 'chatId', 'elapsedTime', 'publishedAt', 'textOriginal']
            
            item = data['info']['items'][0]
            snippet = item['snippet']
            stat = item['statistics']
            item_values = [item[i] for i in item_keys]
            snippet_values = [snippet[i] for i in info_snippet_keys]
            stat_values = []
            for key in stat_keys:
                try: stat_values.append(stat[key])
                except: stat_values.append(None)
                #stat_values = [stat[i] for i in stat_keys]
            all_values = item_values + snippet_values + stat_values
            videos = {i:[j] for i, j in zip(video_keys, all_values)}
            comments = {i:[] for i in comment_keys}
            chats = {i:[] for i in chat_keys}
            ld = lambda i,j,d:  d[i].append(j)
            for comment in data['comment']:
                commentId = [comment['snippet']['topLevelComment']['id']]
                parentId = [comment['id']]
                etag = [comment['etag']]
                top = [comment['snippet']['topLevelComment']['snippet'][i] for i in comment_snippet_keys]
                try: authorId = [comment['snippet']['topLevelComment']['snippet']['authorChannelId']['value']]
                except: authorId = [None]
                all_values = commentId + parentId + etag + top + authorId + [True]
                for i,j in zip(comment_keys, all_values):
                    ld(i,j,comments)
                    
                if 'replies' in comment.keys():
                    for reply in comment['replies']['comments']:
                        commentId = [reply['id']]
                        etag = [reply['etag']]
                        reply_ = [reply['snippet'][i] for i in comment_snippet_keys]
                        authorId = [reply['snippet']['authorChannelId']['value']]
                        all_values = commentId + parentId + etag + reply_ + authorId+ [False]
                        for i,j in zip(comment_keys, all_values):
                            ld(i,j,comments)
        
            if 'livechat' in data.keys():
                videoId = [data['info']['items'][0]['id']]
                for chat in data['livechat']:
                    author = [chat['author'][i] for i in chat_author_key]
                    items =  [chat[i] for i in chat_item_keys]
                    all_values = videoId + author + items
                    for i,j in zip(chat_keys, all_values):
                            ld(i,j,chats)
                    
                
            return videos, comments, chats
        
        chats = {}
        comments = {}
        videos = {}
        for data in get_path(folder):
            try:
                video, comment, chat = parse(data)
                
                if videos == {}:
                    videos = video
                else:
                    for key in videos.keys():
                        videos[key] = videos[key] + video[key]
                if comments == {}:
                    comments = comment
                else:
                    for key in comments.keys():
                        comments[key] = comments[key] + comment[key]
                if chats == {}:
                    chats = chat
                else:
                    for key in chats.keys():
                        chats[key] = chats[key] + chat[key]
            except: pass  
        
        video_df = pd.DataFrame(videos)
        comment_df = pd.DataFrame(comments)
        chat_df = pd.DataFrame(chats)
        try:video_df['publishedAt'] = pd.to_datetime(video_df['publishedAt']).dt.tz_convert(None)
        except: pass
        try:comment_df['publishedAt'] = pd.to_datetime(comment_df['publishedAt']).dt.tz_convert(None)
        except:pass
        try:chat_df['publishedAt'] = pd.to_datetime(chat_df['publishedAt']).dt.tz_convert(None)
        except: pass
        self.upsert('videos', video_df)
        self.upsert('comments', comment_df)
        if chat_df.empty == False:
            self.upsert('chats', chat_df)

   
    def close(self):
        self.con.close()

if __name__ == '__main__':
    main()