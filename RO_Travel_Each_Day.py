#!/usr/bin/env python
# coding: utf-8

# In[1]:


import datetime as dt
from datetime import datetime, timedelta
import numpy as np
import pandas as pd
import time as tm
from pymongo import MongoClient
import sqlalchemy
import warnings
import time
import gspread
import gspread_dataframe as gd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import requests
import json
import google.auth
import psycopg2
from functools import reduce
from sqlalchemy import Table, Column, Integer, String, MetaData, ForeignKey, create_engine, select, inspect, and_, or_
import psycopg2
import psycopg2.extensions
from sqlalchemy.sql import text
#from google.cloud import bigquery
from google.oauth2 import service_account
import gspread
import gspread_dataframe as gd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import requests
import json
import datetime as dt
import warnings
warnings.filterwarnings("ignore")
import time as tm
import numpy as np
import google.auth
galaxy=sqlalchemy.create_engine("postgresql+psycopg2://rathivarun:W443435345m9nZaIr9@redshift-cluster-2.ct93445kqx1dcuaa.ap-south-1.redshift.amazonaws.com:8493/datalake")
scope = ['https://spreadsheets.google.com/feeds',
        'https://www.googleapis.com/auth/drive']
credentials = ServiceAccountCredentials.from_json_keyfile_name('woven-amuletl-3434e341606-206365j980427121.json', scope)
gc = gspread.authorize(credentials)


# In[2]:


sh = gc.open_by_url('https://docs.google.com/spreadsheets/d/1yuY0cRYtoIhbjbEIN1-0WBiGLCHJKzgYO4Mb5QiXhgz13n9nnbjucY/edit#gid=0').worksheet('total_visits')

opr_visit = pd.read_sql('''select created_at, operator_id, sales_user, latitude, longitude, vehicle_number, renewal_done_on_visit, center 
    from ro_visit_table
    order by created_at asc''',galaxy)


# In[3]:


opr_visit = opr_visit[['sales_user', 'created_at', 'operator_id', 'latitude', 'longitude']]


# In[4]:


opr_visit = opr_visit[(opr_visit['created_at'].dt.year == 2022) & (opr_visit['created_at'].dt.month == 2)]


# In[5]:


opr_visit = opr_visit.sort_values(['sales_user', 'created_at'])


# In[6]:


opr_visit['date_part'] = opr_visit['created_at'].dt.date


# In[7]:


opr_visit = opr_visit.drop_duplicates()


# In[8]:


opr_visit['next_latitude'] = opr_visit.groupby(['sales_user', 'date_part'])['latitude'].shift(periods = -1)
opr_visit['next_longitude'] = opr_visit.groupby(['sales_user', 'date_part'])['longitude'].shift(periods = -1)


# In[9]:


opr_visit = opr_visit[['sales_user', 'created_at', 'date_part', 'latitude', 'longitude', 'next_latitude', 'next_longitude']]


# In[10]:


from haversine import haversine as hs

opr_visit['hd_oprwise'] = opr_visit.apply(lambda row : hs((row['latitude'], row['longitude']), (row['next_latitude'], row['next_longitude'])), axis = 1)


# In[11]:


hd_oprwise_sum_each_day_df = opr_visit.groupby(['sales_user', 'date_part'])['hd_oprwise'].sum().reset_index(name = 'hd_oprwise_sum_each_day')


# In[ ]:





# In[12]:


# To read data from GSheet
sh = gc.open_by_url('https://docs.google.com/spreadsheets/d/1aTtjivhwrbjhgywTeULXiYlNYX8Emjhnaeu14krOTCGKLMlRtGUZ3EXkYo/edit#gid=0').worksheet('RO_Home_Loc')
se_loc = pd.DataFrame(sh.get_all_records())


# In[13]:


first_visit_opr = opr_visit.groupby(['sales_user', 'date_part']).head(1)[['sales_user', 'created_at', 'date_part', 'latitude', 'longitude']]


# In[14]:


last_visit_opr = opr_visit.groupby(['sales_user', 'date_part']).tail(1)[['sales_user', 'created_at', 'date_part', 'latitude', 'longitude']]


# In[15]:


first_last_visit = first_visit_opr.append(last_visit_opr)


# In[16]:


first_last_visit = first_last_visit.sort_values(['sales_user', 'created_at'])


# In[17]:


first_last_visit = first_last_visit.drop_duplicates()


# In[18]:


se_first_last = se_loc.merge(first_last_visit, left_on = 'WE Code', right_on = 'sales_user')


# In[19]:


se_first_last['hs_first_last_opr_dist'] = se_first_last.apply(lambda row : hs((row['Home Lat'], row['Home Long']), (row['latitude'], row['longitude'])), axis = 1)


# In[20]:


add1 = se_first_last.groupby(['WE Code', 'date_part'])['hs_first_last_opr_dist'].sum().reset_index(name = 'hd_fisrtlastwise_sum_each_day')


# In[22]:


add2 = se_loc.merge(hd_oprwise_sum_each_day_df, how= 'left', left_on = 'WE Code', right_on = 'sales_user')[['WE Code', 'date_part', 'hd_oprwise_sum_each_day']]


# In[23]:


add2 = add2.rename(columns = {'hd_oprwise_sum_each_day' : 'hd_fisrtlastwise_sum_each_day'})


# In[24]:


add3 = add1.append(add2)


# In[25]:


final_df = add3.groupby(['WE Code', 'date_part'])['hd_fisrtlastwise_sum_each_day'].sum().reset_index(name = 'Distance_travelled_each_day(KM)')


# In[ ]:




