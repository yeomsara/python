# Loading libraries

## 함수 로딩  
import pandas as pd
import numpy as np

import time 
import datetime
from datetime import timedelta
import calendar
import argparse

import multiprocessing
from functools import partial
from tqdm.contrib.concurrent import process_map  
from tqdm import tqdm

import random
from matplotlib import pyplot as plt
import warnings
import logging 
import pickle
import subprocess
import numexpr as ne
import psutil
import shutil

## 전처리 모듈
from sklearn.preprocessing import Normalizer
from itertools import chain

## Gensim 모듈
import gensim
import gensim.corpora as corpora
from gensim.utils import simple_preprocess
from gensim.models import CoherenceModel
from gensim.models import LdaModel
from gensim.models.coherencemodel import CoherenceModel

## SQL & Connection 모듈
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy import types
from sqlalchemy import inspect
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Numeric
from hdbcli import dbapi
import configparser 

## 후처리 모듈
import re
import os
from functools import reduce



conf_dir = '/home/cdsadmin/AMT/src/conf/config.ini'
cfg = configparser.ConfigParser(interpolation=configparser.ExtendedInterpolation())
cfg.read(conf_dir)

global HOST,PORT,DB_ID,DB_PW

HOST = cfg['dbconnect']['host']
PORT = int(cfg['dbconnect']['port'])
DB_ID = cfg['dbconnect']['ID']
DB_PW = cfg['dbconnect']['PW']


get_ipython().run_line_magic('load_ext', 'sql')
get_ipython().run_line_magic('sql', 'hana+hdbcli://{DB_ID}:{DB_PW}@{HOST}:{PORT}')


def DB_Connection(HOST = HOST, PORT = PORT, DB_ID = DB_ID, DB_PW = DB_PW) :
    conn=dbapi.connect(HOST,PORT,DB_ID,DB_PW)
    return conn

def init_connection() :
    engine = create_engine(f'hana+hdbcli://{DB_ID}:{DB_PW}@{HOST}:{PORT}/', echo = False)
    conn = engine.connect()
    
    return (conn, engine)    

def schema_inspector() :
    conn, engine = init_connection()
    inspector = inspect(engine)
    schemas = inspector.get_schema_names()
    tables = inspector.get_table_names(schema = DB_ID)
    
    conn.close()
    engine.dispose()
    
    return schemas, tables 


# db_schema_list, db_table_list = schema_inspector()
tmp_table_list = ['RECMD_LOGIC1_BASKET_AGG','TB_AMT_RECMD_LOGIC1_PROBWT_RESULT','CUST_MICROSEG',
                  'CUST_RECMD_MICROSEG_BASE_RAW','CUST_RECMD_MICROSEG_BASKET','TB_AMT_RECMD_LOGIC2_MICROSEG_RESULT',
                  'RECMD_RANK_TMP','RECMD_RANK_DAILY','RECMD_RANK_DAILY_EXCEPTION']

for i in tmp_table_list:
    tbl = i.lower()
    if tbl in db_table_list :
        get_ipython().run_cell_magic('sql', '', f"""
            DROP TABLE {DB_ID}.{tbl.upper()} ;
            """)
        print('%s.%s'%(DB_ID,tbl.upper()))
