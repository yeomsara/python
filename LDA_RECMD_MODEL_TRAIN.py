#!/usr/bin/env python
# coding: utf-8

# loading libraries and configuration

# In[ ]:


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
from tqdm.contrib.concurrent import process_map  # or thread_map
from tqdm import tqdm

import random
from matplotlib import pyplot as plt
import warnings
import logging # This allows for seeing if the model converges. A log file is created.
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


# In[ ]:


## 현재 pid 확인
print("Hello world! Your pid is :", os.getpid())


# In[ ]:


pd.set_option('display.max_rows',100,'display.max_columns',100)
warnings.filterwarnings('ignore')


# ArgParse 대상날짜

# In[ ]:


model_dev_start_date = datetime.datetime.now()
model_dev_start_date = model_dev_start_date.strftime('%Y-%m-%d')
print("As of",model_dev_start_date, ", check model update schedule..")


# In[ ]:


## -----------------------------------------
# parse init args
## -----------------------------------------
parser = argparse.ArgumentParser(description = 'LDA Recommender System Dev Query')
parser.add_argument('--target_date', 
                    help = '초도 모델을 개발할 데이터의 날짜를 지정할 때 사용')
parser.add_argument('--trainig_days', 
                    help = '모델 개발 시, 몇 일 치 데이터를 업데이트할 것인지 지정')
parser.add_argument('--n_worker', 
                    help = '모델 개발 시, 몇 개의 multiprocesser core 를 사용할 것인지 지정')
parser.add_argument('--n_pass', 
                    help = '모델 개발 시, EM 알고리즘의 몇 번의 M-Step 을 거칠 것인지 지정')
parser.add_argument('--manual_update', 
                    help = 'y/n 으로 설정. 기 설정된 모델업데이트 로직을 무시하고 지정된 target_date 로 모델을 수행할 것인지 지정')
args = parser.parse_args()


try : 
    target_date = args.target_date
except : 
    target_date = None
    
try : 
    training_days = args.training_days
except: 
    training_days = None
    
try : 
    n_worker = args.n_worker
except : 
    n_worker = None
    
try : 
    n_pass = args.n_pass
except : 
    n_pass = None

try : 
    manual_update = args.manual_update
except : 
    manual_update = None


# Target-date setting     
if (target_date == None) | (target_date == 'None') :
    start_time  = datetime.datetime.now()
    target_date = (start_time - datetime.timedelta(days = 5)).strftime('%Y-%m-%d')
    # default 로 D-1 일 기준 4일 전 데이터를 활용하여 초도 모델 개발 세팅

# Training-days setting
if (training_days == None) | (training_days == 'None') :
    training_days = 3 # default 로 초도모델 개발 이후 3일치 데이터를 활용하여 모델 업데이트 세팅
else : 
    training_days = int(training_days)

# multiprocessing core setting    
if (n_worker == None) | (n_worker == 'None') :
    n_worker = 20 # defualt 로 20개의 multiprocess cpu 를 사용
else : 
    n_worker = int(n_worker)

# M-step iteration number setting
if (n_pass == None) | (n_pass == 'None') :
    n_pass = 30 # default 로 30 번의 M-step 을 거치도록 설정
else : 
    n_pass = int(n_pass)

# (21.01.26 updates) 스크립트에 내장된 model_update_interpreter 를 무시하고 target_date 기준으로 모델을 수행할 것인지 설정
if (manual_update == None) | (manual_update == 'None') : 
    manual_update = 'n' ## defualt 는 N 으로 설정
else : 
    manual_update = str(manual_update)

print("================================================")
print("============= Argument setting =================")
print("current_date : ",model_dev_start_date,
      "\ntarget_date : ",target_date,
      "\ntraining_days : ",training_days,
      "\nn_worker : ",n_worker,
      "\nn_pass : ",n_pass, 
      "\nmanual update scheduler : ", manual_update, sep="")
print("================================================")


# target_date = '2020-12-28'
target_date_6digits = datetime.datetime.strptime(target_date, '%Y-%m-%d').strftime('%y%m%d')


## -----------------------------------------
## 초도 모델 개발 이후 업데이트 기간 설정
## -----------------------------------------

start_date = datetime.datetime.strptime(target_date, '%Y-%m-%d') + timedelta(days = 1)
end_date = datetime.datetime.strptime(target_date, '%Y-%m-%d') + timedelta(days = training_days)
step = datetime.timedelta(days=1)

date_list = []
while start_date <= end_date:
    date_list.append(start_date.strftime('%Y-%m-%d'))
    start_date += step
    
## dictionary 형태로 변형
day_dict = []
for i in date_list :
    day_dict += [{'from':i, 'to':i}]

print("Scheduled model update period starts from to end :") 
display(day_dict)


# Connection Setting

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
    
    from sqlalchemy import inspect
    
    conn, engine = init_connection()
    inspector = inspect(engine)
    schemas = inspector.get_schema_names()
    
    conn.close()
    engine.dispose()
    print(schemas)


# Data Path 

# In[ ]:


path = cfg['Recommender']['MODEL_DIR_DEV'] 


# Logging 함수

def add_log(module_nm, module_type_nm, step, query_type, target_table, start_time) : 
    
    global em
    if type(start_time) is float :
        start_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    else: 
        start_time = start_time
    print(f'\n query_type : {query_type} \n target_table : {target_table.lower()} \n target_date : {target_date}')
    tbl_name = 'tb_amt_campaign_anl_log' 
    target_table = target_table.lower()
    
    st = start_time
    end_time = datetime.datetime.now()
    et = end_time.strftime('%Y-%m-%d %H:%M:%S')
    print(f"""\n start_time : {st} \n end_time : {et} \n""")
    print("=======================================================================================")
    
    try : 
        print(em)
        if em != None :
            error_code = 1
            error_state = em
        else :
            error_code = 0
            error_state = None
    except : 
        error_code = 0
        error_state = None
    
    res = [module_nm, module_type_nm, step, query_type, target_table, target_date, 
           st, et, error_code, error_state]
    res = pd.DataFrame(res, index = ['module','module_type','step','query_type','target_table','target_date',
                                     'start_time','end_time','error_code','error_state']).T

    conn, engine = init_connection()
    res.to_sql(tbl_name, schema = DB_ID.lower(), con = engine, index = False, if_exists = 'append',
               dtype = {'module': types.NVARCHAR(50),
                        'module_type': types.NVARCHAR(50),
                        'step': types.NVARCHAR(100),
                        'query_type': types.NVARCHAR(50),
                        'target_table': types.NVARCHAR(200),
                        'target_date': types.DATE,
                        'start_time': types.NVARCHAR(40),
                        'end_time': types.NVARCHAR(40),
                        'error_code': types.DECIMAL(2),
                        'error_state': types.NVARCHAR(200)
                       })
    conn.close()
    engine.dispose()

    start_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return (start_time)  
    
# ## Main Part 

# ### 함수 로딩

try : 

    start_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def worker(a) :
        key_list = list(flt_condition)
        a = "'"+a+"'" 

        if flt_type == 'default' :
            query_main = f""" SELECT * FROM  {db_name} WHERE {key_variable} = {a} """

        elif flt_type == 'cat' :
            query_main = f""" 
            SELECT * FROM {db_name} 
            WHERE ({key_list[0]} = {flt_condition[key_list[0]]} and {key_list[1]} = {flt_condition[key_list[1]]})     
                    AND {key} = {a} """

        else : 
            flt = ["'" + x + "'" for x in list(flt_condition.values())]
            query_main = f"""
            SELECT * FROM {db_name} 
            WHERE ({flt_var} >= {flt[0]} and {flt_var} <= {flt[1]})     
                    AND {key} = {a} """

        conn = DB_Connection()
        result = pd.read_sql(query_main, conn)
        conn.close()

        return result

    def Datachunk_range(key_variable) :
        key_list = list(flt_condition)

        if flt_type == 'default' :
            query_arg = f" SELECT distinct {key_variable} FROM {db_name} "

        elif flt_type == 'cat' :
            query_arg = f""" 
            SELECT distinct {key_variable} FROM {db_name} 
            WHERE ({key_list[0]} = {flt_condition[key_list[0]]} and {key_list[1]} = {flt_condition[key_list[1]]}) 
            """
        else : 
            flt = ["'" + x + "'" for x in list(flt_condition.values())]
            query_arg = f""" 
            SELECT distinct {key_variable} FROM {db_name} 
            WHERE ({flt_var} >= {flt[0]} and {flt_var} <= {flt[1]})     
            """
        conn = DB_Connection()
        arg_list = pd.read_sql(query_arg, conn)

        arg_list = arg_list.iloc[:,0].values.tolist()
        arg_list_flt = [arg_list[i] for i in range(len(arg_list)) if arg_list[i] != None] ## None 타입 제외

        conn.close()

        return arg_list_flt

    def multiprocesser() : 
        
        start_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        start_tt = time.time()

        arg_list = Datachunk_range(key_variable = key)

        if __name__ == '__main__' : 
            p = multiprocessing.Pool(processes = n_core)
            data = p.map(worker, arg_list) 
            p.terminate() ## Close a pipe which informs readers of that pipe
            p.join() ## Wait for a child process to be killed

            time.sleep(10)
            del p

        result = pd.concat(data)
        time.sleep(10)

        print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
              f" || {db_name} Data Loading is completed.",
              "(elapsed time :", np.round((time.time() - start_tt) / 60, 2), "mins)")

        return result   

    ## ---------------------------------------
    ## Data 정제 Function (일별 배치용)
    ## ---------------------------------------
    def data_preparation(df) : 
        start_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        start_tt = time.time()
        ratings = df[['CUST_ID','PRDT_DCODE_CD','DT_CNT','SUM_QTY']]

        ## TF Transformation
        tf, idf, tfidf = tf_idf_matrix(df)

        ## Product Mapping
        prdt_cd = list(df['PRDT_DCODE_CD'].unique())
        prdt_cd_df = prdt_map[prdt_map['PRDT_DCODE_CD'].isin(prdt_cd)].sort_values(by = 'PRDT_DCODE_CD', ascending = True)

        ## Corpus 생성
        corpus = tf.apply(row_to_tuple, mst = prdt_cd_df, axis = 1)

        print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
              " || Data Preprocessing is completed.",
              "(elapsed time :", np.round((time.time() - start_tt) / 60, 2), "mins)")

        return prdt_cd_df, tf, corpus

    def row_to_tuple(a, mst) : 
        a_flt = a[a > 0]
        a_list = a_flt.index.tolist()

        c = mst.PRDT_DCODE_CD 
        c = c[c.isin(a_list)].index.tolist()

        return list(zip(c,a_flt.tolist()))

    def tf_idf_matrix(df) :

        ## document length
        N = len(df['CUST_ID'].unique())

        ## TF Matrix 변환 
        tf = pd.pivot_table(df, values = 'DT_CNT', index = 'CUST_ID', columns = 'PRDT_DCODE_CD')
        tf = tf.fillna(0)

        ## IDF Matrix 계산 
        idf = df.groupby(['PRDT_DCODE_CD']).sum()['DT_CNT']
        idf = np.log((N+1)/(idf + 1)) + 1

        # TF-IDF Matrix 계산
        tfidf = tf.mul(idf)

        # L2 정규화 Term
        tfidf.iloc[:,:] = Normalizer(norm = 'l2').fit_transform(tfidf)

        print("          Total customer number on",target_date,"is :",N)
        print("          tf.shape :",tf.shape, 
              "idf.shape :",idf.shape, "tfidf.shape :", tfidf.shape)

        return tf, idf, tfidf    

    def file_search(file_dir, pattern) : 
        files = os.listdir(file_dir)
        p = re.compile(pattern) 

        f_list = []
        for f in files : 
            out = p.match(f)
            if out != None : f_list.append(f)
            else : pass

        f_list = np.sort(f_list) ## 오름차 순으로 Sorting

        return f_list


    # ### 모델 개발 일자 확인

    ## (20.12.25 update)
    ## 모델이 매월 첫째 주 주말 토요일이면 업데이트 진행되도록 로직 설정
    ## 현업과 사전 협의가 필요한 부분. 
    ## 1. 어느 서버를 사용할 것인지
    ## 2. 사전에 몇 일치 데이터가 적재되어 있어야 함

    ## (21.01.18 update)
    ## 모델 업데이트 주기가 확정되면 반영 예정

    def model_update_interpreter(model_dev_start_date) : 
        """
        계산로직
        ---------
        model_dev_start_date 를 입력 시, 
        첫째 주 주말 토요일의 week_no를 계산하고, model_dev_start_date 의 week_no 와 비교    

        returns
        -------
        Y, N 을 리턴
        """
        ## -----------------------------------
        weekday_seq = 5 ## 토요일로 가정
        ## -----------------------------------

        reformed_target_date = datetime.datetime.strptime(model_dev_start_date,'%Y-%m-%d')

        year = reformed_target_date.year
        month = reformed_target_date.month
        last_day_of_month = calendar.monthrange(year,month)[1]

        weekday_of_target_date = reformed_target_date.weekday() 
        week_of_target_date = reformed_target_date.strftime("%V")

        isoweek_list = []
        for d in range(1,last_day_of_month+1) :
            date_iter =  datetime.datetime(year,month,d)
            week_no = date_iter.strftime("%V") ## ISO-WEEK 추출
            if week_no not in isoweek_list :
                if date_iter.weekday() == weekday_seq : # 토요일 가정
                    isoweek_list += [week_no]
            else :
                d += 1

        isoweek_list = np.sort([int(i) for i in isoweek_list])

        if int(week_of_target_date) == isoweek_list[0] and weekday_of_target_date == weekday_seq :
            return_value = "Y"
        else :
            return_value = "N"

        return return_value

    ## ===================================================================
    ## 여기서 Y 면 if 문으로 모델 fitting & update 하는 로직 구현
    ## 아래 2가지 중 1개의 조건만 충족되면 모델 업데이트 시작 

    ## 1. 매월 첫 째주 토요일 (flag == 'Y')
    ## 2. 수기로 업데이트 진행 시 (manual_update == 'y') 
    ## ===================================================================

    flag = model_update_interpreter(model_dev_start_date)

    if (flag == 'Y') | (manual_update == 'y') :

        print(f">> model update is getting started.. ")

        # ### 상품 마스터 정제 & 저장
        ## ------------------------------------
        ## Setting for multiprocessing
        ## ------------------------------------

        db_name = 'CDS_DW.TB_DW_PRDT_DCODE_CD' ## target DB 
        n_core = 2

        ## multiprocessing 을 위한 arg list
        key = 'PRDT_CAT_CD'

        ## filter variable
        flt_type = 'cat'
        flt_condition = {'AFLCO_CD':'001','BIZTP_CD':'10'}

        prdt_mst = multiprocesser() # multiprocessing.cpu_count()


        ## ---------------------------------------
        ## 상품 단어사전 (id2word) 만들기
        ## ---------------------------------------
        prdt_map = prdt_mst[['PRDT_DCODE_CD','PRDT_DCODE_NM']]
        prdt_map['row_id'] = range(len(prdt_map))
        prdt_map = prdt_map.set_index('row_id')

        id2word = prdt_map[['PRDT_DCODE_NM']].to_dict()['PRDT_DCODE_NM']

        ## ---------------------------------------
        ## prdt_map, id2word 저장 
        ## ---------------------------------------
        master_file_nm = f'product_master_dict_{target_date_6digits}.pkl'
        a_file = open(path + master_file_nm, "wb")
        pickle.dump(id2word, a_file)
        a_file.close()

        prdt_map.to_csv(path + f'prdt_mst_{target_date_6digits}.csv')


        ## ---------------------------------------
        ## id2word 불러오기
        ## ---------------------------------------
        id2word_list = file_search(file_dir = path, pattern = '^product_master_dict_[0-9]+[\.]pkl$')
        id2word_target_nm = id2word_list[-1]
        a_file = open(path + id2word_target_nm, "rb")
        id2word = pickle.load(a_file)

        print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
              " || Chosen the latest id2word is:",id2word_target_nm)

        ## ---------------------------------------
        ## 상품 마스터 불러오기 (product master)
        ## ---------------------------------------
        prdt_mst_list = file_search(file_dir = path, pattern = "^prdt_mst_[0-9]+[\.]csv$")
        prdt_mst_target_nm = prdt_mst_list[-1] # Max 값으로 지정. 가장 최신 일자 모델을 끌고 옴
        prdt_map = pd.read_csv(path + prdt_mst_target_nm)

        print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
              " || Chosen the latest product master is:",prdt_mst_target_nm)


        # ### 데이터 전처리
        ## --------------------------------------------------------
        ## multiprocessing 준비

        db_name = 'CDS_AMT.TB_AMT_RECMD_TMPR' ## target DB 
        n_core = n_worker # 10

        ## data chuck arg 를 위한 기준 변수
        key = 'PRDT_CAT_CD'

        ## filter date
        flt_type = 'date'
        flt_var ='ANL_DT'
        ## --------------------------------------------------------

        flt_condition = {'from':target_date,'to':target_date} ## 일별 배치를 가정
        df_raw = multiprocesser() 

        start_time = add_log(module_nm = '상품추천', module_type_nm = '개발', 
                             step = '1.pre-processing', query_type = 'Pre-processing', 
                             target_table = 'data loading', start_time = start_time)


        df_flt = df_raw[['CUST_ID','ANL_DT','PRDT_DCODE_CD','DT_CNT','SUM_QTY']]
        prdt_df, tf, corpus = data_preparation(df_flt)

        start_time = add_log(module_nm = '상품추천', module_type_nm = '개발', 
                             step = '1.pre-processing', query_type = 'Pre-processing', 
                             target_table = 'data transformation (input > corpus)', start_time = start_time)


        # ### 초도 모델 Fitting

        ## LOG PATH 는 CONFIG.INI 에서 별도로 세팅
        log_path = cfg['Recommender']['DEV_LOG'] 

        log_file_nm = 'lda_model_creation_from_' + target_date_6digits + '.log'
        logging.basicConfig(filename = log_path + log_file_nm, format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)



        ## ------------------------------------------------
        ## 모델 초도 개발 함수 
        ## ArgParse 에서 설정한 n_worker, n_pass 적용
        ## ------------------------------------------------
        lda_model_init = gensim.models.ldamulticore.LdaMulticore(workers = n_worker, # 40 
                                                                 random_state = 1243,
                                                                 corpus = corpus, id2word = id2word, 
                                                                 num_topics = 100,
                                                                 chunksize = 5000, eval_every = 1, 
                                                                 passes = n_pass, # 30 
                                                                 per_word_topics = True) ## 100, 30 

        start_time = add_log(module_nm = '상품추천', module_type_nm = '개발', 
                             step = '2.초도 모델 학습', query_type = 'Fitting', 
                             target_table = 'init model fitting', start_time = start_time)


        ## 초도 개발 Model 저장
        lda_init_nm = 'lda_model_' + target_date_6digits + '.model'
        lda_model_init.save(path + lda_init_nm)

        cm_init = CoherenceModel(model = lda_model_init, corpus = corpus, coherence = 'u_mass')   
        u_mass_score = cm_init.get_coherence() 
        print(u_mass_score)

        # ## Corpus 저장
        # corpus_file_nm = 'corpus_' + target_date_6digits + '.pkl'
        # corpus_file = open(path + corpus_file_nm, "wb")
        # pickle.dump(corpus, corpus_file)
        # corpus_file.close()


        # ### 모델 업데이트 

        ## 기간 설정

        start_date = datetime.datetime.strptime(target_date, '%Y-%m-%d') + timedelta(days = 1)
        end_date = datetime.datetime.strptime(target_date, '%Y-%m-%d') + timedelta(days = training_days)
        step = datetime.timedelta(days=1)

        date_list = []
        while start_date <= end_date:
            date_list.append(start_date.strftime('%Y-%m-%d'))
            start_date += step

        ## dictionary 형태로 변형
        day_dict = []
        for i in date_list :
            day_dict += [{'from':i, 'to':i}]

        display(day_dict)


        ## 모델 업데이트 for loop 진행
        model_list = []
        cm_list = []
        cm_list.append([lda_init_nm,u_mass_score])

        for k,j in enumerate(day_dict) : 

            start_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print(j)
            """
            Step 1.
            시간 필터링을 통하여 df 를 말아 올림
            """
            flt_condition = j # 시간 filtering condition 
            df = multiprocesser()

            date_value = list(j.values())[0]
            start_time = add_log(module_nm = '상품추천', module_type_nm = '개발', 
                             step = '3.모델 업데이트', query_type = 'Update', 
                             target_table = f'{k+1}th iterative process : data loading on {date_value}', start_time = start_time)

            """
            Step 2. 
            데이터 정제하여 각종 input 생성
                - 모델 업데이트 사용 : corpus 리스트
                - 집계에 사용 : tf 정보
                - 상품추천 정제에 사용 : prdt_df
            """
            prdt_df, tf, corpus = data_preparation(df)  

            start_time = add_log(module_nm = '상품추천', module_type_nm = '개발', 
                             step = '3.모델 업데이트', query_type = 'Update', 
                             target_table = f'{k+1}th iterative process : data transformation to corpus on {date_value}', start_time = start_time)

            """
            Step 3.
            매번 최신 모델을 가져와서 갱신하고, Incremental update 를 시행
             >>> 기 개발된 모델 50%, 신규 데이터 50% 의 비율로 업데이트 진행
            target_date 를 기점으로 일별로 업데이트하여 해당 날짜로 저장
            """
            start = time.time()
            dt = df.ANL_DT.unique()[0].strftime('%y%m%d')
            print(dt)

        #     ## Corpus 저장
        #     corpus_file_nm = 'corpus_' + dt + '.pkl'
        #     corpus_file = open(path + corpus_file_nm, "wb")
        #     pickle.dump(corpus, corpus_file)
        #     corpus_file.close()

            ## 모델 로딩
            m_list = file_search(file_dir = path, pattern = "^lda_model_[0-9]+[\.]model$")
            model_target_nm = m_list[-1] # Max 값으로 지정. 가장 최신 일자 모델을 끌고 옴
            lda_model = LdaModel.load(path + model_target_nm) 
            print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
              " || Chosen the latest model is:",model_target_nm)

            ## configuration setting ----------------------------------
            logging.basicConfig(filename = log_path + log_file_nm, format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)
            # lda_model.chunksize = 5000
            # lda_model.workers = 40
            # lda_model.passes = 30
            ## --------------------------------------------------------

            lda_model.update(corpus)
            model_list.append(lda_model) ## model container 추가

            start_time = add_log(module_nm = '상품추천', module_type_nm = '개발', 
                             step = '3.모델 업데이트', query_type = 'Update', 
                             target_table = f'{k+1}th iterative process : model update on {date_value}', start_time = start_time)
            print("calculation time for model update is :", np.round((time.time() - start)/60, 2),"mins")

            # 모델 저장
            start = time.time()
            save_nm = "lda_model_" + dt + ".model"
            lda_model.save(path + save_nm) 

            # CM score 확인 
            cm_iter = CoherenceModel(model = lda_model, corpus = corpus, coherence = 'u_mass')   
            u_mass_score = cm_iter.get_coherence() 
            print("Coherence Score is :",u_mass_score)
            cm_list.append([save_nm, u_mass_score])

            print(f"calculation time for loop in dt={dt} is :", np.round((time.time() - start)/60, 2),"mins")
            print('\n','-----------------------------------------------------------------','\n')



        # ### 최신 파일만 운영 directory 로 import 

        ## 최신 id2word 확인
        id2word_list = file_search(file_dir = path, pattern = '^product_master_dict_[0-9]+[\.]pkl$')
        id2word_target_nm = id2word_list[-1]

        ## 최신 마스터 확인 (product master)
        prdt_mst_list = file_search(file_dir = path, pattern = "^prdt_mst_[0-9]+[\.]csv$")
        prdt_mst_target_nm = prdt_mst_list[-1] # Max 값으로 지정. 가장 최신 일자 모델을 끌고 옴


        f_list = [id2word_target_nm] + [prdt_mst_target_nm]


        ## 최신 LDA 모델 확인 
        ftype_list = ['model','model.expElogbeta.npy','model.id2word','model.state']

        model_target_ftype_list = []
        for ftype in ftype_list : 
            m_list =  file_search(file_dir = path, pattern = f"^lda_model_[0-9]+[\.]{ftype}$")
            m_target_file =  m_list[-1]
            model_target_ftype_list += [m_target_file] 


        f_list += model_target_ftype_list

        print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
              "|| These model files are copied and moved into opt location :\n\n",'\n'.join(f_list),'\n', sep = '')


        ## --------------------------------------------
        ## 이관 디렉토리 설정 

        # path = cfg['Recommender']['MODEL_DIR_DEV']  # from 
        path2 = cfg['Recommender']['MODEL_DIR'] # to
        ## --------------------------------------------

        for f in f_list : 
            shutil.copy2(path + f, path2 + f)

        start_time = add_log(module_nm = '상품추천', module_type_nm = '개발', 
                             step = '4.운영 디렉토리 이관', query_type = 'Copy', 
                             target_table = 'model files are copied into oprtn directory', start_time = start_time)


    else : 

        print(f">> today is not the assigned date.. pass the model updates !")
        em = 'please arguments setting manual_update n --> y'
        start_time = add_log(module_nm = '상품추천', module_type_nm = '개발', 
                             step = '업데이트 일자 확인', query_type = 'terminate', 
                             target_table = 'pass the update process : not the arranged schedule', start_time = start_time)


except Exception as e :
    
    ## 배치 중간 에러 발생 시 
    ## -- 1. 로그 테이블에 error state 및 메시지 적재 후 종료
    
    print(e)
    em = str(e)
    
    start_time = add_log(module_nm = '상품추천', module_type_nm = '개발', 
                     step = 'error', query_type = 'error', 
                     target_table = '==== error occured ====', start_time = start_time)

        
        
# ## 결과 확인
# ## Path 내에서 가장 최신 모델을 가져옴

# model_list = file_search(file_dir = path, pattern = "^lda_model_[0-9]+[\.]model$")
# model_target_nm = model_list[-1] # Max 값으로 지정. 가장 최신 일자 모델을 끌고 옴
# lda_model_iter = LdaModel.load(path + model_target_nm) 

# print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
#       " || Chosen the latest model is:",model_target_nm)# lda_model_iter.print_topics(num_topics =100, num_words = 30)# cm_list = []

# corpus_nm_list =  file_search(file_dir = path, pattern = "^corpus_[0-9]+[\.]pkl$")
# for c in corpus_nm_list :
#     a_file = open(path + c, "rb")
#     corpus_iter = pickle.load(a_file)
    
#     model_nm_list =  file_search(file_dir = path, pattern = "^lda_model_[0-9]+[\.]model$")
#     for m in model_nm_list : 
#         lda_model_iter = LdaModel.load(path + m) 
        
#         cm_iter = CoherenceModel(model = lda_model_iter, corpus = corpus_iter, coherence = 'u_mass')   
#         u_mass_score = cm_iter.get_coherence() 
#         print([c,m, u_mass_score])
#         cm_list.append([c,m, u_mass_score])# ## reference : https://radimrehurek.com/gensim/auto_examples/howtos/run_compare_lda.html

# def plot_difference_plotly(mdiff, title="", annotation=None):
#     """Plot the difference between models.

#     Uses plotly as the backend."""
#     import plotly.graph_objs as go
#     import plotly.offline as py

#     annotation_html = None
#     if annotation is not None:
#         annotation_html = [
#             [
#                 "+++ {}<br>--- {}".format(", ".join(int_tokens), ", ".join(diff_tokens))
#                 for (int_tokens, diff_tokens) in row
#             ]
#             for row in annotation
#         ]

#     data = go.Heatmap(z=mdiff, colorscale='RdBu', text=annotation_html)
#     layout = go.Layout(width=950, height=950, title=title, xaxis=dict(title="topic"), yaxis=dict(title="topic"))
#     py.iplot(dict(data=[data], layout=layout))

# def plot_difference_matplotlib(mdiff, title="", annotation=None):
#     """Helper function to plot difference between models.

#     Uses matplotlib as the backend."""
#     import matplotlib.pyplot as plt
#     fig, ax = plt.subplots(figsize=(18, 14))
#     data = ax.imshow(mdiff, cmap='RdBu_r', origin='lower')
#     plt.title(title)
#     plt.colorbar(data)

# try:
#     get_ipython()
#     import plotly.offline as py
# except Exception:
#     #
#     # Fall back to matplotlib if we're not in a notebook, or if plotly is
#     # unavailable for whatever reason.
#     #
#     plot_difference = plot_difference_matplotlib
# else:
#     py.init_notebook_mode()
#     plot_difference = plot_difference_plotly# ## 기 개발된 model 의 토픽 간 상관성 비교 (m2)

# m2 = lda_model_iter
# mdiff, annotation = m2.diff(m2, distance='jaccard', num_words=50)
# plot_difference(mdiff, title="Topic difference (one model) [jaccard distance]", annotation=annotation)