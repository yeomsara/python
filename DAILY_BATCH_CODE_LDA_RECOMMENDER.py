#!/usr/bin/env python

######################################################
# ### Setting and minor configurations
######################################################

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
# from matplotlib import pyplot as plt
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


## 현재 pid 확인
print("Hello world! Your pid is :", os.getpid())

## python configuration setting
pd.set_option('display.max_rows',100,'display.max_columns',100)
warnings.filterwarnings('ignore')

######################################################
# ArgParser : parse init args
######################################################

parser = argparse.ArgumentParser(description = 'LDA Recommender System Daily Batch Query')
parser.add_argument('--target_date', 
                    help = 'R1: 적재할 데이터의 날짜를 지정할 때 사용')
parser.add_argument('--topic_top_n', 
                    help = 'R2: Logic1 확률곱 lift 계산의 대상이 되는 Top N 토픽 수 결정')
parser.add_argument('--prdt_flt_seg_count', 
                    help = 'R3: Logic2 Micro-seg 확률곱 lift 대상 상품 필터링을 위한 최소 seg 사이즈, Q4 = 50으로 설정')
parser.add_argument('--prdt_flt_purchase_count', 
                    help = 'R4: R3에 해당하는 Micro-seg 의 추천대상 상품 필터링을 위한 최소 구매 건수')
args = parser.parse_args()


try : 
    target_date = args.target_date
except : 
    target_date = None
    
try : 
    topN = args.topic_top_n
except : 
    topN = None
    
try : 
    PRDT_DCODE_MINIMUM_SEG_COUNT = args.prdt_flt_seg_count
except :
    PRDT_DCODE_MINIMUM_SEG_COUNT = None
    
try : 
    PRDT_DCODE_MINIMUM_PURCHASE_COUNT = args.prdt_flt_purchase_count
except : 
    PRDT_DCODE_MINIMUM_PURCHASE_COUNT = None

## target date setting 
if (target_date == None) | (target_date == 'None') :
    start_time  = datetime.datetime.now()
    target_date = (start_time - datetime.timedelta(days = 1)).strftime('%Y-%m-%d')

## logic 1 확률곱 lift 의 상품 필터링용 토픽 Top N 설정 
if (topN == None) | (topN == 'None') :
    topN = 10
else : 
    topN = int(topN)

## logic 2 basket lift 의 상품 필터링용 Micro-seg 사이즈 지정
if (PRDT_DCODE_MINIMUM_SEG_COUNT == None) | (PRDT_DCODE_MINIMUM_SEG_COUNT == 'None') :
    PRDT_DCODE_MINIMUM_SEG_COUNT = 50
else : 
    PRDT_DCODE_MINIMUM_SEG_COUNT = int(PRDT_DCODE_MINIMUM_SEG_COUNT)

## logic 2 basket lift 의 상품 필터링용 Micro-seg 의 최소 상품 구매 건수 지정
if (PRDT_DCODE_MINIMUM_PURCHASE_COUNT == None) | (PRDT_DCODE_MINIMUM_PURCHASE_COUNT == 'None') :
    PRDT_DCODE_MINIMUM_PURCHASE_COUNT = 5
else : 
    PRDT_DCODE_MINIMUM_PURCHASE_COUNT = int(PRDT_DCODE_MINIMUM_PURCHASE_COUNT)

    
print("================================================")
print("============= Argument setting =================")
print("target_date : ",target_date,
      "\ntop_topic_n : ",topN,
      "\nprdt_flt_seg_count : ",PRDT_DCODE_MINIMUM_SEG_COUNT,
      "\nprdt_flt_purchase_count : ",PRDT_DCODE_MINIMUM_PURCHASE_COUNT,sep="")
print("================================================")


# --------------------------------------------
# 날짜 수기로 Key-in 필요 시 여기서 입력
# target_date = '2020-11-28'
# --------------------------------------------

target_date_6digits = datetime.datetime.strptime(target_date, '%Y-%m-%d').strftime('%y%m%d')
print(target_date, "(",target_date_6digits,")","th operation is getting started..")

######################################################
# Connection setting
######################################################

conf_dir = '/home/cdsadmin/AMT/src/conf/config.ini'
cfg = configparser.ConfigParser(interpolation=configparser.ExtendedInterpolation())
cfg.read(conf_dir)

global HOST,PORT,DB_ID,DB_PW

HOST = cfg['dbconnect']['host']
PORT = int(cfg['dbconnect']['port'])
DB_ID = cfg['dbconnect']['ID']
DB_PW = cfg['dbconnect']['PW']

#----------------------------------------------------------
# ■ logging
#----------------------------------------------------------
import logging
import logging.config

PYTHON_IP = '10.253.79.24'
MN = 'DAILY_BATCH_CODE_LDA_RECOMMENDER.py'
LG = 'LDA RECOMMENDER'
# logging.config.fileConfig(str(cfg['common']['log_config']),defaults={'date':datetime.datetime.now().strftime('%Y%m%d')})
logger     = logging.getLogger('aml_log')
fh = logging.FileHandler('/home/cdsadmin/AMT/src/logs/AMS/{:%Y%m%d}_AMS.log'.format(datetime.datetime.now()))
formatter = logging.Formatter(f'\nERR|CDS|{PYTHON_IP}|AMT|1등급|[ %(asctime)s ]|[{MN}]|{LG}|[ LineNo. : %(lineno)d ]| %(message)s')
fh.setFormatter(formatter)
logger.addHandler(fh)
###==========================================================================###

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


# -------------------------------------------------------
# Data Path
#  - 모델 및 상품마스터를 불러올 디렉토리 (폴더) 를 지정
# -------------------------------------------------------

path = cfg['Recommender']['MODEL_DIR'] 


# #-----------------------------------------------------
# # Logging 함수
# #-----------------------------------------------------

def add_log(module_nm, module_type_nm, step, query_type, target_table, start_time) : 
    
    global em
    
    print(f'\n query_type : {query_type} \n target_table : {target_table.lower()} \n target_date : {target_date}')
    tbl_name = 'tb_amt_campaign_anl_log' 
    target_table = target_table.lower()
    
    st = start_time.strftime('%Y-%m-%d %H:%M:%S')
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

    start_time = datetime.datetime.now()
    return (start_time) 

#-----------------------------------------------------
# Directory 내 최신파일 서치 함수
#-----------------------------------------------------
def file_search(file_dir, pattern) : 
    files = os.listdir(file_dir)
    p = re.compile(pattern) 

    f_list = []
    for f in files : 
        out = p.match(f)
        if out != None : f_list.append(f)
        else : pass

    f_list = np.sort(f_list)
    
    return f_list



######################################################
# Part 2. Main Part
######################################################

try : 

    # ====================================================
    # 2.1. Recent Master loading
    # ====================================================

    # 상품 마스터 불러오기 (product master)

    prdt_mst_list = file_search(file_dir = path, pattern = "^prdt_mst_[0-9]+[\.]csv$")
    prdt_mst_target_nm = prdt_mst_list[-1] # Max 값으로 지정. 가장 최신 일자 모델을 끌고 옴
    prdt_map = pd.read_csv(path + prdt_mst_target_nm)

    print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
          " || Chosen the latest product master is:",prdt_mst_target_nm)


    # Recent model loading

    model_list = file_search(file_dir = path, pattern = "^lda_model_[0-9]+[\.]model$")
    model_target_nm = model_list[-1] # Max 값으로 지정. 가장 최신 일자 모델을 끌고 옴
    lda_model_iter = LdaModel.load(path + model_target_nm) 

    print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
          " || Chosen the latest model is:",model_target_nm)


    # Loading Exclude Product list

    ex_prdt_list = file_search(file_dir = path, pattern = "^RECMD_EXCL_Prod_List[\.]csv$")
    ex_prdt_list_target = ex_prdt_list[-1] # Max 값으로 지정. 가장 최신 일자 모델을 끌고 옴
    ex_prdt_df = pd.read_csv(path + ex_prdt_list_target, encoding = 'UTF-8', delimiter='	')

    conn, engine = init_connection()
    ex_prdt_df.to_sql(name = 'recmd_excl_prdt_list', schema = DB_ID.lower(), 
                  con = engine, if_exists = 'replace', index = False, method = None,
                  dtype = {'PRDT_DI_CD': types.NVARCHAR(2),
                           'PRDT_DI_NM': types.NVARCHAR(20),
                           'PRDT_CAT_CD': types.NVARCHAR(10),
                           'PRDT_CAT_NM': types.NVARCHAR(20),
                           'PRDT_GCODE_CD': types.NVARCHAR(10),
                           'PRDT_GCODE_NM': types.NVARCHAR(20),
                           'PRDT_MCODE_CD': types.NVARCHAR(10),
                           'PRDT_MCODE_NM': types.NVARCHAR(20),
                           'PRDT_DCODE_CD': types.NVARCHAR(10),
                           'PRDT_DCODE_NM': types.NVARCHAR(100),
                           'EXCL_REASON': types.NVARCHAR(50)
                          })
    conn.close()
    engine.dispose()

    print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
          " || Chosen the latest product exclude list is:",ex_prdt_list_target)


    # ====================================================
    # 2.2. Raw data preprocessing 
    # ====================================================

    ## Multi-processing 용 함수 설정
    
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

        start_time = time.time()

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
              " || Data Loading is completed.",
              "(elapsed time :", np.round((time.time() - start_time) / 60, 2), "mins)")

        return result   


    
    ## Data 정제 Function (일별 배치용)
    
    def data_preparation(df) : 
        start_time = time.time()

        ## TF Transformation
        tf, idf, tfidf = tf_idf_matrix(df)

        ## Product Mapping
        prdt_cd = list(df['PRDT_DCODE_CD'].unique())
        prdt_cd_df = prdt_map[prdt_map['PRDT_DCODE_CD'].isin(prdt_cd)].sort_values(by = 'PRDT_DCODE_CD', ascending = True)

        ## Corpus 생성
        corpus = tf.apply(row_to_tuple, mst = prdt_cd_df, axis = 1)

        print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
              " || Data Preprocessing is completed.",
              "(elapsed time :", np.round((time.time() - start_time) / 60, 2), "mins)")

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


    # Raw data preprocessing - Corpus 생성

    start_time = datetime.datetime.now()

    ## --------------------------------------------------------
    ## multiprocessing 을 위한 argument setting 

    db_name = 'CDS_AMT.TB_AMT_RECMD_TMPR' ## target DB 
    n_core = 10

    ## data chuck arg 를 위한 기준 변수
    key = 'PRDT_CAT_CD'

    ## filter date
    flt_type = 'date'
    flt_var ='ANL_DT'
    ## --------------------------------------------------------


    flt_condition = {'from':target_date,'to':target_date} ## 일별 배치를 가정
    df_raw = multiprocesser()

    start_time = add_log(module_nm = '상품추천', module_type_nm = '운영', 
                         step = '1.pre-processing', query_type = 'Pre-processing', 
                         target_table = 'Data loading', start_time = start_time)


    df_flt = df_raw[['CUST_ID','ANL_DT','PRDT_DCODE_CD','DT_CNT']]
    prdt_df, tf, corpus = data_preparation(df_flt)

    start_time = add_log(module_nm = '상품추천', module_type_nm = '운영', 
                         step = '1.pre-processing', query_type = 'Pre-processing', 
                         target_table = 'Data Transformation (Input > Corpus)', start_time = start_time)


    # ====================================================
    # 2.3. 상품추천 로직 - 공통 모듈
    # ====================================================

    # ----------------------------------------------------
    # 2.3.1. Corpus 를 Topic DF 형태로 변환
    # ----------------------------------------------------

    ## 신규 토픽 생성 함수 
    def list_converter(lst, topic_len) : 
        n_iter = int(len(lst) / topic_len)
        for idx in range(n_iter) : 
            yield lst[topic_len*idx : topic_len*(idx+1)]
            idx += 1 # if idx % 100000 == 0 : print(idx)

    def Topic_df_converter(lda_model, corpus) : 

        global target_date
        global model_target_nm

        start = time.time()

        ## Step 1. gensim document topic 을 DF 로 변환
        ## 속도를 위해 CSR Matrix 활용
        ## ----------------------------------------------
        all_topics = lda_model.get_document_topics(corpus, minimum_probability=None)
        print("          Topic_df_converter :","Step 1-1. Loading topic is completed..",np.round((time.time() -start)/60,2),"mins")

        all_topics_csr = gensim.matutils.corpus2csc(all_topics)
        print("          Topic_df_converter :","Step 1-2. Sparse Matrix Coversion is completed..",np.round((time.time() -start)/60,2),"mins")

        all_topics_numpy = all_topics_csr.T.toarray()
        print("          Topic_df_converter :","Step 1-3. Sparse Matrix to Array Transformation is completed..",np.round((time.time() -start)/60,2),"mins")

        df = pd.DataFrame(all_topics_numpy) # all_topics_df
        print("          Topic_df_converter :","Step 1-4. Gensim object --> DF conversion is completed..",
              np.round((time.time() -start)/60,2),"mins")

        ## Step 2. DF 내 element 를 내림차순 Sorting 하여 Tuple 형태로 변환
        ## 변환된 Tuple 을 다시 multidimensional list 로 변환하여 DF 준비
        ## ----------------------------------------------
        topic_col_argsort = df.columns.values[np.argsort(-df.values, axis = 1)]
        topic_val_argsort = -np.sort(-df.values, axis = 1)

        topic_col_hstack = np.hstack(topic_col_argsort)
        topic_val_hstack = np.hstack(topic_val_argsort)
        topic_tuple = [(i,j) for i,j in zip(topic_col_hstack, topic_val_hstack)]

        out = list(list_converter(topic_tuple, len(df.columns)))

        print("          Topic_df_converter :","Step 2. Tuple & dimension changes are completed..",
              np.round((time.time() -start)/60,2),"mins")

        ## Step 3. 데이터프레임화 & 결과 테이블 리턴
        ## ----------------------------------------------
        Topic_df= pd.DataFrame(out, index = corpus.index)
        Topic_df.columns = ['topic'+str(i) for i in range(len(df.columns))]
        Topic_df = Topic_df.reset_index().rename(columns = {"index":"cust_id"})

        Topic_df.columns = [col.lower() for col in Topic_df.columns]
    #     Topic_df = Topic_df.set_index('cust_id')

        ## Step 4. sql 용 dF 준비
        df_sql = df.copy()
        df_sql.columns = ['topic_no'+str(i) for i in df_sql.columns ]
        df_sql['cust_id'] = corpus.index
        df_sql['crtn_dt'] = target_date
        df_sql['model_version'] = model_target_nm

        col_seq = ['crtn_dt','cust_id','model_version'] + ['topic_no' + str(i) for i in range(100)]
        df_sql = df_sql[col_seq]

        print("          Topic_df_converter :","Step 3. DF transformation is completed..",
              np.round((time.time() -start)/60,2),"mins")

        ## Print progress 
        print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
              " || Topic DF generation is completed.",
              "(elapsed time :", np.round((time.time() - start) / 60, 2), "mins)")

        return Topic_df, df_sql

    Topic_df, Topic_df_to_sql = Topic_df_converter(lda_model = lda_model_iter, corpus = corpus)

    
    ## Additional : 토픽 해석용 도출 & Topic_df Top3 토픽 필터링 DF 도출
    def topic_distribution(n, lda_model, Topic_df, nword) :

        """
        parameter 
        -------------
        @ n : Topic 수
        @ lda_model : 대상 LDA 모델
        @ Topic_df : 토픽의 Dataframe
        @ nword : 토픽 별 상품 대상 Top N
        """
        ## --------------------------------------------------------
        ## A.Top-N 만큼 해당하는 토픽을 뽑아 데이터 프레임 생성
        ## --------------------------------------------------------

        ## Step 1. Topic_df 중 토픽에 해당하는 유효 칼럼만 추출
        col_list = []
        for i in Topic_df.columns : 
            pattern = re.compile('^topic[0-9]+$')
            out = pattern.search(str(i))
            if out != None : 
                col_list.append(i)
            else : 
                pass

        ## Step 2. Topic_df 중 Top N 개 토픽만 추출하여 데이터프레임화
        col_argmax = [int(i.replace('topic','')) for i in col_list]
        top_n = np.array(col_argmax).argsort()[:n]
        top_n_col = [col_list[i] for i in top_n]

        df_flt = Topic_df[top_n_col]
        df_flt_export = Topic_df[['cust_id']+top_n_col] ## 타 분석의 input 용도로 활용
        # display(df_flt.head())

        ## --------------------------------------------------------
        ## B.토픽별 Count 비중 (Top1 ~ Top N 까지의 누적 비중)
        ## --------------------------------------------------------

        ## Step 1. 토픽을 갖고 있는 고객 수 계산 
        N_valid = len(df_flt[~df_flt.iloc[:,0].isna()])
    #     N_all = len(df_flt.iloc[:,0])
    #     print("The No. of Customers who has topic :",N_valid,"/",N_all )
    #     print("The No. of Customers who don't have topic :",N_all-N_valid,"/",N_all)

        ## Step 2. 토픽 칼럼을 합쳐 list 화 
        array_list = []
        for i in range(n) : 
            array_list.append(df_flt.iloc[:,i])

        # Step 3. Top 1 부터 Top N 까지의 누적 비중 합 구해주기
        df_list = []
        for k in range(n) : 
            if k == 0 :
                t = array_list[0]
            else : 
                t = reduce(lambda x, y: np.hstack([x, y]), array_list[:k+1])
                t = pd.Series(t)

            topic_ls = t[~t.isna()].apply(lambda x : x[0])
            topic_grp = [[i, np.array(topic_ls == i).sum()] for i in np.unique(topic_ls)]
            topic_grp = pd.DataFrame(topic_grp, columns = ['topic_no','topic_cnt_'+str(k)])
            topic_grp['topic_ratio_'+str(k)] = round(topic_grp['topic_cnt_'+str(k)] *100 / N_valid ,5) # Top1_df['topic_cnt'].sum() 

            df_list.append(topic_grp)

        # Step 4. Merge
        df_merge = reduce(lambda x,y: pd.merge(x,y, how = 'left', on = 'topic_no'),  df_list) 

        ## ---------------------------------------
        ## C. 토픽 별 상위 30개 상품 가중치 도출
        ## ---------------------------------------
        topic_keyword = lda_model.print_topics(num_topics = 100, num_words = nword)
        topic_keyword_df = pd.DataFrame(topic_keyword, columns = ['topic_no','topic_keyword_weight'])

        ## ---------------------------------------
        ## D. 데이터 결합
        ## ---------------------------------------
        df_final = pd.merge(df_merge, topic_keyword_df, how = 'left', on = 'topic_no')
        # df_final = df_final.sort_values(by = 'topic_cnt', ascending = False)
        # display(df_final)

        return df_flt, df_final

    Topic_df_TopN, topic_distribution = topic_distribution(n=3,lda_model=lda_model_iter,Topic_df=Topic_df, nword = 30)

    start_time = add_log(module_nm = '상품추천', module_type_nm = '운영', 
                         step = '1.pre-processing', query_type = 'Pre-processing', 
                         target_table = 'Data Transformation (Corpus > Topic DF)', start_time = start_time)


    # ----------------------------------------------------
    # 2.3.2. 고객 별 토픽 DF 저장용 테이블 생성 
    # ----------------------------------------------------

    # Topic DF DB 적재를 위한 컬럼 지정
    col_dtype = {"crtn_dt": 'DATE',"cust_id": 'NVARCHAR(20)','model_version': 'NVARCHAR(50)'}
    topic_col_list = ["topic_no" + str(i) for i in range(100)]

    for i in topic_col_list : 
        col_dtype.update({i:'DECIMAL(18,17)'}) 


    # === 고객 별 토픽 DF 데이터 Insert 본문 : START ======================= # 

    
    ## Step 1. 고객별 토픽 DF 에 기 적재된 고객 리스트 추출 
    topic_df_tbl_nm = 'TB_AMT_RECMD_CUST_TOPIC_DF'

    prv_cust_id = get_ipython().run_line_magic('sql', 'SELECT DISTINCT CUST_ID FROM CDS_AMT.{topic_df_tbl_nm}')
    prv_cust_id = prv_cust_id.DataFrame()

    try : 
        prv_cust_id_list = prv_cust_id.cust_id.unique().tolist()
    except : 
        prv_cust_id_list = []

        
    ## Step 2. 배치날짜 고객 리스트 추출
    cust_id_list = Topic_df_to_sql.cust_id.unique().tolist()

    
    ## Step 3. 과거 구매이력 존재고객 (중복고객) 도출
    cust_id_intersection = list(set(prv_cust_id_list) & set(cust_id_list))
    cust_id_delete_list = str(cust_id_intersection).replace('[','').replace(']','')

    
    ## Step 4. 고객별 토픽 DF --> DB Insert 준비 : 테이블 생성 & 과거 데이터 Delete
    db_schema_list, db_table_list = schema_inspector()

    if topic_df_tbl_nm.lower() not in db_table_list :
        """데이터 삭제 시에만 생성
        """
        col_dtype_for_query_cust_topic = str(col_dtype).replace("{","").replace("}","").replace("'","").replace(":"," ")

        get_ipython().run_cell_magic('sql', '', """
        CREATE TABLE {DB_ID}.{topic_df_tbl_nm} ( {col_dtype_for_query_cust_topic} ) ; """)

        print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")," || Topic DB table creation is completed.." )
        start_time = add_log(module_nm = '상품추천', module_type_nm = '운영', 
                         step = '1.pre-processing', query_type = 'Create', 
                         target_table = 'Topic DB Table', start_time = start_time)

    elif cust_id_delete_list != '' : 
        get_ipython().run_cell_magic('sql', '', """
        DELETE FROM {DB_ID}.{topic_df_tbl_nm} WHERE CUST_ID IN ({cust_id_delete_list}) """)

        print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
              " || Delete duplicated CUST_ID in Topic DB Table is completed.." )
        start_time = add_log(module_nm = '상품추천', module_type_nm = '운영', 
                         step = '1.pre-processing', query_type = 'Delete', 
                         target_table = 'Duplicated CUST_ID IN Topic DB Table', start_time = start_time)

    else : 
        pass


    start = time.time()


    ## Step 5. Topic_df(_to_sql용) 결과 dictionary 형태로 변환
    Topic_df_dict = Topic_df_to_sql.to_dict(orient = 'records') ## listToWrite


    ## Step 6. Main Part: 데이터 INSERT 
    col_list = list(col_dtype.keys())
    col_variables = ','.join(col_list)
    col_values = ','.join([":"+i for i in col_list])
    args = f"""INSERT INTO CDS_AMT.{topic_df_tbl_nm} ({col_variables}) VALUES ({col_values})"""

    conn = DB_Connection()
    cur = conn.cursor()
    cur.executemany(args, Topic_df_dict)  
    cur.close()

    print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
          " || Customer Topic DF update & insert to DB is completed.",
          "(elapsed time :", np.round((time.time() - start) / 60, 2), "mins)")


    # col_variables_rev = col_variables.replace('model_version'," 'lda_model_201024.model' as model_version")
    # args_rev = f"""INSERT INTO CDS_AMT.TB_AMT_RECMD_CUST_TOPIC_DF (
    # SELECT {col_variables_rev} FROM CDS_AMT.TB_AMT_RECMD_CUST_TOPIC_DF_TMP)"""
    # get_ipython().run_cell_magic('sql', '', """ {args_rev}""")


    start_time = add_log(module_nm = '상품추천', module_type_nm = '운영', 
                     step = '1.pre-processing', query_type = 'Insert', 
                     target_table = topic_df_tbl_nm, start_time = start_time)

    # === 고객 별 토픽 DF 데이터 Insert 본문 : END ======================= # 
    


    # ----------------------------------------------------
    # 2.3.3. 구매이력이 0 인 고객번호 제외 후 [예외처리 1]
    # 발생 case :
    # > 상품마스터가 모델 업데이트 이후 업데이트 될 경우 (i.e. 신규상품 추가)
    #   신규 상품만 solely 구매한 이력밖에 없을 경우 발생
    # ----------------------------------------------------
    
    bf_n = len(corpus)
    cust_id_remove = corpus[corpus.map(lambda x: len(x)) < 1].index.tolist()

    corpus_iter = corpus[~corpus.index.isin(cust_id_remove)]
    Topic_df = Topic_df[~Topic_df.cust_id.isin(cust_id_remove)]

    print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
          " || Exclude those solely bought new products not covered by current product master :",
          bf_n,"->",len(corpus_iter),"/",f"{len(cust_id_remove)} excluded.")


    # ----------------------------------------------------
    # 2.3.4. LIFT 계산용 판매량 집계
    # ----------------------------------------------------

    get_ipython().run_cell_magic('sql', '', """

    CREATE TABLE CDS_AMT.RECMD_LOGIC1_BASKET_AGG (
        prdt_cat_cd    nvarchar(10),
        prdt_dcode_cd  nvarchar(10),
        dt_cnt_all     bigint,
        cust_cnt_all   bigint,
        dt_cnt_rt_all  decimal
    ) ; 

    INSERT INTO CDS_AMT.RECMD_LOGIC1_BASKET_AGG (
        WITH TMP AS (
            -- 최근 7일간 구매이력 있는 고객만 필터링
            SELECT CUST_ID, PRDT_CAT_CD, PRDT_DCODE_CD, ANL_DT, DT_CNT, 
                   DENSE_RANK() OVER (PARTITION BY CUST_ID ORDER BY ANL_DT DESC) AS CUST_RANK
              FROM CDS_AMT.TB_AMT_RECMD_TMPR
             WHERE ANL_DT BETWEEN ADD_DAYS(TO_DATE('{target_date}','YYYY-MM-DD'),-6) AND '{target_date}' 
        ), 
        TMP2 AS (
            SELECT  PRDT_CAT_CD
                   ,PRDT_DCODE_CD
                   ,SUM(DT_CNT) AS DT_CNT_ALL
              FROM TMP 
             WHERE CUST_RANK = 1 -- 최근 7일 이내 2번 이상 내방한 고객들의 구매이력 중 최신 이력만 필터링

          GROUP BY PRDT_CAT_CD, PRDT_DCODE_CD
        )
        SELECT  A.PRDT_CAT_CD
                ,A.PRDT_DCODE_CD
                ,A.DT_CNT_ALL
                ,B.CUST_CNT_ALL
                ,A.DT_CNT_ALL / B.CUST_CNT_ALL AS DT_CNT_RT_ALL -- 최근 7일간 전체 고객의 90일간 품목별 구매 비율
          FROM  TMP2 A,
                (SELECT COUNT(DISTINCT CUST_ID) AS CUST_CNT_ALL FROM TMP) B
         -- WHERE  A.DT_CNT_ALL > 1 -- 집계 시 구매 건수가 1인 아이템은 제외
    ) """)


    conn, engine = init_connection()
    basket_agg_df = pd.read_sql("select * from CDS_AMT.RECMD_LOGIC1_BASKET_AGG", conn)
    conn.close()
    engine.dispose()

    basket_agg_df.columns = [col.lower() for col in basket_agg_df.columns]
    basket_agg_df.prdt_dcode_cd = basket_agg_df.prdt_dcode_cd.apply(lambda x : int(x))
    basket_agg_df.dt_cnt_rt_all = basket_agg_df.dt_cnt_all / basket_agg_df.cust_cnt_all
    basket_agg_df.sort_values(by = 'dt_cnt_rt_all', ascending = False, inplace = True)
    

    # ====================================================
    # 2.4. 상품추천 로직 1 : 확률곱 & Lift
    # ====================================================

    ## -----------------------------------------------
    ## 2.4.1. 함수 세팅
    ## -----------------------------------------------
    
    def recommender_logic_1(row_id) :  

        global lda_model_iter
        global corpus_iter
        global Topic_df
        global prdt_map

        lda_model = lda_model_iter
        corpus = corpus_iter.copy() 
        prdt_mst = prdt_map.copy()


        ##  고객 기구매이력 (Corpus_df) 과 상품마스터 (dict_df)를 붙임
        corpus_df = pd.DataFrame(corpus[row_id], columns = ['id','dt_cnt'])
        prdt_mst = prdt_mst.rename(columns = {"row_id":"id","PRDT_DCODE_CD":"prdt_dcode_cd","PRDT_DCODE_NM":"prdt_dcode_nm"})

        try : 
            ## 고객 별 Topic list 를 X 에 저장
            X = Topic_df.iloc[:,1:].iloc[row_id]

            Xa = X[X.map(lambda y: y[1]) > 0].tolist() 
            X = [i[0] for i in Xa] 

            ## 토픽별로 상품 가중치 DataFrame 화
            X_list = [] 
            for j,i in enumerate(X) :
                """토픽별 상품 Top 30 개만 가져옴
                """
                X_term = lda_model.get_topic_terms(i, topn = 30) 
                X_df = pd.DataFrame(X_term, columns = ['id',"topic"+str(j)+"_dist"])
                X_list += [X_df]

            X_merge = reduce(lambda x, y: pd.merge(x, y, how = 'outer', on = 'id'), X_list)

            X_final = pd.merge(corpus_df, X_merge, how = 'outer', on = 'id')
            X_final = pd.merge(prdt_mst, X_final, how = 'right', on = 'id')
            X_final['cust_id'] = corpus.index[row_id]

        except : 
            """
            구매 이력이 거의 없는 케이스 예외 처리 (20.11.30 업데이트)
            토픽이 생성되지 않는 고객의 경우 구매 이력만 가지고 떨굼
            """
            Xa = [(9999,1)]
            X_final = pd.merge(corpus_df, prdt_mst, how = 'left', on = 'id')
            X_final['cust_id'] = corpus.index[row_id]

        return recommender_logic_1_ranking(topic_dist = Xa, df = X_final)

    def recommender_logic_1_ranking(topic_dist, df) : 

        """
        params
        ----------
        @ topic_dist : 고객 별 토픽 분포 확률 
        @ df : 고객 X 토픽 별 상품 및 구매이력 Dataframe
        """
        global topN
        global target_date

        cust_id_repair = []

        try : 
            ## 로직 1 : 고객 별 토픽확률 X 토픽별 상품확률 곱 합산 방식 
            X_col = df.columns[df.columns.str.contains('topic')] # sales['컬럼명'].str.contains('검색하고자 하는 문자열')
            wt = [i[1] for i in topic_dist] ## 토픽 분포 확률 추출
            X_wt = df[X_col] * wt

            df['prob_wt'] = X_wt.sum(axis=1)
            df['prob_wt_rank'] = df['prob_wt'].rank(method = 'dense', ascending = False)

            ## LIFT 로직 : 해당 상품의 전체판매비중 대비 로직1계산(확률곱) 상대비중
            ## (20.12.07 업데이트 : lift 계산하는 과정을 sql 로 진행)
            ## (20.12.14 업데이트 : sql 작업 진행 방식이 너무 느려, 다시 multiprocessing 으로 진행)

            ## ----------------
            df = pd.merge(df, basket_agg_df, how = 'left', on = 'prdt_dcode_cd')
            df['lift'] = df['prob_wt'] / df['dt_cnt_rt_all']
            df['lift_rank'] = df['lift'].rank(method = 'dense', ascending = False)
            ## ----------------

            ## (20.12.18 업데이트 : LIFT 로직 보정)
            ## TOPN 토픽만 대상으로 확률곱 계산하고, LIFT 랭킹 구함
            X_wt_topN = df[X_col].iloc[:,:topN] * wt[:topN] 
            df['prob_wt_top'+str(topN)] = X_wt_topN.sum(axis=1)
            df['lift_top'+str(topN)] = df['prob_wt_top'+str(topN)] / df['dt_cnt_rt_all']
            df['lift_rank_top'+str(topN)] = df['lift_top'+str(topN)].rank(method = 'dense', ascending = False)
            ## -----------------

        except : 
            cust_id_repair += [df.cust_id[0]]

            """ 구매 이력이 거의 없는 케이스 예외 처리 (20.11.30 업데이트)
                 >> Xa = [(9999,1)] 인 경우 처리 로직에 해당

                이 경우, 최종 로직에서 장바구니의 랭킹으로 처리 (*최종 협의 필요)
                flag 는 prob_wt = 9 , lift = 9999999999 로 처리 
            """
            df['dt_cnt_all'] = [None] * len(df) 
            df['cust_cnt_all'] = [None] * len(df)
            df['dt_cnt_rt_all'] = [None] * len(df)

            df['lift'] = 9999999999
            df['lift_rank'] = [None] * len(df)
            df['prob_wt'] = 9 
            df['prob_wt_rank'] = [None] * len(df)   

            ## 20.12.18 추가 Top 5 토픽 리프트
            df['prob_wt_top'+str(topN)] = [None] * len(df)
            df['lift_top'+str(topN)] = [None] * len(df)
            df['lift_rank_top'+str(topN)] = [None] * len(df)
            ## -----------------

        df.columns = [col.lower() for col in df.columns]
        df['anl_dt'] = datetime.datetime.strptime(target_date, '%Y-%m-%d').strftime('%Y-%m-%d') 

        ## --------------------------------------------------------------------------
        ## 20.12.10 로직 추가 
        ## 속도를 위하여 Top 20 토픽까지만 필터 --> 정형화시킴
        ## 정형화시킨 데이터프레임을 concat 형태가 아닌 append 형태로 바로 적재 진행
        ## --------------------------------------------------------------------------


        """ 데이터 프레임용 칼럼 순서 결정 
        """
        col_common = ["anl_dt","cust_id","id","prdt_dcode_cd", "prdt_dcode_nm","dt_cnt","prob_wt","prob_wt_rank",
                      "dt_cnt_all","cust_cnt_all","dt_cnt_rt_all","lift","lift_rank",
                      "prob_wt_top"+str(topN),"lift_top"+str(topN),"lift_rank_top"+str(topN)] 
        pattern = re.compile(r"^topic[0-9]+[\_]dist$")

        col_topic = [col for col in list(df.columns) if pattern.match(col) != None ]

        """ 고객 별 토픽 수가 20개 미만인 경우: 기존 토픽을 넣고 나머지는 dummy 처리
            고객 별 토픽 수가 20개 이상인 경우: 상위 토픽 20개만 짤라서 가져옴
        """
        if len(col_topic) < 20 : 
            col_topic = [ "topic" + str(i) + "_dist" for i in range(0,len(col_topic))]
            col_seq = col_common + col_topic
            df_flt = df[col_seq]

            col_topic_dummy = [ "topic" + str(i) + "_dist" for i in range(len(col_topic),20)]
            for col_dummy in col_topic_dummy :
                df_flt[col_dummy] = [None] * len(df_flt)

        else : 
            col_topic = [ "topic" + str(i) + "_dist" for i in range(0,20)]
            col_seq = col_common + col_topic
            df_flt = df[col_seq]

        df_flt = df_flt.where(pd.notnull(df_flt), None)
        listToWrite = df_flt.to_dict(orient = 'records')

        """SAP HANA DB 로 insert 시킴

           Alchemy Core & Executemany 방식이 더 나은 것으로 판단
           --> 따라서 편차가 적은 EXECUTEMANY 방식으로 최종 결정

           ## 단위 속도 테스트 결과 (20.12.16) - 고객 1명 처리 시간 :
           0. bulk_insert_mapping : 221 ms
           1. exexutemany : 70 ~ 80 ms 
           2. to_sql : 100 ~ 120 ms
           3. alchemy core insert : 60 ~ 100 ms 
        """

        ## 방식 1 :  executemany
        cur.executemany(query, listToWrite)  

        ## 방식 2 : to_sql
        # df_flt.to_sql(name = 'RECMD_LOGIC1_PROBWT_RESULT', schema = 'cds_amt', con = engine, if_exists = 'append', index = False)

        ## 방식 3 :  Alchemy core
        # engine.execute( Current.__table__.insert(), listToWrite)

        return cust_id_repair # 1 # df_flt #listToWrite # df_flt[col_list_old]  

    def multiprocesser_logic1_tqdm(worker, arg_list, n_core) :  

        start_time = time.time()
        repair_cust_list = []

        print("          Multiprocessing is getting started...", np.round((time.time() - start_time)/60,2),"mins")
        if __name__ == '__main__' : 
            p = multiprocessing.Pool(processes = n_core)
            p.deamon = True
            for result in tqdm(p.imap_unordered(func = worker, iterable = arg_list), total = len(arg_list)) :
                repair_cust_list += result

            print(1)
            p.close()

            print(2)
            p.terminate()

            print(3)
            time.sleep(2)
            p.join()

            print(4)
            time.sleep(2)
            del p

        time.sleep(10)

        print("          Multiprocessing for Logic_1 result is completed...", np.round((time.time() - start_time)/60,2),"mins")
        print(f"          Exceptional treatment needs to be applied to customer n = {len(repair_cust_list)} ...", np.round((time.time() - start_time)/60,2),"mins")

    #     # output 을 dictionary (= listToWrite) 로 받아오는 경우
    #     import itertools
    #     result_list = list(itertools.chain.from_iterable(result_list))

    #     # output 을 DF 로 받아오는 경우
    #     ## mutliprocessing 결과를 DF 로 변환
    #     col_list = result_list[0].columns

    #     res = pd.DataFrame()
    #     for idx, col in enumerate(col_list) : 
    #         v = map(lambda x: x.iloc[:,idx], result_list)
    #         v_h = np.hstack(list(v))
    #         res[col] = v_h

        ## Print progress 
        print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
              " || [Logic 1 : Prob Weight] calculation & DB Insert task are completed.",
              "(elapsed time :", np.round((time.time() - start_time) / 60, 2), "mins)")

        return repair_cust_list # i # result_list # res 


    ## -----------------------------------------------
    ## 2.4.2. 최종 적재용 테이블 생성용 데이터타입 지정
    ## -----------------------------------------------
    col_dtype = {"anl_dt": 'DATE',
                 "cust_id": 'NVARCHAR(20)',
                 "id": 'DECIMAL(10)', 
                 "prdt_dcode_cd": 'NVARCHAR(10)',
                 "prdt_dcode_nm": 'NVARCHAR(100)',
                 "dt_cnt": 'DECIMAL(20)',
                 "prob_wt": 'DECIMAL(18,17)',
                 "prob_wt_rank": 'DECIMAL(10)' }

    col_dtype2 = {"cust_cnt_all": 'DECIMAL(20)', # BIGINT
                 "dt_cnt_all": 'DECIMAL(20)', # BIGINT
                 "dt_cnt_rt_all": 'DECIMAL(20,10)', 
                 "lift": 'DECIMAL(20,10)',
                 "lift_rank": 'DECIMAL(10)',
                 "prob_wt_top"+str(topN):'DECIMAL(20,10)',
                 "lift_top"+str(topN):'DECIMAL(20,10)',
                 "lift_rank_top"+str(topN):'DECIMAL(10)'}

    topic_col_list = ["topic" + str(i) + "_dist" for i in range(0,20)]

    col_final = dict(col_dtype , **col_dtype2)
    for i in topic_col_list : 
        col_final.update({i:'DECIMAL(18,17)'}) 

    col_dtype_for_query_final = str(col_final).replace("{","").replace("}","").replace("'","").replace(":"," ")

    ## -----------------------------------------------
    ## ※ Alchemy Core Insert 방식 메타 지정
    ## -----------------------------------------------
    # Base = declarative_base()

    # class Current(Base):
    #     __tablename__ = "RECMD_LOGIC1_PROBWT_RESULT"

    #     anl_dt = Column(types.DATE)
    #     cust_id = Column(types.NVARCHAR(20), primary_key = True)
    #     id = Column(types.INTEGER)
    #     prdt_dcode_cd = Column(types.NVARCHAR(10))
    #     prdt_dcode_nm = Column(types.NVARCHAR(100))
    #     dt_cnt = Column(types.BIGINT)
    #     prob_wt = Column(types.DECIMAL)
    #     prob_wt_rank = Column(types.INTEGER)
    #     cust_cnt_all = Column(types.DECIMAL)
    #     dt_cnt_all = Column(types.DECIMAL)
    #     dt_cnt_rt_all = Column(types.DECIMAL)
    #     lift = Column(types.DECIMAL)
    #     lift_rank = Column(types.INTEGER)
    #     topic0_dist = Column(types.DECIMAL)
    #     topic1_dist = Column(types.DECIMAL)
    #     topic2_dist = Column(types.DECIMAL)
    #     topic3_dist = Column(types.DECIMAL)
    #     topic4_dist = Column(types.DECIMAL)
    #     topic5_dist = Column(types.DECIMAL)
    #     topic6_dist = Column(types.DECIMAL)
    #     topic7_dist = Column(types.DECIMAL)
    #     topic8_dist = Column(types.DECIMAL)
    #     topic9_dist = Column(types.DECIMAL)
    #     topic10_dist = Column(types.DECIMAL)
    #     topic11_dist = Column(types.DECIMAL)
    #     topic12_dist = Column(types.DECIMAL)
    #     topic13_dist = Column(types.DECIMAL)
    #     topic14_dist = Column(types.DECIMAL)
    #     topic15_dist = Column(types.DECIMAL)
    #     topic16_dist = Column(types.DECIMAL)
    #     topic17_dist = Column(types.DECIMAL)
    #     topic18_dist = Column(types.DECIMAL)
    #     topic19_dist = Column(types.DECIMAL)


    ## --------------------------------------------------
    ## 2.4.3. 배치 로직 실행 전 테이블 Drop & Create
    ## --------------------------------------------------
    tbl_name = 'TB_AMT_RECMD_LOGIC1_PROBWT_RESULT'
    db_schema_list, db_table_list = schema_inspector()

    if tbl_name.lower() in db_table_list : 
        get_ipython().run_cell_magic('sql', '', """DROP TABLE {DB_ID}.{tbl_name} ; """)

    else : 
        pass


    get_ipython().run_cell_magic('sql', '', """
    -- 적재용 테이블 생성 
    CREATE TABLE CDS_AMT.TB_AMT_RECMD_LOGIC1_PROBWT_RESULT (
    {col_dtype_for_query_final}
    )""")

    start_time = add_log(module_nm = '상품추천', module_type_nm = '운영', 
                     step = '2.LDA모델 적용 및 추천결과 도출 : LOGIC 1', query_type = 'Create', 
                     target_table = 'TB_AMT_RECMD_LOGIC1_PROBWT_RESULT', start_time = start_time)


    ## --------------------------------------------------
    ## 2.4.4. Logic 1 table 에 Multiprocessing 결과 Insert 수행
    ## --------------------------------------------------
    
    # ## Alchemy 용 세팅
    
    # conn, engine = init_connection()
    # input_list = range(len(corpus_iter))
    # res_df1 = multiprocesser_logic1_tqdm(worker = recommender_logic_1,  arg_list = input_list, n_core = 20)
    # conn.close()
    # engine.dispose()

    
    
    ## executemany 용 세팅

    col_em = list(col_final.keys())
    col_list = ','.join(col_em)
    col_values = ','.join([":"+i for i in col_em])
    query = f"""INSERT INTO CDS_AMT.TB_AMT_RECMD_LOGIC1_PROBWT_RESULT ({col_list}) VALUES ({col_values})"""


    ## input - array split & Multiprocessing

    conn = DB_Connection()
    cur = conn.cursor()

    input_list = range(len(corpus_iter))

    if len(corpus_iter) > 400000 : 
        input_list_split = np.array_split(input_list, 2)

        res_df1 = []
        for k, l_iter in enumerate(input_list_split) :
            print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
              f" || [Logic 1 : Prob Weight] {k+1}/2 of multiprocessing is getting started.." )
            res_df1 += multiprocesser_logic1_tqdm(worker = recommender_logic_1,  arg_list = l_iter, n_core = 15)
    else : 
        res_df1 = multiprocesser_logic1_tqdm(worker = recommender_logic_1,  arg_list = input_list, n_core = 15)

    cur.close()

    start_time = add_log(module_nm = '상품추천', module_type_nm = '운영', 
                     step = '2.LDA모델 적용 및 추천결과 도출 : LOGIC 1', query_type = 'Insert', 
                     target_table = 'TB_AMT_RECMD_LOGIC1_PROBWT_RESULT', start_time = start_time)


    # ====================================================
    # 2.5. 상품추천 로직 2 : Micro-segment
    # ====================================================

    ## -----------------------------------------------
    ## 2.5.1. 함수 세팅 및 Micro-segment 후보군 도출 (Top1, Top2, Top3 토픽)
    ## -----------------------------------------------

    def recommender_logic_2_micro_segment_creation(seg_n, Topic_df) :

        global target_date
        global model_target_nm

        """
        params 
        -----------------
        @ seg_n : 몇 개의 topic 으로 micro-segment 를 만들 것인지 설정
        @ Topic_df : micro_segment 생성을 위한 Topic_df input 
        @ df_raw : DT_CNT 집계를 위한 원본 데이터 input 

        return 
        -----------------
        1. seg_df : 고객 cust_id 별 최종 micro-segment 번호 

        """
        start_time = time.time()
        print("          Topic parsing is getting started..", np.round((time.time() - start_time)/60, 2),"mins")    

        ## Step 1 : seg_n 수에 맞춰 micro_segment : 1 ~ n 만큼 DataFrame 을 생성
        seg_list = []

        for i in range(seg_n) :
            print(f"          Creating Micro-segment using Top {i+1} topic is in progress..", np.round((time.time() - start_time)/60, 2),"mins")
            res = Topic_df.apply(recommender_logic_2_micro_segment_parsing, seg_count = i+1, axis = 1)
            res.name = 'seg_' + str(i+1) + 'grp'
            res = pd.DataFrame(res).set_index(Topic_df['cust_id']).reset_index()
            seg_list.append(res)

        seg_df = reduce(lambda x,y: pd.merge(x,y, how = 'left',on = 'cust_id'), seg_list)
        print("          Topic parsing is completed..", np.round((time.time() - start_time)/60, 2),"mins")


        seg_df['anl_dt'] = datetime.datetime.strptime(target_date, '%Y-%m-%d') 
        seg_df['model_version'] = model_target_nm

        print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
          " || Micro-segment creation is completed.",
          "(elapsed time :", np.round((time.time() - start_time) / 60, 2), "mins)")

        return seg_df

    def recommender_logic_2_micro_segment_parsing(a, seg_count) :
        """
        토픽확률이 0 이상인 토픽만 발라옴
        만약 토픽이 3개 미만인 고객의 경우 9999 처리
        """
        try : 
            a_flt = [i for i in a if type(i) == tuple and i[1] > 0 ]

            if len(a_flt) < seg_count : 
                res = [x[0] for x in a_flt] + [9999] * (seg_count - len(a_flt))
                a_sort = [str(i) for i in np.sort(res)]
                a_join = '@'.join(a_sort)

            else : 
                res = [x[0] for x in a_flt[:seg_count]]
                a_sort = [str(i) for i in np.sort(res)]
                a_join = '@'.join(a_sort)

        except : 
            a_sort = ['9999'] *3
            a_join = '@'.join(a_sort)

        return a_join

    seg_df = recommender_logic_2_micro_segment_creation(3, Topic_df)



    ## -----------------------------------------------
    ## 2.5.2. 테이블 생성 및 과거 중복일자 데이터 Delete (유사시 진행)
    ## -----------------------------------------------
    
    seg_df_tbl_name = 'TB_AMT_CUST_MICROSEG_TMP'
    db_schema_list, db_table_list = schema_inspector()

    if seg_df_tbl_name.lower() not in db_table_list :
        """데이터 삭제 시에만 생성
        """
        get_ipython().run_cell_magic('sql', '', """
        CREATE TABLE CDS_AMT.{seg_df_tbl_name} (
            cust_id    NVARCHAR(20),
            seg_1grp   NVARCHAR(4),
            seg_2grp   NVARCHAR(20),
            seg_3grp   NVARCHAR(20),
            anl_dt     DATE,
            model_version NVARCHAR(50) ) """)

        print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")," || Micro-segment Raw data Table creation is completed.." )
        start_time = add_log(module_nm = '상품추천', module_type_nm = '운영', 
                             step = '2.LDA모델 적용 및 추천결과 도출 : LOGIC 2', query_type = 'Create', 
                             target_table = f'Customer Micro-seg Raw Table : {seg_df_tbl_name}', start_time = start_time)

    else :
        get_ipython().run_cell_magic('sql', '', """
        DELETE FROM CDS_AMT.{seg_df_tbl_name} WHERE ANL_DT = '{target_date}' AND MODEL_VERSION = '{model_target_nm}' """)

        print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
              " || Delete Old ver. of customer's Micro-segment information is completed.." )
        start_time = add_log(module_nm = '상품추천', module_type_nm = '운영', 
                             step = '2.LDA모델 적용 및 추천결과 도출 : LOGIC 2', query_type = 'Delete', 
                             target_table = f'Customer Micro-seg Raw Table : {seg_df_tbl_name}', start_time = start_time)


    ## -----------------------------------------------
    ## 2.5.3. seg_df 를 DB Insert
    ## -----------------------------------------------
    
    start = time.time()

    conn, engine = init_connection()
    seg_df.to_sql(name = seg_df_tbl_name.lower(), schema = DB_ID.lower(), 
                  con = engine, if_exists = 'append', index = False, chunksize = 1000,
                  dtype = {'cust_id': types.NVARCHAR(20),
                           'seg_1grp': types.NVARCHAR(4),
                           'seg_2grp': types.NVARCHAR(20),
                           'seg_3grp': types.NVARCHAR(20),
                           'anl_dt': types.DATE,
                           'model_version':types.NVARCHAR(50)})
    conn.close()
    engine.dispose()

    print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
          " || Insert Micro-segment DF into DB is completed.",
          "(elapsed time :", np.round((time.time() - start) / 60, 2), "mins)")

    start_time = add_log(module_nm = '상품추천', module_type_nm = '운영', 
                     step = '2.LDA모델 적용 및 추천결과 도출 : LOGIC 2', query_type = 'Insert', 
                     target_table = f'Customer Micro-seg Raw Table : {seg_df_tbl_name}', start_time = start_time)


    ## -----------------------------------------------
    ## ※ 21.01.15 updates !! 
    ## 모델 적용 첫 날 로직 추가
    ## 추가적으로 최근 7일치 데이터 insert 로직만 추가하여 실행
    ## -----------------------------------------------

    get_ipython().run_cell_magic('sql', 'date_count <<', """
    SELECT MAX(ANL_DT) AS ANL_DT_MAX, COUNT(DISTINCT ANL_DT) AS ANL_DT_CNT 
    FROM CDS_AMT.{seg_df_tbl_name} 
    WHERE MODEL_VERSION = '{model_target_nm}'
    """)

    new_dt_cnt = date_count.DataFrame()['anl_dt_cnt'].values[0]

    if new_dt_cnt <= 1 :

        ## 기간 설정
        insert_date_from = datetime.datetime.strptime(target_date, '%Y-%m-%d') - timedelta(days = 7) 
        insert_date_to = datetime.datetime.strptime(target_date, '%Y-%m-%d') - timedelta(days = int(new_dt_cnt))
        step = datetime.timedelta(days=1)

        date_list = []
        while insert_date_from <= insert_date_to:
            date_list.append(insert_date_from.strftime('%Y-%m-%d'))
            insert_date_from += step

        ## dictionary 형태로 변형
        day_dict = []
        for i in date_list :
            day_dict += [{'from':i, 'to':i}]

        start_time = add_log(module_nm = '상품추천', module_type_nm = '운영', 
                     step = '2.LDA모델 적용 및 추천결과 도출 : LOGIC 2', query_type = 'Create', 
                     target_table = 'Replenish Customer Micro-seg Raw Table upon new model updates', start_time = start_time)

        print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
          " || Replenishing Micro-segment data for recent 7 days as model is now getting started.. ")

        for t in day_dict : 

            flt_condition = t # 시간 filtering condition 
            df_raw = multiprocesser() 

            df_flt = df_raw[['CUST_ID','ANL_DT','PRDT_DCODE_CD','DT_CNT','SUM_QTY']]
            prdt_df, tf, corpus = data_preparation(df_flt)
            Topic_df, Topic_df_to_sql = Topic_df_converter(lda_model = lda_model_iter, corpus = corpus)

            # seg_df 생성하여 DB 에 Insert
            seg_df_r = recommender_logic_2_micro_segment_creation(3, Topic_df)
        
            replenish_date = list(t.values())[0]
            seg_df_r['anl_dt'] = replenish_date
            
            start = time.time()

            conn, engine = init_connection()
            seg_df_r.to_sql(name = seg_df_tbl_name.lower(), schema = DB_ID.lower(), 
                            con = engine, if_exists = 'append', index = False, chunksize = 1000,
                            dtype = {'cust_id': types.NVARCHAR(20),
                                     'seg_1grp': types.NVARCHAR(4),
                                     'seg_2grp': types.NVARCHAR(20),
                                     'seg_3grp': types.NVARCHAR(20),
                                     'anl_dt': types.DATE,
                                     'model_version':types.NVARCHAR(50)})
            conn.close()
            engine.dispose()

            print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                  f" || Insert New Micro-segment DF into DB is completed : {t}",
                  "(elapsed time :", np.round((time.time() - start) / 60, 2), "mins)")
            
            start_time = add_log(module_nm = '상품추천', module_type_nm = '운영', 
                     step = '2.LDA모델 적용 및 추천결과 도출 : LOGIC 2', query_type = 'Insert', 
                     target_table = f'Replenish Customer Micro-seg Raw Table upon new model updates : {t}', start_time = start_time)

    else :
        pass 


    get_ipython().run_cell_magic('sql', '', """
    -- 최근 30일치 데이터만 rolling 하여 적재
    DELETE FROM CDS_AMT.{seg_df_tbl_name} WHERE ANL_DT < ADD_DAYS(TO_DATE('{target_date}','YYYY-MM-DD'),-29)  ;
    """)


    ## -----------------------------------------------
    ## 2.5.4. 고객 별 Micro-seg 최종 확정 작업
    ## -----------------------------------------------

    get_ipython().run_cell_magic('sql', '', """
    CREATE TABLE CDS_AMT.CUST_MICROSEG (
        anl_dt        date,
        cust_id       varchar(20),
        seg_1grp      varchar(4),
        seg_2grp      varchar(20),
        seg_3grp      varchar(20),
        seg_1grp_cnt  bigint,
        seg_2grp_cnt  bigint,
        seg_3grp_cnt  bigint,
        seg_final     varchar(20),
        seg_final_flag varchar(1)
    )
    """)

    start_time = add_log(module_nm = '상품추천', module_type_nm = '운영', 
                         step = '2.LDA모델 적용 및 추천결과 도출 : LOGIC 2', query_type = 'Create', 
                         target_table = 'CUST_MICROSEG', start_time = start_time)


    # In[ ]:


    get_ipython().run_cell_magic('sql', '', """
    INSERT INTO CDS_AMT.CUST_MICROSEG (
        WITH TMP AS (
            SELECT ANL_DT, CUST_ID, SEG_1GRP, SEG_2GRP, SEG_3GRP
                   ,DENSE_RANK() OVER (PARTITION BY CUST_ID ORDER BY ANL_DT DESC) AS CUST_RANK
              FROM CDS_AMT.{seg_df_tbl_name} 

             -- 몇 일간 데이터를 집계하여 MICRO-SEG 를 집계할 것인지 설정해야 하며,
             -- 설정된 기간에 맞춰 자동 KEY-IN 되도록 설정 
             -- ( 현재는 최근 7일로 설정되어 있음 )

             WHERE ANL_DT BETWEEN ADD_DAYS(TO_DATE('{target_date}','YYYY-MM-DD'),-6) AND '{target_date}' 
                   AND MODEL_VERSION = '{model_target_nm}' 

        ), 
        TMP2 AS (
            -- 해당 기간 중 가장 최신 데이터만 가져옴
            SELECT ANL_DT, CUST_ID, SEG_1GRP, SEG_2GRP, SEG_3GRP 
              FROM TMP 
             WHERE CUST_RANK = 1
        ) 

        -- ###########################################################################################
        -- ## MAIN PART

        SELECT   F.ANL_DT, F.CUST_ID
                ,F.SEG_1GRP, F.SEG_2GRP, F.SEG_3GRP
                ,F.SEG_1GRP_CNT, F.SEG_2GRP_CNT, F.SEG_3GRP_CNT

                -- 예외처리로직
                ,CASE 
                    -- RULE 1 : TOP2 TOPIC 만 존재하는 경우
                    WHEN (F.SEG_1GRP_MISSING = 0 AND F.SEG_2GRP_MISSING = 0 AND F.SEG_3GRP_MISSING = 1) 
                            AND F.SEG_2GRP_CNT > 4 THEN SEG_2GRP
                    WHEN (F.SEG_1GRP_MISSING = 0 AND F.SEG_2GRP_MISSING = 0 AND F.SEG_3GRP_MISSING = 1) 
                            AND F.SEG_2GRP_CNT < 4 AND F.SEG_1GRP_CNT > 4 THEN SEG_1GRP
                    WHEN (F.SEG_1GRP_MISSING = 0 AND F.SEG_2GRP_MISSING = 0 AND F.SEG_3GRP_MISSING = 1) 
                            AND F.SEG_2GRP_CNT < 4 AND F.SEG_1GRP_CNT < 4 THEN '9999@9999@9999'

                    -- RULE 2 : TOP1 TOPIC 만 존재하는 경우
                    WHEN (F.SEG_1GRP_MISSING = 0 AND F.SEG_2GRP_MISSING = 1 AND F.SEG_3GRP_MISSING = 1) 
                            AND F.SEG_1GRP_CNT > 4 THEN SEG_1GRP
                    WHEN (F.SEG_1GRP_MISSING = 0 AND F.SEG_2GRP_MISSING = 1 AND F.SEG_3GRP_MISSING = 1) 
                            AND F.SEG_1GRP_CNT < 4 THEN '9999@9999@9999'

                    -- 나머지는 SEG_FINAL 로 처리
                    ELSE F.SEG_FINAL END AS SEG_FINAL

                ,CASE 
                    -- RULE 1 : TOP2 TOPIC 만 존재하는 경우
                    WHEN (F.SEG_1GRP_MISSING = 0 AND F.SEG_2GRP_MISSING = 0 AND F.SEG_3GRP_MISSING = 1) 
                            AND F.SEG_2GRP_CNT > 4 THEN '2'
                    WHEN (F.SEG_1GRP_MISSING = 0 AND F.SEG_2GRP_MISSING = 0 AND F.SEG_3GRP_MISSING = 1) 
                            AND F.SEG_2GRP_CNT < 4 AND F.SEG_1GRP_CNT > 4 THEN '1'
                    WHEN (F.SEG_1GRP_MISSING = 0 AND F.SEG_2GRP_MISSING = 0 AND F.SEG_3GRP_MISSING = 1) 
                            AND F.SEG_2GRP_CNT < 4 AND F.SEG_1GRP_CNT < 4 THEN '9'

                    -- RULE 2 : TOP1 TOPIC 만 존재하는 경우
                    WHEN (F.SEG_1GRP_MISSING = 0 AND F.SEG_2GRP_MISSING = 1 AND F.SEG_3GRP_MISSING = 1) 
                            AND F.SEG_1GRP_CNT > 4 THEN '1'
                    WHEN (F.SEG_1GRP_MISSING = 0 AND F.SEG_2GRP_MISSING = 1 AND F.SEG_3GRP_MISSING = 1) 
                            AND F.SEG_1GRP_CNT < 4 THEN '9'

                    -- 나머지는 SEG_FINAL_FLAG 로 처리
                    WHEN F.SEG_FINAL = '9999@9999@9999' THEN '9'
                    ELSE F.SEG_FINAL_FLAG END AS SEG_FINAL_FLAG

        -- ###########################################################################################

        FROM (

            SELECT  A.ANL_DT, A.CUST_ID, A.SEG_1GRP, A.SEG_2GRP, A.SEG_3GRP
                    ,B.SEG_1GRP_CNT, C.SEG_2GRP_CNT, D.SEG_3GRP_CNT
                    ,CASE WHEN D.SEG_3GRP_CNT > 4 THEN A.SEG_3GRP
                          WHEN C.SEG_2GRP_CNT > 4 THEN A.SEG_2GRP
                          WHEN B.SEG_1GRP_CNT > 4 THEN A.SEG_1GRP
                          ELSE '9999@9999@9999' END AS SEG_FINAL
                    ,CASE WHEN D.SEG_3GRP_CNT > 4 THEN '3'
                          WHEN C.SEG_2GRP_CNT > 4 THEN '2'
                          WHEN B.SEG_1GRP_CNT > 4 THEN '1'
                          ELSE '9' END AS SEG_FINAL_FLAG
                    ,CASE WHEN A.SEG_3GRP LIKE '%@9999%' THEN 1 ELSE 0 END SEG_3GRP_MISSING
                    ,CASE WHEN A.SEG_2GRP LIKE '%@9999%' THEN 1 ELSE 0 END SEG_2GRP_MISSING
                    ,CASE WHEN A.SEG_1GRP LIKE '%9999%' THEN 1 ELSE 0 END SEG_1GRP_MISSING
              FROM  TMP2 A

            LEFT JOIN (
                SELECT SEG_1GRP, COUNT(DISTINCT CUST_ID) AS SEG_1GRP_CNT
                  FROM TMP2 -- CDS_AMT.CUST_MICROSEG_TMP
              GROUP BY SEG_1GRP
            ) B
                ON A.SEG_1GRP = B.SEG_1GRP

            LEFT JOIN (
                SELECT SEG_2GRP, COUNT(DISTINCT CUST_ID) AS SEG_2GRP_CNT
                  FROM TMP2 -- CDS_AMT.CUST_MICROSEG_TMP 
              GROUP BY SEG_2GRP
            ) C
                ON A.SEG_2GRP = C.SEG_2GRP

            LEFT JOIN (
                SELECT SEG_3GRP, COUNT(DISTINCT CUST_ID) AS SEG_3GRP_CNT
                  FROM TMP2 -- CDS_AMT.CUST_MICROSEG_TMP 
              GROUP BY SEG_3GRP
            ) D
                ON A.SEG_3GRP = D.SEG_3GRP 
        ) F
    )
    """)

    start_time = add_log(module_nm = '상품추천', module_type_nm = '운영', 
                     step = '2.LDA모델 적용 및 추천결과 도출 : LOGIC 2', query_type = 'Insert', 
                     target_table = 'CUST_MICROSEG', start_time = start_time)


    ## -----------------------------------------------
    ## 2.5.5. Micro-seg 별 장바구니 집계 작업
    ## -- 고객 구매이력 데이터 (RECMD_TMPR) x MICROSEG 결합
    ## -----------------------------------------------

    get_ipython().run_cell_magic('sql', '', """
    CREATE TABLE CDS_AMT.CUST_RECMD_MICROSEG_BASE_RAW (
        cust_id       varchar(20),
        prdt_cat_cd   varchar(10),
        prdt_dcode_cd varchar(10),
        anl_dt        date,
        dt_cnt        bigint,
        sum_qty       decimal(18,2),
        sum_amt       decimal(18,2),
        prdt_dcode_cd_unit_prc       decimal(18,2),
        seg_1grp      varchar(4),
        seg_2grp      varchar(20),
        seg_3grp      varchar(20),
        seg_1grp_cnt  bigint,
        seg_2grp_cnt  bigint,
        seg_3grp_cnt  bigint,
        seg_final     varchar(20),
        seg_final_flag varchar(1)
    ) ;

    """)

    start_time = add_log(module_nm = '상품추천', module_type_nm = '운영', 
                         step = '2.LDA모델 적용 및 추천결과 도출 : LOGIC 2', query_type = 'Create', 
                         target_table = 'CUST_RECMD_MICROSEG_BASE_RAW', start_time = start_time)




    get_ipython().run_cell_magic('sql', '', """
    INSERT INTO CDS_AMT.CUST_RECMD_MICROSEG_BASE_RAW (

        -- 고객 영수증 데이터와 INNER JOIN 하여 MICROSEG 별 장바구니 집계를 위한 DATASET 를 생성함

        -- 고객들의 90일 치 구매 이력 집계 데이터 
        SELECT  A.CUST_ID, A.PRDT_CAT_CD, A.PRDT_DCODE_CD , A.ANL_DT, A.DT_CNT, A.SUM_QTY, A.SUM_AMT
                ,A.SUM_AMT / A.SUM_QTY AS PRDT_DCODE_CD_UNIT_PRC
                ,B.SEG_1GRP, B.SEG_2GRP, B.SEG_3GRP
                ,B.SEG_1GRP_CNT, B.SEG_2GRP_CNT, B.SEG_3GRP_CNT
                ,B.SEG_FINAL, B.SEG_FINAL_FLAG
        FROM CDS_AMT.TB_AMT_RECMD_TMPR A

        -- 최근 7 일간 구매이력이 있는 고객번호 중 최신 이력만 INNER JOIN 필터링
        INNER JOIN CDS_AMT.CUST_MICROSEG B
            ON A.CUST_ID = B.CUST_ID AND A.ANL_DT = B.ANL_DT
    )

    """)

    start_time = add_log(module_nm = '상품추천', module_type_nm = '운영', 
                     step = '2.LDA모델 적용 및 추천결과 도출 : LOGIC 2', query_type = 'Insert', 
                     target_table = 'CUST_RECMD_MICROSEG_BASE_RAW', start_time = start_time)


    ## -----------------------------------------------
    ## 2.5.6. MICROSEG 별 장바구니 집계 데이터 생성
    ## - 여기서 MICROSEG 별로 DT_CNT = 1 인 품목은 기본적으로 제외
    ## -----------------------------------------------

    get_ipython().run_cell_magic('sql', '', """
    CREATE TABLE CDS_AMT.CUST_RECMD_MICROSEG_BASKET (
        ANL_DT                  date,
        SEG_FINAL               varchar(20),
        PRDT_CAT_CD             varchar(10),
        PRDT_DCODE_CD           varchar(10),
        PRDT_DCODE_CD_UNIT_PRC  decimal(18,10),
        DT_CNT_BASKET           bigint,
        SEG_CNT                 bigint,
        DT_CNT_RT_BASKET        decimal(18,10),
        BASKET_RANK             bigint,
        DT_CNT_ALL              bigint,
        CUST_CNT_ALL            bigint,
        DT_CNT_RT_ALL           decimal(18,10),
        LIFT                    decimal(18,10),
        LIFT_RANK               bigint,
        LIFT_FLT_OVER_Q4        decimal(18,10),
        LIFT_RANK_FLT_OVER_Q4   bigint
    )

    """)

    start_time = add_log(module_nm = '상품추천', module_type_nm = '운영', 
                     step = '2.LDA모델 적용 및 추천결과 도출 : LOGIC 2', query_type = 'Create', 
                     target_table = 'CUST_RECMD_MICROSEG_BASKET', start_time = start_time)




    get_ipython().run_cell_magic('sql', '', """
    INSERT INTO CDS_AMT.CUST_RECMD_MICROSEG_BASKET (

        /*  
        배치일자 포함 7일간 구매이력이 발생한 고객들의 90일치 구매 이력을
        MICRO-SEGMENT 별로 집계하여 저장함

        아래 상품 조건에 해당하는 경우 추천대상에서 제외
        1. MICROSEG 내 구매 이력 중 구매 건수 = 1 인 PRDT_DCODE_CD 는 제외
        2. MICROSEG 의 고객 수(SEG SIZE) 가 50 명 이상인 경우 구매건수 < 5 미만 상품은 제외
        --> 2번의 요건은 PRDT_DCODE_MINIMUM_SEG_COUNT 와 PURCHASE_PRDT_DCODE_MINIMUM_COUNT 로 잡혀 있으며, 추후 수정 필요 시 여기서 수정 
        */

        WITH TMP AS (

            SELECT
                SEG_3GRP AS SEG_FINAL
                ,PRDT_CAT_CD, PRDT_DCODE_CD 
                ,SUM(DT_CNT) AS DT_CNT
                ,MAX(SEG_3GRP_CNT) AS SEG_CNT
                ,AVG(PRDT_DCODE_CD_UNIT_PRC) AS PRDT_DCODE_CD_UNIT_PRC 
                ,SUM(DT_CNT) / MAX(SEG_3GRP_CNT) AS DT_CNT_RT 
            FROM CDS_AMT.CUST_RECMD_MICROSEG_BASE_RAW
            GROUP BY SEG_3GRP, PRDT_CAT_CD, PRDT_DCODE_CD

        UNION ALL
            SELECT 
                SEG_2GRP AS SEG_FINAL
                ,PRDT_CAT_CD, PRDT_DCODE_CD 
                ,SUM(DT_CNT) AS DT_CNT
                ,MAX(SEG_2GRP_CNT) AS SEG_CNT
                ,AVG(PRDT_DCODE_CD_UNIT_PRC) AS PRDT_DCODE_CD_UNIT_PRC 
                ,SUM(DT_CNT) / MAX(SEG_2GRP_CNT) AS DT_CNT_RT 
            FROM CDS_AMT.CUST_RECMD_MICROSEG_BASE_RAW
            GROUP BY SEG_2GRP, PRDT_CAT_CD, PRDT_DCODE_CD

        UNION ALL
            SELECT
                SEG_1GRP AS SEG_FINAL
                ,PRDT_CAT_CD, PRDT_DCODE_CD 
                ,SUM(DT_CNT) AS DT_CNT
                ,MAX(SEG_1GRP_CNT) AS SEG_CNT
                ,AVG(PRDT_DCODE_CD_UNIT_PRC) AS PRDT_DCODE_CD_UNIT_PRC 
                ,SUM(DT_CNT) / MAX(SEG_1GRP_CNT) AS DT_CNT_RT 
            FROM CDS_AMT.CUST_RECMD_MICROSEG_BASE_RAW
            GROUP BY SEG_1GRP, PRDT_CAT_CD, PRDT_DCODE_CD

        UNION ALL
            SELECT 
                'ALL' AS SEG_FINAL
                ,A.PRDT_CAT_CD, A.PRDT_DCODE_CD 
                ,SUM(A.DT_CNT) AS DT_CNT
                ,MAX(B.ALL_CNT) AS SEG_CNT
                ,AVG(A.PRDT_DCODE_CD_UNIT_PRC) AS PRDT_DCODE_CD_UNIT_PRC
                ,SUM(A.DT_CNT) / MAX(B.ALL_CNT) AS DT_CNT_RT
            FROM CDS_AMT.CUST_RECMD_MICROSEG_BASE_RAW A,
                 (SELECT COUNT(DISTINCT CUST_ID) AS ALL_CNT FROM CDS_AMT.CUST_RECMD_MICROSEG_BASE_RAW) B 
            GROUP BY PRDT_CAT_CD, PRDT_DCODE_CD 
        ),
        TMP2 AS (
            SELECT COUNT(DISTINCT CUST_ID) AS ALL_CNT FROM CDS_AMT.CUST_RECMD_MICROSEG_BASE_RAW
        )

        SELECT  '{target_date}' as ANL_DT
                ,A.SEG_FINAL
                ,A.PRDT_CAT_CD, A.PRDT_DCODE_CD --, C.PRDT_DCODE_NM
                ,A.PRDT_DCODE_CD_UNIT_PRC
                ,A.DT_CNT_BASKET
                ,A.SEG_CNT
                ,A.DT_CNT_RT_BASKET
                ,A.BASKET_RANK
                ,B.DT_CNT_ALL
                ,B.CUST_CNT_ALL
                ,B.DT_CNT_RT_ALL
                ,A.DT_CNT_RT_BASKET * (SELECT ALL_CNT FROM TMP2) / B.DT_CNT_ALL AS LIFT
                ,DENSE_RANK() OVER (PARTITION BY SEG_FINAL ORDER BY A.DT_CNT_RT_BASKET * (SELECT ALL_CNT FROM TMP2) / B.DT_CNT_ALL DESC ) AS LIFT_RANK

                /* MICROSEG 보삽 기준 : 
                    MICROSEG 고객 수 기준 4분위수 이상인 경우 (= 50)
                    -->  DT_CNT_BASKET (품목별 구매일수-DT_CNT) < 5 미만인 경우 0 처리 */

                ,CASE WHEN A.SEG_CNT > {PRDT_DCODE_MINIMUM_SEG_COUNT} AND A.DT_CNT_BASKET < {PRDT_DCODE_MINIMUM_PURCHASE_COUNT} THEN 0 
                      ELSE A.DT_CNT_RT_BASKET * (SELECT ALL_CNT FROM TMP2) / B.DT_CNT_ALL END AS LIFT_FLT_OVER_Q4
                ,DENSE_RANK() OVER (PARTITION BY SEG_FINAL ORDER BY 
                                    (CASE WHEN A.SEG_CNT > {PRDT_DCODE_MINIMUM_SEG_COUNT} AND A.DT_CNT_BASKET < {PRDT_DCODE_MINIMUM_PURCHASE_COUNT} THEN 0 
                                          ELSE A.DT_CNT_RT_BASKET * (SELECT ALL_CNT FROM TMP2) / B.DT_CNT_ALL END) DESC) AS LIFT_RANK_FLT_OVER_Q4

        FROM (
            SELECT SEG_FINAL
                   ,PRDT_CAT_CD, PRDT_DCODE_CD
                   ,DT_CNT AS DT_CNT_BASKET
                   ,SEG_CNT
                   ,DT_CNT_RT AS DT_CNT_RT_BASKET
                   ,PRDT_DCODE_CD_UNIT_PRC
                   ,DENSE_RANK() OVER (PARTITION BY SEG_FINAL ORDER BY DT_CNT_RT DESC) AS BASKET_RANK
              FROM TMP
             WHERE DT_CNT > 1 -- MICROSEG 내 구매 이력 중 구매이력이 1 인 PRDT_DCODE_CD 는 제외
          ORDER BY SEG_FINAL, PRDT_CAT_CD, PRDT_DCODE_CD ASC ) A

        -- 전체 장바구니 구매 내역을 붙여 LIFT 계산
        LEFT JOIN (
            SELECT  PRDT_CAT_CD, PRDT_DCODE_CD
                    ,DT_CNT AS DT_CNT_ALL
                    ,SEG_CNT AS CUST_CNT_ALL
                    ,DT_CNT_RT AS DT_CNT_RT_ALL
              FROM  TMP
             WHERE  SEG_FINAL = 'ALL'
        ) B ON A.PRDT_CAT_CD = B.PRDT_CAT_CD 
            AND CAST(A.PRDT_DCODE_CD AS INTEGER) = CAST(B.PRDT_DCODE_CD AS INTEGER)
    )

    """)

    start_time = add_log(module_nm = '상품추천', module_type_nm = '운영', 
                     step = '2.LDA모델 적용 및 추천결과 도출 : LOGIC 2', query_type = 'Insert', 
                     target_table = 'CUST_RECMD_MICROSEG_BASKET', start_time = start_time)


    
    ## -----------------------------------------------
    ## 2.5.7. 고객별 Micro-seg 별 장바구니 결합 & 상품추천 작업 
    ## -----------------------------------------------
    
    # 모든 데이터 결합하여 micro-seg 별 상품추천 리스트 완성
    # - 시작 베이스 : 고객 별 microsegment 리스트 (CDS_AMT.CUST_RECMD_MICROSEG_BASE_RAW) - 당일 ANL_DT 기준
    #   > STEP 1. [LEFT JOIN] micro-segment 별 장바구니 집계 & 랭킹 리스트 (CDS_AMT.RECMD_MICROSEG_BASKET)    
    #   
    #   > STEP 2. [LEFT JOIN] 고객별 구매 집계 리스트 (CDS_AMT.TB_AMT_RECMD_TMPR) - 당일 ANL_DT 기준
    ## -----------------------------------------------

    start = time.time()

    
    
    ## 배치 로직 실행 전 테이블 Drop & Create

    tbl_name = 'TB_AMT_RECMD_LOGIC2_MICROSEG_RESULT'
    db_schema_list, db_table_list = schema_inspector()

    if tbl_name.lower() in db_table_list : 
        get_ipython().run_cell_magic('sql', '', """DROP TABLE {DB_ID}.{tbl_name} ; """)

    else : 
        pass

    get_ipython().run_cell_magic('sql', '', """
    CREATE TABLE CDS_AMT.TB_AMT_RECMD_LOGIC2_MICROSEG_RESULT (
        ANL_DT                  date,             -- 분석 일자
        CUST_ID                 nvarchar(20),     -- 고객 번호
        SEG_1GRP                nvarchar(4),      -- TOP 1 TOPIC 으로 만든 MICRO-SEG
        SEG_2GRP                nvarchar(20),     -- TOP 2 TOPICS 로 만든 MICRO-SEG
        SEG_3GRP                nvarchar(20),     -- TOP 3 TOPICS 로 만든 MICRO-SEG
        SEG_1GRP_CNT            decimal(20),      -- SEG_1GRP 에 해당하는 고객 수
        SEG_2GRP_CNT            decimal(20),      -- SEG_2GRP 에 해당하는 고객 수
        SEG_3GRP_CNT            decimal(10),      -- SEG_3GRP 에 해당하는 고객 수
        SEG_FINAL               nvarchar(20),     -- 고객 별 최종 MICRO-SEG
        SEG_FINAL_FLAG          nvarchar(9),      -- MICRO-SEG 종류에 해당하는 FLAG (3: SEG_3GRP / 2: SEG_2GRP / 1: SEG_1GRP)
        PRDT_CAT_CD             nvarchar(10),     -- 상품 카테고리
        PRDT_DCODE_CD           nvarchar(10),     -- 상품 소분류
        PRDT_DCODE_NM           nvarchar(100),    -- 상품 소분류 이름
        PRDT_DCODE_CD_UNIT_PRC  decimal(18,10),   -- 상품 소분류 평균 가격
        DT_CNT                  decimal(20),      -- 해당 고객 별 상품 구매 횟수 (1일 MAX 1회)
        DT_CNT_BASKET           decimal(20),      -- MICRO-SEG 별 상품 구매 횟수
        SEG_CNT                 decimal(20),      -- 해당 MICRO-SEG 의 고객 수 (SEG_1/2/3_GRP_CNT 와 동일)
        DT_CNT_RT_BASKET        decimal(20,10),   -- MICRO-SEG 장바구니 비중 ( = DT_CNT_BASKET / SEG_CNT 로 산출된 비율)
        BASKET_RANK             decimal(10),      -- MICRO-SEG 장바구니 비중으로 산출된 랭킹 (내림차순)
        DT_CNT_ALL              decimal(20),      -- 전체 고객의 상품 구매 횟수
        CUST_CNT_ALL            decimal(20),      -- 최근7일간 고객 총 수 (= COUNT(DISTINCT CUST_ID))
        DT_CNT_RT_ALL           decimal(20,10),   -- 전체 고객의 장바구니 비중
        LIFT                    decimal(20,10),   -- 전체 고객의 장바구니 비중 대비 MICRO-SEG 장바구니 비중 LIFT
        LIFT_RANK               decimal(10),      -- LIFT 로 산출된 랭킹 (내림차순)
        LIFT_FLT_OVER_Q4        decimal(20,10),
        LIFT_RANK_FLT_OVER_Q4   decimal(10)
    )

    """)

    start_time = add_log(module_nm = '상품추천', module_type_nm = '운영', 
                     step = '2.LDA모델 적용 및 추천결과 도출 : LOGIC 2', query_type = 'Create', 
                     target_table = 'TB_AMT_RECMD_LOGIC2_MICROSEG_RESULT', start_time = start_time)


    ## Logic 2 결과 Insert

    get_ipython().run_cell_magic('sql', '', """
    INSERT INTO CDS_AMT.TB_AMT_RECMD_LOGIC2_MICROSEG_RESULT (

        SELECT  LA.ANL_DT                   -- 분석 일자
                ,LA.CUST_ID                 -- 고객 번호
                ,LA.SEG_1GRP                -- TOP 1 TOPIC 으로 만든 MICRO-SEG
                ,LA.SEG_2GRP                -- TOP 2 TOPICS 로 만든 MICRO-SEG
                ,LA.SEG_3GRP                -- TOP 3 TOPICS 로 만든 MICRO-SEG
                ,LA.SEG_1GRP_CNT            -- SEG_1GRP 에 해당하는 고객 수
                ,LA.SEG_2GRP_CNT            -- SEG_2GRP 에 해당하는 고객 수
                ,LA.SEG_3GRP_CNT            -- SEG_3GRP 에 해당하는 고객 수
                ,LA.SEG_FINAL               -- 고객 별 최종 MICRO-SEG
                ,LA.SEG_FINAL_FLAG          -- MICRO-SEG 종류에 해당하는 FLAG (3: SEG_3GRP / 2: SEG_2GRP / 1: SEG_1GRP)

                ,COALESCE(LA.PRDT_CAT_CD, LB.PRDT_CAT_CD) AS PRDT_CAT_CD             -- 상품 카테고리
                ,COALESCE(LA.PRDT_DCODE_CD, LB.PRDT_DCODE_CD) AS PRDT_DCODE_CD       -- 상품 소분류 코드
                ,LC.PRDT_DCODE_NM           -- 상품 소분류 이름
                ,LA.PRDT_DCODE_CD_UNIT_PRC  -- 상품 소분류 평균 가격
                ,LB.DT_CNT                  -- 해당 고객 별 상품 구매 횟수 (1일 MAX 1회)
                ,LA.DT_CNT_BASKET           -- MICRO-SEG 별 상품 구매 횟수
                ,LA.SEG_CNT                 -- 해당 MICRO-SEG 의 고객 수 (SEG_1/2/3_GRP_CNT 와 동일)
                ,LA.DT_CNT_RT_BASKET        -- MICRO-SEG 장바구니 비중 ( = DT_CNT_BASKET / SEG_CNT 로 산출된 비율)
                ,LA.BASKET_RANK             -- MICRO-SEG 장바구니 비중으로 산출된 랭킹 (내림차순)
                ,LA.DT_CNT_ALL              -- 전체 고객의 상품 구매 횟수
                ,LA.CUST_CNT_ALL            -- 최근7일간 고객 총 수 (= COUNT(DISTINCT CUST_ID))
                ,LA.DT_CNT_RT_ALL           -- 전체 고객의 장바구니 비중
                ,LA.LIFT                    -- 전체 고객의 장바구니 비중 대비 MICRO-SEG 장바구니 비중 LIFT
                ,LA.LIFT_RANK               -- LIFT 로 산출된 랭킹 (내림차순)
                ,LA.LIFT_FLT_OVER_Q4        -- [LIFT 보정] SEG_SIZE >= 50 이상 MICRO-SEG 대상 구매건수 5 미만 품목 제외한 LIFT 값
                ,LA.LIFT_RANK_FLT_OVER_Q4   -- [LIFT 보정] LIFT_FLT_OVER_Q4 기준 LIFT_RANK

        FROM (
            SELECT  SA.ANL_DT ,SA.CUST_ID ,SA.SEG_1GRP ,SA.SEG_2GRP ,SA.SEG_3GRP
                   ,SA.SEG_1GRP_CNT ,SA.SEG_2GRP_CNT ,SA.SEG_3GRP_CNT
                   ,SA.SEG_FINAL ,SA.SEG_FINAL_FLAG

                   ,SB.PRDT_CAT_CD ,SB.PRDT_DCODE_CD 
                   ,SB.PRDT_DCODE_CD_UNIT_PRC 
                   ,SB.DT_CNT_BASKET
                   ,SB.SEG_CNT ,SB.DT_CNT_RT_BASKET ,SB.BASKET_RANK
                   ,SB.DT_CNT_ALL ,SB.CUST_CNT_ALL ,SB.DT_CNT_RT_ALL
                   ,SB.LIFT ,SB.LIFT_RANK
                   ,SB.LIFT_FLT_OVER_Q4, SB.LIFT_RANK_FLT_OVER_Q4

              -- TARGET_DATE 에 맞춰 해당 날짜의 CUST_ID 및 MICROSEG ID 를 끌고 옴
              FROM (SELECT ANL_DT ,CUST_ID ,SEG_1GRP ,SEG_2GRP ,SEG_3GRP ,SEG_1GRP_CNT ,SEG_2GRP_CNT ,SEG_3GRP_CNT 
                          ,SEG_FINAL ,SEG_FINAL_FLAG
                      FROM CDS_AMT.CUST_MICROSEG  
                     WHERE ANL_DT = '{target_date}' 
                   ) SA

              LEFT 
              JOIN CDS_AMT.CUST_RECMD_MICROSEG_BASKET SB
                ON SA.SEG_FINAL = SB.SEG_FINAL 

        ) LA

        -- 고객의 구매/미구매이력과 상품추천 결과와의 비교를 위해 가져옴
        FULL OUTER JOIN (
               SELECT CUST_ID, ANL_DT, PRDT_CAT_CD, PRDT_DCODE_CD, DT_CNT 
                 FROM CDS_AMT.TB_AMT_RECMD_TMPR
                WHERE ANL_DT = '{target_date}' ) LB 

          ON  LA.CUST_ID = LB.CUST_ID 
                AND CAST(LA.PRDT_CAT_CD AS INTEGER) = CAST(LB.PRDT_CAT_CD AS INTEGER)
                AND CAST(LA.PRDT_DCODE_CD AS INTEGER) = CAST(LB.PRDT_DCODE_CD AS INTEGER)
                AND LA.ANL_DT = LB.ANL_DT 

        -- 상품 소분류 마스터 조인
        LEFT JOIN (
            SELECT DISTINCT PRDT_CAT_CD, PRDT_DCODE_CD, PRDT_DCODE_NM 
              FROM CDS_DW.TB_DW_PRDT_DCODE_CD
             WHERE AFLCO_CD = '001' AND BIZTP_CD = '10' AND PRDT_DCODE_CD IS NOT NULL 
        ) LC 
            ON CAST(COALESCE(LA.PRDT_DCODE_CD, LB.PRDT_DCODE_CD) AS INTEGER)  = CAST(LC.PRDT_DCODE_CD AS INTEGER) 

        WHERE (LA.LIFT_RANK <= 100) 
              OR (LA.BASKET_RANK <= 100) 
              OR (LA.LIFT_RANK_FLT_OVER_Q4 <= 100)
              OR (LB.DT_CNT IS NOT NULL)

    )

    """)

    ## Print progress 
    print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
          " || [Logic 2 : Micro-segment] calculation & DB Insert task are completed.",
          "(elapsed time :", np.round((time.time() - start) / 60, 2), "mins)")

    start_time = add_log(module_nm = '상품추천', module_type_nm = '운영', 
                     step = '2.LDA모델 적용 및 추천결과 도출 : LOGIC 2', query_type = 'Insert', 
                     target_table = 'TB_AMT_RECMD_LOGIC2_MICROSEG_RESULT', start_time = start_time)


    # ====================================================
    # 2.8. 데이터 최종 결합 및 결과테이블 생성 
    # ====================================================

    # ----------------------------------------------------
    ## 2.6.1. 테이블 결합 및 MRR 랭킹 생성
    # ----------------------------------------------------
    
    get_ipython().run_cell_magic('sql', '', """
    CREATE TABLE CDS_AMT.RECMD_RANK_TMP (
        anl_dt                    date, 
        cust_id                   nvarchar(20),
        prdt_dcode_cd             nvarchar(10),
        prdt_dcode_nm             nvarchar(100),
        dt_cnt                    decimal(20),
        recmd_flag                nvarchar(100),
        prob_wt                   decimal(18,10),
        prob_wt_rank              decimal(10),
        lift_top10_topic          decimal(20,10),
        lift_rank_top10_topic     decimal(20,10),
        dt_cnt_rt_microseg_basket decimal(20,10),
        rank_microseg_basket      decimal(10),
        lift_microseg_basket      decimal(20,10),
        lift_rank_microseg_basket decimal(10)
    ) ;

    """)

    start_time = add_log(module_nm = '상품추천', module_type_nm = '운영', 
                     step = '3-0.최종 결합테이블 생성', query_type = 'Create', 
                     target_table = 'RECMD_RANK_TMP', start_time = start_time)




    get_ipython().run_cell_magic('sql', '', """
    INSERT INTO CDS_AMT.RECMD_RANK_TMP (

        SELECT   COALESCE(D.ANL_DT,C.ANL_DT) AS ANL_DT
                ,COALESCE(D.CUST_ID, C.CUST_ID) AS CUST_ID
                ,LPAD(CAST(COALESCE(D.PRDT_DCODE_CD, C.PRDT_DCODE_CD) AS VARCHAR),4,'0') AS PRDT_DCODE_CD
                ,COALESCE(D.PRDT_DCODE_NM, C.PRDT_DCODE_NM) AS PRDT_DCODE_NM
                ,D.DT_CNT

                /* ## 21.01.06 UPDATES  
                -- 침투율 높은 상품은 FLAG 만들지 말고 
                -- 미구매상품 (CROSS-SELL 대상 상품) / 기구매상품 (RE-SELL 대상 상품) 2가지로만 구분
                -- 추천제외상품은 맨 마지막 단계에서 필터링 하기로 함 */

                ,CASE WHEN D.DT_CNT IS NULL THEN 'CROSS-SELL PRDT'
                      WHEN D.DT_CNT IS NOT NULL THEN 'RE-SELL PRDT' END AS RECMD_FLAG

                ,C.PROB_WT, C.PROB_WT_RANK
                ,C.LIFT_TOP10 AS LIFT_TOP10_TOPIC, C.LIFT_RANK_TOP10 AS LIFT_RANK_TOP10_TOPIC

                ,D.DT_CNT_RT_BASKET AS DT_CNT_RT_MICROSEG_BASKET, D.BASKET_RANK AS RANK_MICROSEG_BASKET 
                ,D.LIFT_FLT_OVER_Q4 AS LIFT_MICROSEG_BASKET, D.LIFT_RANK_FLT_OVER_Q4 AS LIFT_RANK_MICROSEG_BASKET

        FROM (
            SELECT  ANL_DT, CUST_ID, PRDT_DCODE_CD, PRDT_DCODE_NM, DT_CNT
                   ,PROB_WT, PROB_WT_RANK
                   ,LIFT_TOP10, LIFT_RANK_TOP10

              FROM  CDS_AMT.TB_AMT_RECMD_LOGIC1_PROBWT_RESULT ) C

        FULL OUTER JOIN (
            SELECT   ANL_DT, CUST_ID, SEG_FINAL, PRDT_DCODE_CD, PRDT_DCODE_NM, DT_CNT
                    ,DT_CNT_RT_BASKET, BASKET_RANK
                    ,LIFT_FLT_OVER_Q4, LIFT_RANK_FLT_OVER_Q4

              FROM  CDS_AMT.TB_AMT_RECMD_LOGIC2_MICROSEG_RESULT ) D -- CDS_AMT.RECMD_LOGIC2_MICROSEG_CMPR_RESULT_REV
            ON C.ANL_DT = D.ANL_DT 
                AND C.CUST_ID = D.CUST_ID 
                AND CAST(C.PRDT_DCODE_CD AS INTEGER) = CAST(D.PRDT_DCODE_CD AS INTEGER)
    )
    """)

    start_time = add_log(module_nm = '상품추천', module_type_nm = '운영', 
                     step = '3-0.최종 결합테이블 생성', query_type = 'Insert', 
                     target_table = 'RECMD_RANK_TMP', start_time = start_time)




    ## ----------------------------------------------------
    ## 2.6.2. 상품 소분류 단위 최종 결과 테이블 생성
    ## ----------------------------------------------------
    
    ## 주요 테이블은 배치 로직 마지막 단계에서 Drop & Create
    
    tbl_name = 'RECMD_RANK_DAILY'
    db_schema_list, db_table_list = schema_inspector()

    if tbl_name.lower() in db_table_list : 
        get_ipython().run_cell_magic('sql', '', """DROP TABLE {DB_ID}.{tbl_name} ; """)

    else : 
        pass

    get_ipython().run_cell_magic('sql', '', """
    CREATE TABLE CDS_AMT.RECMD_RANK_DAILY (
        anl_dt                    date, 
        cust_id                   nvarchar(20),
        prdt_dcode_cd             nvarchar(10),
        prdt_dcode_nm             nvarchar(100),
        dt_cnt                    decimal(20),
        recmd_flag                nvarchar(100),
        prob_wt                   decimal(18,17),
        prob_wt_rank              decimal(10),
        lift_top10_topic          decimal(20,10),
        lift_rank_top10_topic     decimal(10),
        dt_cnt_rt_microseg_basket decimal(20,10),
        rank_microseg_basket      decimal(10),
        lift_microseg_basket      decimal(28,10),
        lift_rank_microseg_basket decimal(10),
        mrr                       decimal(18,10),
        mrr_rank                  decimal(10)
    ) ;

    """)

    start_time = add_log(module_nm = '상품추천', module_type_nm = '운영', 
                     step = '3-0.최종 결합테이블 생성', query_type = 'Create', 
                     target_table = 'RECMD_RANK_DAILY', start_time = start_time)


    ## 데이터 Insert 진행

    get_ipython().run_cell_magic('sql', '', """
    INSERT INTO CDS_AMT.RECMD_RANK_DAILY (
        WITH TMP AS (
            -- 랭킹 빈칸 보삽 진행 (일괄적으로 랭킹 9999 위로 지정)
            SELECT  ANL_DT, CUST_ID, PRDT_DCODE_CD, PRDT_DCODE_NM, DT_CNT, RECMD_FLAG

                    ,PROB_WT
                    ,COALESCE(PROB_WT_RANK, 9999) AS PROB_WT_RANK

                    ,LIFT_TOP10_TOPIC
                    ,COALESCE(LIFT_RANK_TOP10_TOPIC, 9999) AS LIFT_RANK_TOP10_TOPIC

                    ,DT_CNT_RT_MICROSEG_BASKET
                    ,COALESCE(RANK_MICROSEG_BASKET,9999) AS RANK_MICROSEG_BASKET

                    ,LIFT_MICROSEG_BASKET
                    ,COALESCE(LIFT_RANK_MICROSEG_BASKET, 9999) AS LIFT_RANK_MICROSEG_BASKET

            FROM  CDS_AMT.RECMD_RANK_TMP 
        ), 
        TMP2 AS (
            -- MRR 스코어와 RE-SELL/CR0SS-SELL FLAG 지정
            SELECT  ANL_DT, CUST_ID, PRDT_DCODE_CD, PRDT_DCODE_NM, DT_CNT, RECMD_FLAG
                    ,PROB_WT
                    ,PROB_WT_RANK
                    ,LIFT_TOP10_TOPIC
                    ,LIFT_RANK_TOP10_TOPIC
                    ,DT_CNT_RT_MICROSEG_BASKET
                    ,RANK_MICROSEG_BASKET
                    ,LIFT_MICROSEG_BASKET
                    ,LIFT_RANK_MICROSEG_BASKET

                    -- MRR 계산
                    ,0.5 * ( 1/PROB_WT_RANK + 1/RANK_MICROSEG_BASKET ) AS RS_MRR -- RE-SELL 랭킹 스코어
                    ,0.5 * ( 1/LIFT_RANK_TOP10_TOPIC + 1/LIFT_RANK_MICROSEG_BASKET ) AS CS_MRR -- CROSS-SELL 랭킹 스코어

                    -- MRR 랭킹 계산용 FLAG
                    ,CASE WHEN RECMD_FLAG = 'RE-SELL PRDT' THEN 1 ELSE 0 END AS RS_FLAG
                    ,CASE WHEN RECMD_FLAG = 'CROSS-SELL PRDT' THEN 1 ELSE 0 END AS CS_FLAG

            FROM TMP
        ), 
        TMP3 AS (
            -- RE-SELL / CROSS-SELL 랭킹 계산
            SELECT  ANL_DT, CUST_ID, PRDT_DCODE_CD, PRDT_DCODE_NM, DT_CNT, RECMD_FLAG
                    ,PROB_WT
                    ,PROB_WT_RANK
                    ,LIFT_TOP10_TOPIC
                    ,LIFT_RANK_TOP10_TOPIC
                    ,DT_CNT_RT_MICROSEG_BASKET
                    ,RANK_MICROSEG_BASKET
                    ,LIFT_MICROSEG_BASKET
                    ,LIFT_RANK_MICROSEG_BASKET

                    ,RS_MRR  -- RE-SELL 용 MRR 스코어
                    ,RS_FLAG -- RE-SELL 용 FLAG
                    ,DENSE_RANK() OVER (PARTITION BY ANL_DT, CUST_ID ORDER BY RS_FLAG * RS_MRR DESC ) AS RS_RANK

                    ,CS_MRR  -- CROSS-SELL 용 MRR 스코어
                    ,CS_FLAG -- CROSS-SELL 용 FLAG
                    ,DENSE_RANK() OVER (PARTITION BY ANL_DT, CUST_ID ORDER BY CS_FLAG * CS_MRR DESC ) AS CS_RANK

            FROM TMP2 
        )

        -- UNION 방식으로 TABLE APPEND 

        SELECT  ANL_DT, CUST_ID, PRDT_DCODE_CD, PRDT_DCODE_NM, DT_CNT, RECMD_FLAG
                    ,PROB_WT
                    ,PROB_WT_RANK
                    ,LIFT_TOP10_TOPIC
                    ,LIFT_RANK_TOP10_TOPIC
                    ,DT_CNT_RT_MICROSEG_BASKET
                    ,RANK_MICROSEG_BASKET
                    ,LIFT_MICROSEG_BASKET
                    ,LIFT_RANK_MICROSEG_BASKET
                    ,RS_MRR AS MRR       
                    ,RS_RANK AS MRR_RANK

        FROM   TMP3 A 
        WHERE  RECMD_FLAG = 'RE-SELL PRDT' 
        AND ANL_DT IS NOT NULL

        UNION ALL (
            SELECT  ANL_DT, CUST_ID, PRDT_DCODE_CD, PRDT_DCODE_NM, DT_CNT, RECMD_FLAG
                    ,PROB_WT
                    ,PROB_WT_RANK
                    ,LIFT_TOP10_TOPIC
                    ,LIFT_RANK_TOP10_TOPIC
                    ,DT_CNT_RT_MICROSEG_BASKET
                    ,RANK_MICROSEG_BASKET
                    ,LIFT_MICROSEG_BASKET
                    ,LIFT_RANK_MICROSEG_BASKET
                    ,CS_MRR AS MRR       
                    ,CS_RANK AS MRR_RANK

            FROM   TMP3 A 
            WHERE  RECMD_FLAG = 'CROSS-SELL PRDT' 
            AND ANL_DT IS NOT NULL
        )

    )
    """)

    start_time = add_log(module_nm = '상품추천', module_type_nm = '운영', 
                     step = '3-0.최종 결합테이블 생성', query_type = 'Insert', 
                     target_table = 'RECMD_RANK_DAILY', start_time = start_time)


    ## ====================================================
    ## 2.7. 예외처리 고객 정제
    ## ====================================================
    ## 미분류 고객들 추출
    ## res_df1 : Logic1 에서의 예외 처리 고객 
    ## cust_id_remove : 상품마스터 미존재 상품만 구매한 고객
    ## res_df2 : Logic2 에서의 예외 처리 고객
    ## ====================================================
    
    ## ----------------------------------------------------
    ## 2.7.1. 예외처리 대상 고객 추출 집계
    ## ----------------------------------------------------

    res_df2 = get_ipython().run_line_magic('sql', "SELECT DISTINCT CUST_ID FROM CDS_AMT.CUST_MICROSEG WHERE SEG_FINAL_FLAG = '9'")
    res_df2_list = res_df2.DataFrame()['cust_id'].tolist()

    cust_repair_list = list(set(res_df1) | set(cust_id_remove) | set(res_df2_list))
    cust_repair_list_str = str(cust_repair_list).replace(']','').replace('[','')

    excl_cust_df = pd.DataFrame(cust_repair_list, columns = ['cust_id'])
    excl_cust_df['anl_dt'] = target_date

    print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
          f" || n={len(cust_repair_list)} customers will be applied to the exception handling logic.")

    ## ----------------------------------------------------
    ## 2.7.2. EXPORT 하여 DB 에 저장
    ## ----------------------------------------------------
    excl_tbl_nm = 'RECMD_EXCL_CUST_LIST'
    db_schema_list, db_table_list = schema_inspector()

    if excl_tbl_nm.lower() in db_table_list :
        get_ipython().run_cell_magic('sql', '', """
        DELETE FROM CDS_AMT.{excl_tbl_nm} WHERE ANL_DT = '{target_date}' """)

    else : 
        pass

    conn, engine = init_connection()
    excl_cust_df.to_sql(excl_tbl_nm.lower(), schema = DB_ID.lower(), con = engine, 
                        if_exists = 'append', index = False,
                        dtype = {'cust_id':types.NVARCHAR(20),
                                 'anl_dt': types.DATE})
    conn.close()
    engine.dispose()  


    ## --------------------------------------------------
    ## 2.7.3. 최종 결과 테이블은 배치 로직 마지막 단계에서 Drop
    ## --------------------------------------------------

    tbl_name = 'TB_AMT_RECMD_CPN_RANK_DAILY'
    db_schema_list, db_table_list = schema_inspector()

    if tbl_name.lower() in db_table_list : 
        get_ipython().run_cell_magic('sql', '', """DROP TABLE {DB_ID}.{tbl_name} ; """)

    else : 
        pass


    if len(cust_repair_list) > 0 :

        ## -----------------------------------------------
        ## 2.7.4. 예외 처리 고객 별도 정제
        ## -----------------------------------------------

        get_ipython().run_cell_magic('sql', '', """
        CREATE TABLE CDS_AMT.RECMD_RANK_DAILY_EXCEPTION (
            CUST_ID             NVARCHAR(20), 
            ANL_DT              DATE,
            AGRDE               NVARCHAR(2),
            GNDR_CD             NVARCHAR(1),
            PRDT_CAT_CD         NVARCHAR(10),
            PRDT_DCODE_CD       NVARCHAR(10),
            DT_CNT              DECIMAL(20),
            DT_CNT_ALL          DECIMAL(20),
            CUST_CNT_ALL        DECIMAL(20),
            DT_CNT_RT_ALL       DECIMAL(20,10),
            DT_CNT_RT_RANK      DECIMAL(10)
        ) ;""")

        start_time = add_log(module_nm = '상품추천', module_type_nm = '운영', 
                             step = '3-1.예외처리고객 정제', query_type = 'Create', 
                             target_table = 'RECMD_RANK_DAILY_EXCEPTION', start_time = start_time)

        get_ipython().run_cell_magic('sql', '', """
        INSERT INTO CDS_AMT.RECMD_RANK_DAILY_EXCEPTION (

        /*  구매이력이 희박하여 별도 정제가 필요한 고객들을 모아 (= cust_repair_list)
            BGNDR_CD x AGRDE GROUP 별 구매가 가장 많이 발생하는 상품 리스트 100위를 추출하여
            상품추천 리스트를 생성
        */

        -------------- SUB PART ---------------------
        -- 성별 X 연령 별 최근 7일간 구매 발생 고객의 최근 90일 구매이력 집계


        -- 최근 7일간 구매이력 있는 고객 & 최근 90일 구매 상품만 필터링 (TMP ~ TMP3)
        WITH TMP AS (

            SELECT CUST_ID, PRDT_CAT_CD, PRDT_DCODE_CD, ANL_DT, DT_CNT, AGRDE, GNDR_CD,
                   DENSE_RANK() OVER (PARTITION BY CUST_ID ORDER BY ANL_DT DESC) AS CUST_RANK
              FROM CDS_AMT.TB_AMT_RECMD_TMPR
             WHERE ANL_DT BETWEEN ADD_DAYS(TO_DATE('{target_date}','YYYY-MM-DD'),-6) AND '{target_date}' 
             -- 최근 7일 이내 전체 고객의 구매 이력 필터링
        ), 
        TMP2 AS (
            SELECT  PRDT_CAT_CD
                   ,PRDT_DCODE_CD
                   ,AGRDE
                   ,GNDR_CD
                   ,SUM(DT_CNT) AS DT_CNT_ALL
              FROM TMP 
             WHERE CUST_RANK = 1 -- 최근 7일 이내 2번 이상 내방한 고객들의 구매이력 중 최신 이력만 필터링
          GROUP BY PRDT_CAT_CD, PRDT_DCODE_CD, AGRDE, GNDR_CD
        ), 
        TMP3 AS ( 
            SELECT  A.AGRDE, A.GNDR_CD
                    ,A.PRDT_CAT_CD
                    ,A.PRDT_DCODE_CD
                    ,A.DT_CNT_ALL
                    ,B.CUST_CNT_ALL
                    ,A.DT_CNT_ALL / B.CUST_CNT_ALL AS DT_CNT_RT_ALL -- 최근 7일간 전체 고객의 90일간 품목별 구매일수 비율
                    ,DENSE_RANK() OVER (PARTITION BY AGRDE, GNDR_CD ORDER BY (A.DT_CNT_ALL / B.CUST_CNT_ALL) DESC) AS DT_CNT_RT_RANK
              FROM  TMP2 A,
                    (SELECT COUNT(DISTINCT CUST_ID) AS CUST_CNT_ALL FROM TMP) B
             WHERE  A.DT_CNT_ALL > 1 -- 집계 시 구매 건수가 1인 아이템은 제외
        ),

        -- INPUT TABLE 에서 추가정제가 필요한 고객 정보만 추출 (TMP4)
        TMP4 AS (
            SELECT CUST_ID, ANL_DT, AGRDE, GNDR_CD, PRDT_CAT_CD, PRDT_DCODE_CD, DT_CNT -- , SHOP_DNA_SGMNT, CUST_GRADE_CD
            FROM   CDS_AMT.TB_AMT_RECMD_TMPR

            WHERE  ANL_DT = '{target_date}' 
              AND CUST_ID IN ({cust_repair_list_str})  -- 구매이력이 희박하여 별도 정제가 필요한 고객 리스트만 추출
              -- AND  CUST_ID = 'C89786129'

        )
        --------------- MAIN PART ---------------------

        SELECT   A.CUST_ID, A.ANL_DT, A.AGRDE, A.GNDR_CD
                ,B.PRDT_CAT_CD
                ,B.PRDT_DCODE_CD
                ,C.DT_CNT
                ,B.DT_CNT_ALL
                ,B.CUST_CNT_ALL
                ,B.DT_CNT_RT_ALL
                ,B.DT_CNT_RT_RANK
        FROM (
            SELECT DISTINCT CUST_ID, ANL_DT, AGRDE, GNDR_CD 
            FROM   TMP4
        ) A

        LEFT JOIN TMP3 B
            ON  A.GNDR_CD = B.GNDR_CD
            AND A.AGRDE = B.AGRDE

        LEFT JOIN (
            SELECT CUST_ID, PRDT_CAT_CD, PRDT_DCODE_CD, DT_CNT
            FROM   TMP4 
        ) C
            ON  A.CUST_ID = C.CUST_ID 
            AND B.PRDT_DCODE_CD = C.PRDT_DCODE_CD
            AND B.PRDT_CAT_CD = C.PRDT_CAT_CD

        WHERE (B.DT_CNT_RT_RANK < 300) OR (C.DT_CNT IS NOT NULL)
        ORDER BY B.DT_CNT_RT_RANK ASC )

        """)

        start_time = add_log(module_nm = '상품추천', module_type_nm = '운영', 
                             step = '3-1.예외처리고객 정제', query_type = 'Insert',
                             target_table = 'RECMD_RANK_DAILY_EXCEPTION', start_time = start_time)


        ## ========================================================
        ## 2.8. 쿠폰 분류체계 결합 및 불필요 상품 필터링 
        ## -- 최종 테이블 도출 
        ## ========================================================

        # 임시로 최종테이블 명 뒤에 분석일자를 붙여서 최종 결과 테이블 생성 시에만 사용 
        # tbl_name = 'CDS_AMT.TB_AMT_RECMD_CPN_RANK_DAILY_' + target_date_6digits
        # print(tbl_name)

        get_ipython().run_cell_magic('sql', '', """
        CREATE COLUMN TABLE {tbl_name} (
            CRTN_DT         DATE             NOT NULL COMMENT '생성일자',
            CUST_ID         NVARCHAR(20)     NOT NULL COMMENT '고객ID',
            CPN_CLAS_CD     NVARCHAR(10)     NOT NULL COMMENT '쿠폰분류코드',
            RECMD_DIVCD_NM  NVARCHAR(100)    NOT NULL COMMENT '추천구분명',
            CPN_RECMD_RANKING DECIMAL(10)    COMMENT '쿠폰추천랭킹',

            CONSTRAINT PK_TB_AMT_RECMD_CPN_RANK_DAILY_{target_date_6digits} PRIMARY KEY (CRTN_DT, CUST_ID, CPN_CLAS_CD, RECMD_DIVCD_NM) 
        )
        UNLOAD PRIORITY 5 AUTO MERGE COMMENT '(분석마트)고객추천쿠폰분류' ;
        """)

        start_time = add_log(module_nm = '상품추천', module_type_nm = '운영', 
                             step = '3-2.불필요상품 필터링 및 최종 결과테이블 도출', query_type = 'Create',
                             target_table = tbl_name, start_time = start_time)


        get_ipython().run_cell_magic('sql', '', """
        INSERT INTO {tbl_name} ( 
            WITH TMP AS (
                /* -- 쿠폰 분류체계 데이터 조인 */

                SELECT DISTINCT A.DCODE_CLAS_CD, A.CPN_CLAS_CD, B.CPN_CLAS_NM 
                FROM  CDS_AMT.TB_AMT_CPN_CLAS_DCODE_CD A 

                LEFT JOIN CDS_AMT.TB_AMT_CPN_CLAS_CD B
                    ON A.CPN_CLAS_CD = B.CPN_CLAS_CD
            ), 
            TMP2 AS (
                SELECT  A.*, B.CPN_CLAS_CD, B.CPN_CLAS_NM
                FROM CDS_AMT.RECMD_RANK_DAILY A

                /* -- FLT RULE 1 : 쿠폰분류체계에 해당되는 상품코드만 INNER JOIN 하여 FILTERING */
                INNER JOIN TMP B 
                    ON A.PRDT_DCODE_CD = B.DCODE_CLAS_CD 
                    AND B.CPN_CLAS_NM <> 'X(제외)'
                WHERE A.CUST_ID IS NOT NULL AND A.ANL_DT IS NOT NULL
            ),
            TMP3 AS (
                SELECT A.* 
                FROM   TMP2 A

                /* -- FLT RULE 2 : 일반판매상품X, 담배/성인용품 추가 제외 */
                LEFT JOIN 
                    CDS_AMT.RECMD_EXCL_PRDT_LIST C
                    ON CAST(A.PRDT_DCODE_CD AS INTEGER) = CAST(C.PRDT_DCODE_CD AS INTEGER)

                WHERE C.PRDT_DCODE_CD IS NULL
            )

            SELECT   CRTN_DT, CUST_ID, CPN_CLAS_CD, RECMD_DIVCD_NM 
                    ,DENSE_RANK() OVER (PARTITION BY CRTN_DT, CUST_ID, RECMD_DIVCD_NM 
                                        ORDER BY MRR_FLT_MAX DESC) AS CPN_RECMD_RANKING
            FROM (
                SELECT   F.ANL_DT AS CRTN_DT
                        ,F.CUST_ID 
                        ,F.CPN_CLAS_CD
                        ,F.CPN_CLAS_NM
                        ,F.RECMD_FLAG AS RECMD_DIVCD_NM
                        ,MAX(F.MRR) AS MRR_FLT_MAX 

                FROM TMP3 F 
                WHERE CUST_ID NOT IN ({cust_repair_list_str}) 
                GROUP BY F.ANL_DT, F.CUST_ID, F.RECMD_FLAG, F.CPN_CLAS_CD, F.CPN_CLAS_NM
            )     

            UNION ALL
            -- 예외 처리 고객들 데이터를 별도로 붙임

            SELECT CRTN_DT, CUST_ID, CPN_CLAS_CD, RECMD_DIVCD_NM
                   ,DENSE_RANK() OVER (PARTITION BY CRTN_DT, CUST_ID, RECMD_DIVCD_NM 
                                       ORDER BY DT_CNT_RT_MAX DESC) AS CPN_RECMD_RANKING
            FROM (
                SELECT   F.ANL_DT AS CRTN_DT
                        ,F.CUST_ID
                        ,F.CPN_CLAS_CD
                        ,F.CPN_CLAS_NM
                        ,F.RECMD_DIVCD_NM
                        ,MAX(DT_CNT_RT_ALL) AS DT_CNT_RT_MAX
                FROM (
                    SELECT  A.*
                            ,CASE WHEN A.DT_CNT > 0 THEN 'RE-SELL PRDT' ELSE 'CROSS-SELL PRDT' END AS RECMD_DIVCD_NM
                            ,B.CPN_CLAS_CD
                            ,B.CPN_CLAS_NM
                    FROM    CDS_AMT.RECMD_RANK_DAILY_EXCEPTION A 

                    INNER JOIN TMP B 
                        ON A.PRDT_DCODE_CD = B.DCODE_CLAS_CD 
                        AND B.CPN_CLAS_NM <> 'X(제외)' ) F
                GROUP BY F.ANL_DT, F.CUST_ID, F.CPN_CLAS_CD, F.CPN_CLAS_NM, F.RECMD_DIVCD_NM 
            ) 
            ORDER BY RECMD_DIVCD_NM, CPN_RECMD_RANKING ASC 
        ) ;
        """)

        start_time = add_log(module_nm = '상품추천', module_type_nm = '운영', 
                             step = '3-2.불필요상품 필터링 및 최종 결과테이블 도출', query_type = 'Insert',
                             target_table = tbl_name, start_time = start_time)

    else : 
        ## ========================================================
        ## 2.8. 쿠폰 분류체계 결합 및 불필요 상품 필터링 
        ## -- 최종 테이블 도출 
        ## ========================================================

        # 임시로 최종테이블 명 뒤에 분석일자를 붙여서 최종 결과 테이블 생성 시에만 사용
        # tbl_name = 'CDS_AMT.TB_AMT_RECMD_CPN_RANK_DAILY_' + target_date_6digits
        # print(tbl_name)

        get_ipython().run_cell_magic('sql', '', """
        CREATE COLUMN TABLE {tbl_name} (
            CRTN_DT         DATE             NOT NULL COMMENT '생성일자',
            CUST_ID         NVARCHAR(20)     NOT NULL COMMENT '고객ID',
            CPN_CLAS_CD     NVARCHAR(10)     NOT NULL COMMENT '쿠폰분류코드',
            RECMD_DIVCD_NM  NVARCHAR(100)    NOT NULL COMMENT '추천구분명',
            CPN_RECMD_RANKING DECIMAL(10)    COMMENT '쿠폰추천랭킹',

            CONSTRAINT PK_TB_AMT_RECMD_CPN_RANK_DAILY_{target_date_6digits} PRIMARY KEY (CRTN_DT, CUST_ID, CPN_CLAS_CD, RECMD_DIVCD_NM) 
        )
        UNLOAD PRIORITY 5 AUTO MERGE COMMENT '(분석마트)고객추천쿠폰분류' ;
        """)

        start_time = add_log(module_nm = '상품추천', module_type_nm = '운영', 
                             step = '3-2.불필요상품 필터링 및 최종 결과테이블 도출', query_type = 'Create',
                             target_table = tbl_name, start_time = start_time)

        get_ipython().run_cell_magic('sql', '', """
        INSERT INTO {tbl_name} ( 
            WITH TMP AS (
                /* -- 쿠폰 분류체계 데이터 조인 */

                SELECT DISTINCT A.DCODE_CLAS_CD, A.CPN_CLAS_CD, B.CPN_CLAS_NM 
                FROM  CDS_AMT.TB_AMT_CPN_CLAS_DCODE_CD A 

                LEFT JOIN CDS_AMT.TB_AMT_CPN_CLAS_CD B
                    ON A.CPN_CLAS_CD = B.CPN_CLAS_CD
            ), 
            TMP2 AS (
                SELECT  A.*, B.CPN_CLAS_CD, B.CPN_CLAS_NM
                FROM CDS_AMT.RECMD_RANK_DAILY A

                /* -- FLT RULE 1 : 쿠폰분류체계에 해당되는 상품코드만 INNER JOIN 하여 FILTERING */
                INNER JOIN TMP B 
                    ON A.PRDT_DCODE_CD = B.DCODE_CLAS_CD 
                    AND B.CPN_CLAS_NM <> 'X(제외)'
            ),
            TMP3 AS (
                SELECT A.* 
                FROM   TMP2 A

                /* -- FLT RULE 2 : 일반판매상품X, 담배/성인용품 추가 제외 */
                LEFT JOIN 
                    CDS_AMT.RECMD_EXCL_PRDT_LIST C
                    ON CAST(A.PRDT_DCODE_CD AS INTEGER) = CAST(C.PRDT_DCODE_CD AS INTEGER)

                WHERE C.PRDT_DCODE_CD IS NULL
            )

            SELECT   CRTN_DT, CUST_ID, CPN_CLAS_CD, RECMD_DIVCD_NM 
                    ,DENSE_RANK() OVER (PARTITION BY CRTN_DT, CUST_ID, RECMD_DIVCD_NM 
                                        ORDER BY MRR_FLT_MAX DESC) AS CPN_RECMD_RANKING
            FROM (
                SELECT   F.ANL_DT AS CRTN_DT
                        ,F.CUST_ID 
                        ,F.CPN_CLAS_CD
                        ,F.CPN_CLAS_NM
                        ,F.RECMD_FLAG AS RECMD_DIVCD_NM
                        ,MAX(F.MRR) AS MRR_FLT_MAX 
                FROM TMP2 F 
                GROUP BY F.ANL_DT, F.CUST_ID, F.RECMD_FLAG, F.CPN_CLAS_CD, F.CPN_CLAS_NM
            ) 

            WHERE CRTN_DT IS NOT NULL AND CUST_ID IS NOT NULL AND CPN_CLAS_CD IS NOT NULL AND RECMD_DIVCD_NM IS NOT NULL
        ) ;
        """)

        start_time = add_log(module_nm = '상품추천', module_type_nm = '운영', 
                             step = '3-2.불필요상품 필터링 및 최종 결과테이블 도출', query_type = 'Insert',
                             target_table = tbl_name, start_time = start_time)



    ## ========================================================
    ## 2.9. 임시 테이블 모두 정리
    ## ========================================================

    get_ipython().run_cell_magic('sql', '', """
    -- Logic1 : 확률곱 관련 임시 테이블
    DROP TABLE CDS_AMT.RECMD_LOGIC1_BASKET_AGG ;
    /* DROP TABLE CDS_AMT.TB_AMT_RECMD_LOGIC1_PROBWT_RESULT ; */

    -- Logic2 : Micro-seg 관련 임시 테이블
    DROP TABLE CDS_AMT.CUST_MICROSEG ;
    DROP TABLE CDS_AMT.CUST_RECMD_MICROSEG_BASE_RAW ;
    DROP TABLE CDS_AMT.CUST_RECMD_MICROSEG_BASKET ;
    /* DROP TABLE CDS_AMT.TB_AMT_RECMD_LOGIC2_MICROSEG_RESULT ; */

    -- L1 & L2 결합 임시 테이블 
    DROP TABLE CDS_AMT.RECMD_RANK_TMP ;
    /* DROP TABLE CDS_AMT.RECMD_RANK_DAILY ; */

    -- 예외처리 고객들 임시 테이블
    DROP TABLE CDS_AMT.RECMD_RANK_DAILY_EXCEPTION ;
    """)

except Exception as e :
    
    ## 배치 중간 에러 발생 시 
    ## -- 1. 중간에 만든 모든 임시 테이블들 전체 삭제 
    ## -- 2. 로그 테이블에 error state 및 메시지 적재 후 종료
    
    print(e)
    em = str(e)
    logger.error(em)
    start_time = add_log(module_nm = '상품추천', module_type_nm = '운영', 
                     step = 'error', query_type = 'error', 
                     target_table = '==== error occured ====', start_time = start_time)
    
    
    get_ipython().run_cell_magic('sql', '', """
    -- Logic1 : 확률곱 관련 임시 테이블
    DROP TABLE CDS_AMT.RECMD_LOGIC1_BASKET_AGG ;
    /* DROP TABLE CDS_AMT.TB_AMT_RECMD_LOGIC1_PROBWT_RESULT ; */

    -- Logic2 : Micro-seg 관련 임시 테이블
    DROP TABLE CDS_AMT.CUST_MICROSEG ;
    DROP TABLE CDS_AMT.CUST_RECMD_MICROSEG_BASE_RAW ;
    DROP TABLE CDS_AMT.CUST_RECMD_MICROSEG_BASKET ;
    /* DROP TABLE CDS_AMT.TB_AMT_RECMD_LOGIC2_MICROSEG_RESULT ; */

    -- L1 & L2 결합 임시 테이블 
    DROP TABLE CDS_AMT.RECMD_RANK_TMP ;
    /* DROP TABLE CDS_AMT.RECMD_RANK_DAILY ; */

    -- 예외처리 고객들 임시 테이블
    DROP TABLE CDS_AMT.RECMD_RANK_DAILY_EXCEPTION ;
    """)
