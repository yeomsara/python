import configparser
global HOST,PORT,DB_ID,DB_PW,MODEL_DIR,MODEL_NAME
conf_dir = '/home/cdsadmin/AMT/src/conf/config.ini'
cfg      = configparser.ConfigParser(interpolation=configparser.ExtendedInterpolation())
cfg.read(conf_dir)
#----------------------------------------------------------
HOST      = cfg['dbconnect']['host']
PORT      = int(cfg['dbconnect']['port'])
DB_ID     = cfg['dbconnect']['ID']
DB_PW     = cfg['dbconnect']['PW']
MODEL_DIR = cfg['event']['MODEL_DIR']
#----------------------------------------------------------

#----------------------------------------------------------
# ■ logging
#----------------------------------------------------------
import logging
import logging.config
from datetime import datetime
PYTHON_IP = '10.253.79.23'
MN = 'MONTHLY_BATCH_CODE_KUKSAN_EVENT.py'
LG = 'EVENT KUKSAN'
# logging.config.fileConfig(str(cfg['common']['log_config']),defaults={'date':datetime.now()})
logger     = logging.getLogger('aml_log')
fh = logging.FileHandler('/home/cdsadmin/AMT/src/logs/AMS/{:%Y%m%d}_AMS.log'.format(datetime.now()))
formatter = logging.Formatter(f'\nERR|CDS|{PYTHON_IP}|AMT|1등급|[ %(asctime)s ]|[{MN}]|{LG}|[ LineNo. : %(lineno)d ]| %(message)s')
fh.setFormatter(formatter)
logger.addHandler(fh)
###==========================================================================###
import numpy as np
import pandas as pd 
import time 
import os
import psutil
from hdbcli import dbapi ## hana DB client 
from IPython.core.display import display, HTML
from collections import Counter
import argparse

def DB_Connection() :
    conn=dbapi.connect(HOST,PORT,DB_ID,DB_PW)
    return conn

def select_query(sql) :
    conn = DB_Connection()
    cnt = pd.read_sql(sql, conn)
    conn.close()
    
def load_model(filename):
    model_dir  = MODEL_DIR+filename
    load_model = joblib.load(model_dir)
    return load_model

def execute_query(query):
    conn = DB_Connection()
    try:
        cur  = conn.cursor()
        cur.execute(query)
        conn.commit()
        print("%s 실행완료"%query)
        check = 1
    except Exception as e:
        print('ERROR : ',e)
        check = 0
    finally:
        conn.close()
    return check

def insert_table(table_name,df):
    conn = DB_Connection() 
    cur = conn.cursor()
    input_data = [tuple(x)for x in df.values]
    cols_len = '?,'*int(len(df.columns))
    cols_len = cols_len[:-1]
    query    = '''INSERT INTO %s VALUES(%s)'''%(table_name,cols_len)
    print(query)
    cur.executemany(query, input_data)
    conn.commit()
    conn.close()
    return print("%s 테이블 데이터 입력완료"%table_name)

def query_SQL(query): 
    ## DB Connection
    conn=DB_Connection() ## DB 연결 정보
    ## Get a DataFrame 
    start_time = time.time()
    query_result = pd.read_sql(query, conn)
    ## Close Connection
    print( '---- %s seconds ------'%(time.time()-start_time))
    conn.close()
    
    return query_result

def add_logs(module,mdl_type,step,qt,tt,td,st,et,ec=0,es=None) :
    log_df = pd.DataFrame({'MODULE'  :[module],
                            'MODULE_TYPE':[mdl_type],
                            'STEP':[step],
                            'QUERY_TYPE':[qt],
                            'TARGET_TABLE':[tt],
                            'TARGET_DATE' :[td],
                            'START_TIME':[st],
                            'END_TIME':[et],
                            'ERROR_CODE':[ec],
                            'ERROR_STATE':[es]})
    return log_df
   
#■=====================================================================
try : 
    parser = argparse.ArgumentParser(description = 'EVENT SIMILARITY BATCH')
    parser.add_argument('--DNA_YM', help = '예측 시행년월')
    args = parser.parse_args()
    DNA_YM = args.DNA_YM
except: 
    DNA_YM = None

if (DNA_YM == None) | (DNA_YM == 'None'):
    mask ='%Y%m'
    DNA_YM = datetime.now().strftime(mask)
#■===================================================================== 

try :
    #     DNA_YM = '202105'
    now_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    d_format = "%Y%m"
    datetime.strptime(DNA_YM, d_format)
    target_dt = str(DNA_YM[0:4])+"-"+str(DNA_YM[4:6])+"-01"
    result_tb = 'TB_AMT_EVENT_SIMILARITY_KUKSAN_EVENT_RESULT'
    log_df = add_logs(module = '행사유사도(국산의힘)',mdl_type='운영',step='0.예측시작',qt='Create',tt=result_tb,
                      td=target_dt,st=now_date,et=now_date)
    insert_table('CDS_AMT.TB_AMT_CAMPAIGN_ANL_LOG' ,log_df)

    BF_M = 3 #3개월전은 3 입력

    print("================================================")
    print("============= Argument setting =================")
    print("DNA_YM : ",DNA_YM,
          "\nBF_M : ",BF_M,sep="")
    print("================================================")

    today_date = datetime.now().strftime('%Y%m%d')
    print('MODEL WEIGHT YM_WCNT : %s'% today_date)

    YM_WCNT=DNA_YM+'01'

    KUKSAN_WEIGHT_SQL = f'''
                        SELECT SKU_WEIGHT,MAIN_PURCHS_WEIGHT,EVENT_WEIGHT,PRICE_PREFER_WEIGHT
                        FROM CDS_AMT.TB_AMT_EVENT_SIMILARITY_MODEL_WEIGHT
                        WHERE EVENT_MODEL = '국산의힘'
                        AND SDT_YM_WCNT <= '{today_date}' 
                        AND EDT_YM_WCNT > '{today_date}' 
                         '''
    weight_df = query_SQL(KUKSAN_WEIGHT_SQL)

    SKU_WEIGHT                = float(weight_df['SKU_WEIGHT'])  #SKU BASKET 비중
    MAIN_PURCHS_WEIGHT        = float(weight_df['MAIN_PURCHS_WEIGHT']) #주구매 SCORE
    EVENT_WEIGHT              = float(weight_df['EVENT_WEIGHT']) #행사선호도(통합)
    HGHPC_PREFER_WEIGHT       = float(weight_df['PRICE_PREFER_WEIGHT']) # 고가선호도
    EVENT_MONTH= DNA_YM  #월 숫자만 교체 #CSV 파일에 년/월 양식이 바뀌면 그 양식에 맞춰 교체 필요

    query2 = f'''
     WITH BASE_RCIP_CUST_ID AS ( SELECT A.CUST_ID,A.BSN_DT,A.PRDT_CD,A.AFLCO_CD ,A.BIZTP_CD
                                FROM CDS_DW.TB_DW_RCIPT_DETAIL A 
                                WHERE   A.AFLCO_CD ='001'
                                    AND A.BIZTP_CD ='10'
                                    AND A.RL_SALE_TRGT_YN = 'Y'
                                    AND TO_CHAR(A.BSN_DT,'YYYYMM') BETWEEN ADD_MONTHS (TO_CHAR ('{DNA_YM}', 'YYYYMM'),-3) AND ADD_MONTHS (TO_CHAR ('{DNA_YM}', 'YYYYMM'), -1) --고객산출 기간변경
                                    AND A.CUST_ID IS NOT NULL)
        ,EMART_PRODUCT_LIST AS (SELECT D.PRDT_DCODE_CD,D.PRDT_DCODE_NM,PRDT_CD,PRDT_NM
                                FROM CDS_DW.TB_DW_PRDT_MASTR AS A
                                LEFT JOIN CDS_DW.TB_DW_PRDT_DCODE_CD AS D
                                 ON A.PRDT_DCODE_CD=D.PRDT_DCODE_CD AND D.AFLCO_CD = A.AFLCO_CD AND D.BIZTP_CD = A.BIZTP_CD 
                                 WHERE PRDT_NM LIKE('%국산의%')
                                 AND A.AFLCO_CD='001'
                                 AND A.BIZTP_CD='10')
        SELECT  T2.CUST_ID
              , '{YM_WCNT}'        AS YM_WCNT
              , T2.TOTAL_SCORE     AS F_TOTAL_SCORE
              , CASE 
                     WHEN  (TOTAL_SCORE > M_TOTAL_SCORE*0.9 AND TOTAL_SCORE <= M_TOTAL_SCORE*1.0) OR (PERCENT_GRADE >= 0.9) THEN '90'
                     WHEN   TOTAL_SCORE > M_TOTAL_SCORE*0.8 AND TOTAL_SCORE <= M_TOTAL_SCORE*0.9 THEN '80'
                     WHEN   TOTAL_SCORE > M_TOTAL_SCORE*0.7 AND TOTAL_SCORE <= M_TOTAL_SCORE*0.8 THEN '70'
                     WHEN   TOTAL_SCORE > M_TOTAL_SCORE*0.6 AND TOTAL_SCORE <= M_TOTAL_SCORE*0.7 THEN '60'
                     WHEN   TOTAL_SCORE > M_TOTAL_SCORE*0.5 AND TOTAL_SCORE <= M_TOTAL_SCORE*0.6 THEN '50'
                     WHEN   TOTAL_SCORE > M_TOTAL_SCORE*0.4 AND TOTAL_SCORE <= M_TOTAL_SCORE*0.5 THEN '40'
                     WHEN   TOTAL_SCORE > M_TOTAL_SCORE*0.3 AND TOTAL_SCORE <= M_TOTAL_SCORE*0.4 THEN '30'
                     WHEN   TOTAL_SCORE > M_TOTAL_SCORE*0.2 AND TOTAL_SCORE <= M_TOTAL_SCORE*0.3 THEN '20'
                     WHEN   TOTAL_SCORE > M_TOTAL_SCORE*0.1 AND TOTAL_SCORE <= M_TOTAL_SCORE*0.2 THEN '10'
                     WHEN   TOTAL_SCORE <=M_TOTAL_SCORE*0.1 THEN '0'
                     END AS SCORING_GROUP 
        FROM (
                SELECT  T1.CUST_ID
                      , T1.YM_WCNT
                      , TOTAL_SCORE
                      , PERCENT_GRADE
                      , PERCENT_GRADE_CAT
                      , MAX(TOTAL_SCORE) OVER (PARTITION BY PERCENT_GRADE_CAT) AS M_TOTAL_SCORE
                FROM (
                        SELECT  A.CUST_ID 																					   	                       AS CUST_ID
                              , MAX(D.YM_WCNT)                                                                                                         AS YM_WCNT
                              , ((CASE WHEN SUM(PURCHA_SKU) IS NULL THEN 0 ELSE SUM(PURCHA_SKU) END)/SUM(TOT_SKU_VISIT))*{SKU_WEIGHT}+
                                ((CASE WHEN MAX(C.AVG_MAIN_PURCHS_SCORE) IS NULL THEN 0 ELSE MAX(C.AVG_MAIN_PURCHS_SCORE) END))*{MAIN_PURCHS_WEIGHT}+
                                ((CASE WHEN MAX(D.TOTAL_EVENT_TYP_PRE_UNITY) IS NULL THEN 0 ELSE MAX(D.TOTAL_EVENT_TYP_PRE_UNITY) END))*{EVENT_WEIGHT}+
                                ((CASE WHEN MAX(E.HGHPC_PREFER) IS NULL THEN 0 ELSE MAX(E.HGHPC_PREFER) END))*{HGHPC_PREFER_WEIGHT}              AS TOTAL_SCORE
                              , CUME_DIST() OVER (ORDER BY ((CASE WHEN SUM(PURCHA_SKU) IS NULL THEN 0 ELSE SUM(PURCHA_SKU) END)/SUM(TOT_SKU_VISIT))*{SKU_WEIGHT}+
                                        ((CASE WHEN MAX(C.AVG_MAIN_PURCHS_SCORE) IS NULL THEN 0 ELSE MAX(C.AVG_MAIN_PURCHS_SCORE) END))*{MAIN_PURCHS_WEIGHT}+
                                        ((CASE WHEN MAX(D.TOTAL_EVENT_TYP_PRE_UNITY) IS NULL THEN 0 ELSE MAX(D.TOTAL_EVENT_TYP_PRE_UNITY) END))*{EVENT_WEIGHT}+
                                        ((CASE WHEN MAX(E.HGHPC_PREFER) IS NULL THEN 0 ELSE MAX(E.HGHPC_PREFER) END))*{HGHPC_PREFER_WEIGHT} ASC) AS PERCENT_GRADE
                              , CASE WHEN CUME_DIST() OVER (ORDER BY ((CASE WHEN SUM(PURCHA_SKU) IS NULL THEN 0 ELSE SUM(PURCHA_SKU) END)/SUM(TOT_SKU_VISIT))*{SKU_WEIGHT}+
                                        ((CASE WHEN MAX(C.AVG_MAIN_PURCHS_SCORE) IS NULL THEN 0 ELSE MAX(C.AVG_MAIN_PURCHS_SCORE) END))*{MAIN_PURCHS_WEIGHT}+
                                        ((CASE WHEN MAX(D.TOTAL_EVENT_TYP_PRE_UNITY) IS NULL THEN 0 ELSE MAX(D.TOTAL_EVENT_TYP_PRE_UNITY) END))*{EVENT_WEIGHT}+
                                        ((CASE WHEN MAX(E.HGHPC_PREFER) IS NULL THEN 0 ELSE MAX(E.HGHPC_PREFER) END))*{HGHPC_PREFER_WEIGHT} ASC) >= 0.9
                                     THEN 'H'
                                     ELSE 'L'
                                     END                                                                                                               AS PERCENT_GRADE_CAT
                        -- A: 최근 3개월 동안 구매이력이 있는 고객의 방문횟수
                        FROM (
                                SELECT CUST_ID,PRDT_CD,COUNT(DISTINCT BSN_DT) AS TOT_SKU_VISIT
                                FROM BASE_RCIP_CUST_ID
                                WHERE TO_CHAR(BSN_DT,'YYYYMM') BETWEEN ADD_MONTHS (TO_CHAR ('{DNA_YM}', 'YYYYMM'),-3) AND ADD_MONTHS (TO_CHAR ('{DNA_YM}', 'YYYYMM'), -1)
                                GROUP BY CUST_ID,PRDT_CD
                              ) A 
                        -- B: 고객별 국산의힘 상품 구매횟수
                        LEFT JOIN (SELECT A.CUST_ID,A.PRDT_CD,COUNT(DISTINCT A.BSN_DT) AS PURCHA_SKU
                                    FROM BASE_RCIP_CUST_ID A
                                    JOIN EMART_PRODUCT_LIST B ON A.PRDT_CD = B.PRDT_CD
                                    WHERE B.PRDT_CD IS NOT NULL 
                                    AND TO_CHAR(A.BSN_DT,'YYYYMM') BETWEEN ADD_MONTHS (TO_CHAR ('{DNA_YM}', 'YYYYMM'),-3) AND ADD_MONTHS (TO_CHAR ('{DNA_YM}', 'YYYYMM'), -1)
                                    GROUP BY A.CUST_ID,A.PRDT_CD 
                                  ) B ON A.CUST_ID = B.CUST_ID AND A.PRDT_CD = B.PRDT_CD 
                        -- C : 주 구매 스코어
                        LEFT JOIN (
                                    SELECT CUST_ID,AVG(MAIN_PURCHS_SCORE) AS AVG_MAIN_PURCHS_SCORE
                                    FROM TB_AMT_CUST_PRDT_DNA_DATA 
                                    WHERE YM_WCNT='{YM_WCNT}'
                                    AND AFLCO_CD ='001'
                                    AND BIZTP_CD ='10' 
                                    AND PRDT_DCODE_CD IN (SELECT DISTINCT PRDT_DCODE_CD FROM EMART_PRODUCT_LIST)
                                    GROUP BY CUST_ID
                                  ) C ON A.CUST_ID = C.CUST_ID
                        -- D : 행사 이벤트 선호도 
                        LEFT JOIN ( 
                                    SELECT CUST_ID,YM_WCNT,CASE WHEN EVENT_TYP_PRE_UNITY IS NULL THEN 0 ELSE EVENT_TYP_PRE_UNITY END AS TOTAL_EVENT_TYP_PRE_UNITY
                                    FROM TB_AMT_BIZTP_CUST_DNA_DATA
                                    WHERE YM_WCNT ='{YM_WCNT}' --월 변경
                                    AND AFLCO_CD ='001'
                                    AND BIZTP_CD ='10'
                                    ORDER BY EVENT_TYP_PRE_UNITY DESC
                                   ) AS D ON A.CUST_ID = D.CUST_ID
                        -- E : 고가선호도
                        LEFT JOIN (
                                    SELECT CUST_ID,CASE WHEN HGHPC_PREFER IS NULL THEN 0 ELSE HGHPC_PREFER END AS HGHPC_PREFER
                                    FROM TB_AMT_AFLCO_CUST_DNA_DATA
                                    WHERE YM_WCNT ='{YM_WCNT}' 
                                    AND AFLCO_CD ='001'
                                  ) AS E ON A.CUST_ID = E.CUST_ID
                        GROUP BY A.CUST_ID
                 ) T1
        )T2
    '''

    KUKSAN_EVENT=query_SQL( query2)
    print(KUKSAN_EVENT.head())

    del_sql = f'''DELETE FROM {result_tb} WHERE YM_WCNT='{YM_WCNT}' '''
    execute_query(del_sql)
    insert_table(result_tb,KUKSAN_EVENT)
    end_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    log_df = add_logs(module = '행사유사도(국산의힘)',mdl_type='운영',step='1.APPLY 테이블 생성 및 결과적재',qt='Insert',tt=result_tb,
                      td=target_dt,st=now_date,et=end_date)
    insert_table('CDS_AMT.TB_AMT_CAMPAIGN_ANL_LOG' ,log_df)
except Exception as e:
    error = str(e)
    logger.error(error)
    end_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S') 
    log_df = add_logs(module = '행사유사도(국산의힘)',mdl_type='운영',step='error',qt='error',tt='==== error occured ====',
                  td=datetime.now().strftime('%Y-%m-%d'),st=now_date,et=end_date,ec=1,es=error)
    insert_table('CDS_AMT.TB_AMT_CAMPAIGN_ANL_LOG' ,log_df)
