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
MN = 'MONTHLY_BATCH_CODE_PEACOCK.py'
LG = 'EVENT PEACOCK'
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

def add_logs(module,mdl_type,step,qt,tt,td,st,et,ec=0,es=None):
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
    parser = argparse.ArgumentParser(description = 'PEACOCK EVENT SIMILARITY BATCH')
    parser.add_argument('--DNA_YM', help = '예측 시행년월')
    args = parser.parse_args()
    DNA_YM = args.DNA_YM
except : 
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
    result_tb = 'CDS_AMT.TB_AMT_EVENT_SIMILARITY_PEACOCK_RESULT'
    log_df = add_logs(module = '행사유사도(피코크)',mdl_type='운영',step='0.예측시작',qt='Create',tt=result_tb,
                      td=target_dt,st=now_date,et=now_date)
    insert_table('CDS_AMT.TB_AMT_CAMPAIGN_ANL_LOG' ,log_df)
    
    BF_M = 3 #3개월전은 3 입력

    print("================================================")
    print("============= Argument setting =================")
    print("DNA_YM : ",DNA_YM,
          "\nBF_M : ",BF_M,sep="")
    print("================================================")

    DATE= f'''TO_CHAR(R.BSN_DT,'YYYYMM') BETWEEN ADD_MONTHS (TO_CHAR ('{DNA_YM}', 'YYYYMM'),-{BF_M}) AND ADD_MONTHS (TO_CHAR ('{DNA_YM}', 'YYYYMM'), -1)'''
    YM_WCNT=DNA_YM+'01'

    VISIT_NUM = '3' #주이용 고객 방문횟수 ,월 평균 1회이상 고객 기준
    COUNT_RCIPT_NUM = '0.7' #A급 상품 영수증 구매 건수 상위 누적% FILTER CRITERIA
    FILTER_CRITERIA = '1.5' #상품 연관성 지표 평균 00배 FILTER 기준 

    #21,03,24 수정 (현업제공)
    PEACOCK_PRODUCT='''SELECT DISTINCT PRDT_CD,PRDT_NM
                        FROM
                        (
                        SELECT DISTINCT PRDT_CD,PRDT_NM
                        FROM CDS_DW.TB_DW_PRDT_MASTR
                        WHERE BRAND_CD='100105'
                        AND AFLCO_CD='001'
                        AND BIZTP_CD='10'
                        AND PRDT_DCODE_CD<>'0416'

                        UNION ALL

                        SELECT DISTINCT PRDT_CD,PRDT_NM
                        FROM CDS_DW.TB_DW_PRDT_MASTR
                        WHERE (PRDT_NM LIKE('%피코크%')
                        OR PRDT_NM LIKE('%PEACOCK%')
                        )
                        AND AFLCO_CD='001'
                        AND BIZTP_CD='10'

                        UNION ALL

                        SELECT DISTINCT PRDT_CD,PRDT_NM
                        FROM CDS_DW.TB_DW_PRDT_MASTR AS A
                        INNER JOIN CDS_DW.TB_DW_PRDT_DCODE_CD AS B ON A.PRDT_DCODE_CD=B.PRDT_DCODE_CD AND A.AFLCO_CD=B.AFLCO_CD AND A.BIZTP_CD=B.BIZTP_CD
                        WHERE PRDT_DCODE_NM LIKE('P)%')
                        AND A.AFLCO_CD='001'
                        AND A.BIZTP_CD='10'
                        )'''

    query1=f'''
    SELECT CUST_ID,'{YM_WCNT}' AS YM_WCNT,F_TOTAL_SCORE,
           CASE WHEN F_TOTAL_SCORE >= M_TOTAL_SCORE*1.0 THEN '90' END AS SCORING_GROUP
    FROM(          
    SELECT CUST_ID,F_TOTAL_SCORE,PERCENT_GRADE,MIN(F_TOTAL_SCORE) OVER() AS M_TOTAL_SCORE
    FROM(
    SELECT CUST_ID,F_TOTAL_SCORE,PERCENT_GRADE
    FROM(
    SELECT CUST_ID,AVG_CONFIDENCE,COUNT_PURCHA_A,TOTAL_SCORE,
           CASE WHEN TOTAL_SCORE IS NULL THEN 0 ELSE TOTAL_SCORE END AS F_TOTAL_SCORE,
           CUME_DIST() OVER (ORDER BY TOTAL_SCORE ASC) AS PERCENT_GRADE  
    FROM(
    SELECT CUST_ID,AVG_CONFIDENCE,COUNT_PURCHA_A,AVG_CONFIDENCE*COUNT_PURCHA_A AS TOTAL_SCORE  
    FROM(
    SELECT CUST_ID,AVG(CONFIDENCE_A) AS AVG_CONFIDENCE, COUNT(DISTINCT PURCHA_A) AS COUNT_PURCHA_A
    FROM(
    SELECT R.CUST_ID,R.BSN_DT,R.PRDT_CD,P.PRDT_CD,P.PRDT_GCODE_CD,P.PRDT_MCODE_CD,P.PRDT_DCODE_CD,D.PRDT_DCODE_NM,N.A_PRDT_DCODE_CD,N.PRDT_DCODE_NM_A,N.B_PRDT_DCODE_CD,N.PRDT_DCODE_NM_B,N.CONFIDENCE,
        CASE WHEN A_PRDT_DCODE_CD IS NULL THEN '' ELSE R.BSN_DT END AS PURCHA_A,
        CASE WHEN A_PRDT_DCODE_CD IS NULL THEN 0 ELSE N.CONFIDENCE END AS CONFIDENCE_A
    FROM CDS_DW.TB_DW_RCIPT_DETAIL AS R
        LEFT JOIN CDS_DW.TB_DW_PRDT_MASTR AS P
        ON R.PRDT_CD = P.PRDT_CD
        AND R.BIZTP_CD=P.BIZTP_CD
        AND R.AFLCO_CD=P.AFLCO_CD
        LEFT JOIN CDS_DW.TB_DW_PRDT_DCODE_CD AS D
        ON R.BIZTP_CD=D.BIZTP_CD
        AND R.AFLCO_CD=D.AFLCO_CD
        AND P.PRDT_DCODE_CD = D.PRDT_DCODE_CD
        LEFT JOIN 
                                        (SELECT A_PRDT_DCODE_CD,PRDT_DCODE_NM_A,B_PRDT_DCODE_CD,PRDT_DCODE_NM_B,CONFIDENCE,LIFT,JACARDS_3M,ODDS_RATIO_TRAN,ODDS_RATIO_YEAR,CONFIDENCE_CR,LIFT_CR,JACARDS_3M_CR,ODDS_RATIO_TRAN_CR,ODDS_RATIO_YEAR_CR,CONFIDENCE_PASS_FAIL,LIFT_PASS_FAIL,JACARDS_3M_PASS_FAIL,ODDS_RATIO_TRAN_PASS_FAIL,ODDS_RATIO_YEAR_PASS_FAIL
                                         FROM(
                                        SELECT BIZTP_CD,AGE_DIV,GNDR_CD,A_PRDT_DCODE_CD,PRDT_DCODE_NM_A,B_PRDT_DCODE_CD,PRDT_DCODE_NM_B,CONFIDENCE,LIFT,JACARDS_3M,ODDS_RATIO_TRAN,ODDS_RATIO_YEAR,CONFIDENCE_CR,LIFT_CR,JACARDS_3M_CR,ODDS_RATIO_TRAN_CR,ODDS_RATIO_YEAR_CR,
                                               CASE WHEN CONFIDENCE >= CONFIDENCE_CR THEN 'PASS' ELSE 'FAIL' END AS CONFIDENCE_PASS_FAIL,
                                               CASE	WHEN LIFT >= LIFT_CR THEN 'PASS' ELSE 'FAIL' END AS LIFT_PASS_FAIL,
                                               CASE	WHEN JACARDS_3M >= JACARDS_3M_CR THEN 'PASS' ELSE 'FAIL' END AS JACARDS_3M_PASS_FAIL,
                                               CASE	WHEN ODDS_RATIO_TRAN >= ODDS_RATIO_TRAN_CR THEN 'PASS' ELSE 'FAIL' END AS ODDS_RATIO_TRAN_PASS_FAIL,
                                               CASE WHEN ODDS_RATIO_YEAR >= ODDS_RATIO_YEAR_CR THEN 'PASS' ELSE 'FAIL' END AS ODDS_RATIO_YEAR_PASS_FAIL
                                    FROM(
                                    SELECT BIZTP_CD,AGE_DIV,GNDR_CD,A_PRDT_DCODE_CD,PRDT_DCODE_NM_A,B_PRDT_DCODE_CD,PRDT_DCODE_NM_B,CONFIDENCE,LIFT,JACARDS_3M,ODDS_RATIO_TRAN,ODDS_RATIO_YEAR,AVG(CONFIDENCE*{FILTER_CRITERIA}) OVER() AS CONFIDENCE_CR, AVG(LIFT*{FILTER_CRITERIA}) OVER() AS LIFT_CR,AVG(JACARDS_3M*{FILTER_CRITERIA}) OVER() AS JACARDS_3M_CR,AVG(ODDS_RATIO_TRAN*{FILTER_CRITERIA}) OVER() AS ODDS_RATIO_TRAN_CR,AVG(ODDS_RATIO_YEAR*{FILTER_CRITERIA}) OVER() AS ODDS_RATIO_YEAR_CR	
                                    FROM
                                    (SELECT D.BIZTP_CD,D.AGE_DIV,D.GNDR_CD,D.A_PRDT_DCODE_CD,C.PRDT_DCODE_NM AS PRDT_DCODE_NM_A,A.PRDT_DCODE_NM AS PRDT_DCODE_NM_B,D.B_PRDT_DCODE_CD,D.SUPPORT,D.CONFIDENCE,D.LIFT,D.JACARDS_3M,D.ODDS_RATIO_TRAN,D.ODDS_RATIO_YEAR
                                    FROM CDS_AMT.TB_AMT_PRDT_PRDT_DNA_DATA AS D
                                    LEFT JOIN CDS_DW.TB_DW_PRDT_MASTR AS P
                                    ON D.BIZTP_CD=P.BIZTP_CD
                                    AND D.A_PRDT_DCODE_CD=P.PRDT_DCODE_CD 
                                    LEFT JOIN CDS_DW.TB_DW_PRDT_DCODE_CD AS C
                                    ON P.BIZTP_CD = C.BIZTP_CD 
                                    AND P.PRDT_DI_CD =C.PRDT_DI_CD 
                                    AND D.A_PRDT_DCODE_CD = C.PRDT_DCODE_CD
                                    LEFT JOIN (SELECT PRDT_DCODE_CD,PRDT_DCODE_NM,COUNT_RCIPT_NO,PERCENT_GRADE --A급 상품 추출
                                    FROM (
                                    SELECT PRDT_DCODE_CD,PRDT_DCODE_NM,COUNT_RCIPT_NO,
                                           CUME_DIST() OVER (ORDER BY COUNT_RCIPT_NO DESC) AS PERCENT_GRADE --RCIPT_NO 기준 Percentile 산출
                                    FROM (
                                    SELECT DISTINCT PRDT_DCODE_CD,PRDT_DCODE_NM,
                                           COUNT(DISTINCT RCIPT_NO) OVER (PARTITION BY PRDT_DCODE_CD) AS COUNT_RCIPT_NO --소분류 기준 영수증건수 COUNT
                                    FROM(
                                    SELECT CUST_ID,BSN_DT,RCIPT_NO,PRDT_CD,PRDT_DCODE_CD,PRDT_DCODE_NM,NUM_VISIT_MONTH --주이용 고객 산출
                                    FROM(
                                    SELECT R.CUST_ID,R.BSN_DT,R.RCIPT_NO,N.PRDT_CD,F.PRDT_DCODE_CD,F.PRDT_DCODE_NM,
                                           COUNT(DISTINCT R.BSN_DT) OVER(PARTITION BY R.CUST_ID) AS NUM_VISIT_MONTH
                                    FROM CDS_DW.TB_DW_RCIPT_DETAIL AS R
                                        LEFT JOIN CDS_DW.TB_DW_PRDT_MASTR AS P
                                        ON R.PRDT_CD = P.PRDT_CD
                                        AND R.BIZTP_CD=P.BIZTP_CD
                                        AND R.AFLCO_CD=P.AFLCO_CD
                                        LEFT JOIN CDS_DW.TB_DW_PRDT_DCODE_CD AS D
                                        ON R.BIZTP_CD=D.BIZTP_CD
                                        AND R.AFLCO_CD=D.AFLCO_CD
                                        AND P.PRDT_DCODE_CD = D.PRDT_DCODE_CD
                                        LEFT JOIN ({PEACOCK_PRODUCT}) AS N
                                        ON P.PRDT_CD = N.PRDT_CD
                                        LEFT JOIN CDS_DW.TB_DW_PRDT_DCODE_CD AS F
                                        ON P.PRDT_DCODE_CD=F.PRDT_DCODE_CD 
                                        WHERE P.AFLCO_CD ='001'
                                        AND P.BIZTP_CD ='10'
                                        AND D.AFLCO_CD ='001'
                                        AND D.BIZTP_CD ='10'
                                        AND N.PRDT_CD IS NOT NULL
                                        AND R.CUST_ID IS NOT NULL
                                        AND {DATE} --상품연관성 측정 시 A급 고객 추출 산정 기간 
                                        AND R.AFLCO_CD ='001'
                                        AND R.BIZTP_CD ='10'
                                        AND R.RL_SALE_TRGT_YN = 'Y'
                                        AND F.AFLCO_CD ='001'
                                        AND F.BIZTP_CD ='10'
                                        GROUP BY R.CUST_ID,R.BSN_DT,R.RCIPT_NO,N.PRDT_CD,F.PRDT_DCODE_CD,F.PRDT_DCODE_NM)
                                        WHERE NUM_VISIT_MONTH >= {VISIT_NUM})
                                        GROUP BY PRDT_DCODE_CD,PRDT_DCODE_NM,RCIPT_NO
                                        ORDER BY COUNT_RCIPT_NO DESC))
                                        WHERE PERCENT_GRADE <= {COUNT_RCIPT_NUM}) AS A --영수증 건수 기준 상위 70% A급 상품
                                        ON A.PRDT_DCODE_CD=D.B_PRDT_DCODE_CD
                                    WHERE D.AFLCO_CD ='001'
                                    AND D.YM_WCNT = '{YM_WCNT}'
                                    AND P.AFLCO_CD='001'
                                    AND P.BIZTP_CD ='10'
                                    AND C.AFLCO_CD ='001'
                                    AND C.BIZTP_CD ='10'
                                    AND D.AFLCO_CD ='001'
                                    AND D.BIZTP_CD ='10'
                                    AND P.PRDT_DI_CD IS NOT NULL
                                    AND CONFIDENCE IS NOT NULL
                                    AND D.AGE_DIV ='전체'
                                    AND D.GNDR_CD ='전체'
                                    AND A.PRDT_DCODE_CD IS NOT NULL
                                    GROUP BY D.BIZTP_CD,D.AGE_DIV,D.GNDR_CD,D.A_PRDT_DCODE_CD,C.PRDT_DCODE_NM,A.PRDT_DCODE_NM,D.B_PRDT_DCODE_CD,D.SUPPORT,D.CONFIDENCE,D.LIFT,D.JACARDS_3M,D.ODDS_RATIO_TRAN,D.ODDS_RATIO_YEAR)
                                    GROUP BY BIZTP_CD,AGE_DIV,GNDR_CD,A_PRDT_DCODE_CD,PRDT_DCODE_NM_A,B_PRDT_DCODE_CD,PRDT_DCODE_NM_B,CONFIDENCE,LIFT,JACARDS_3M,ODDS_RATIO_TRAN,ODDS_RATIO_YEAR
                                    ORDER BY B_PRDT_DCODE_CD))
                                    WHERE  CONFIDENCE_PASS_FAIL= 'PASS'  --상품 FILTER 기준 PASS OR FAIL 선정
                                    AND  LIFT_PASS_FAIL='PASS'
                                    AND  JACARDS_3M_PASS_FAIL  ='PASS'
                                    AND  ODDS_RATIO_TRAN_PASS_FAIL  ='PASS'
                                    AND  ODDS_RATIO_YEAR_PASS_FAIL ='PASS') AS N
                                    ON P.PRDT_DCODE_CD=N.A_PRDT_DCODE_CD --구매 코드와 A급 상품 코드 일치
    WHERE R.CUST_ID IS NOT NULL
    AND {DATE} --스코어 고객 산정기간
    AND R.AFLCO_CD ='001'
    AND R.BIZTP_CD ='10'
    AND R.RL_SALE_TRGT_YN = 'Y'
    ORDER BY CUST_ID)
    GROUP BY CUST_ID)))
    GROUP BY CUST_ID,F_TOTAL_SCORE,PERCENT_GRADE)
    WHERE PERCENT_GRADE >= 0.9)  --상위 10% 구간 CRITERIA 선정
    GROUP BY CUST_ID,'{YM_WCNT}',F_TOTAL_SCORE,M_TOTAL_SCORE,PERCENT_GRADE


    UNION ALL


    SELECT CUST_ID,'{YM_WCNT}' AS YM_WCNT,F_TOTAL_SCORE,
           CASE WHEN F_TOTAL_SCORE > M_TOTAL_SCORE*0.9 AND F_TOTAL_SCORE <=M_TOTAL_SCORE*1.0 THEN '90'
                WHEN F_TOTAL_SCORE > M_TOTAL_SCORE*0.8 AND F_TOTAL_SCORE <= M_TOTAL_SCORE*0.9 THEN '80'
                WHEN F_TOTAL_SCORE > M_TOTAL_SCORE*0.7 AND F_TOTAL_SCORE <= M_TOTAL_SCORE*0.8 THEN '70'
                WHEN F_TOTAL_SCORE > M_TOTAL_SCORE*0.6 AND F_TOTAL_SCORE <= M_TOTAL_SCORE*0.7 THEN '60'
                WHEN F_TOTAL_SCORE > M_TOTAL_SCORE*0.5 AND F_TOTAL_SCORE <= M_TOTAL_SCORE*0.6 THEN '50'
                WHEN F_TOTAL_SCORE > M_TOTAL_SCORE*0.4 AND F_TOTAL_SCORE <= M_TOTAL_SCORE*0.5 THEN '40'
                WHEN F_TOTAL_SCORE > M_TOTAL_SCORE*0.3 AND F_TOTAL_SCORE <= M_TOTAL_SCORE*0.4 THEN '30'
                WHEN F_TOTAL_SCORE > M_TOTAL_SCORE*0.2 AND F_TOTAL_SCORE <= M_TOTAL_SCORE*0.3 THEN '20'
                WHEN F_TOTAL_SCORE > M_TOTAL_SCORE*0.1 AND F_TOTAL_SCORE <= M_TOTAL_SCORE*0.2 THEN '10'
                WHEN F_TOTAL_SCORE <=M_TOTAL_SCORE*0.1 THEN '0'END AS SCORING_GROUP 
    FROM(          
    SELECT CUST_ID,F_TOTAL_SCORE,PERCENT_GRADE,MAX(F_TOTAL_SCORE) OVER() AS M_TOTAL_SCORE
    FROM(
    SELECT CUST_ID,F_TOTAL_SCORE,PERCENT_GRADE
    FROM(
    SELECT CUST_ID,AVG_CONFIDENCE,COUNT_PURCHA_A,TOTAL_SCORE,
           CASE WHEN TOTAL_SCORE IS NULL THEN 0 ELSE TOTAL_SCORE END AS F_TOTAL_SCORE,
           CUME_DIST() OVER (ORDER BY TOTAL_SCORE ASC) AS PERCENT_GRADE  
    FROM(
    SELECT CUST_ID,AVG_CONFIDENCE,COUNT_PURCHA_A,AVG_CONFIDENCE*COUNT_PURCHA_A AS TOTAL_SCORE  
    FROM(
    SELECT CUST_ID,AVG(CONFIDENCE_A) AS AVG_CONFIDENCE,COUNT(DISTINCT PURCHA_A) AS COUNT_PURCHA_A
    FROM(
    SELECT R.CUST_ID,R.BSN_DT,R.PRDT_CD,P.PRDT_CD,P.PRDT_GCODE_CD,P.PRDT_MCODE_CD,P.PRDT_DCODE_CD,D.PRDT_DCODE_NM,N.A_PRDT_DCODE_CD,N.PRDT_DCODE_NM_A,N.B_PRDT_DCODE_CD,N.PRDT_DCODE_NM_B,N.CONFIDENCE,
          CASE WHEN A_PRDT_DCODE_CD IS NULL THEN '' ELSE R.BSN_DT END AS PURCHA_A,
          CASE WHEN A_PRDT_DCODE_CD IS NULL THEN 0 ELSE N.CONFIDENCE END AS CONFIDENCE_A
    FROM CDS_DW.TB_DW_RCIPT_DETAIL AS R
        LEFT JOIN CDS_DW.TB_DW_PRDT_MASTR AS P
        ON R.PRDT_CD = P.PRDT_CD
        AND R.BIZTP_CD=P.BIZTP_CD
        AND R.AFLCO_CD=P.AFLCO_CD
        LEFT JOIN CDS_DW.TB_DW_PRDT_DCODE_CD AS D
        ON R.BIZTP_CD=D.BIZTP_CD
        AND R.AFLCO_CD=D.AFLCO_CD
        AND P.PRDT_DCODE_CD = D.PRDT_DCODE_CD
        LEFT JOIN 
                                        (SELECT A_PRDT_DCODE_CD,PRDT_DCODE_NM_A,B_PRDT_DCODE_CD,PRDT_DCODE_NM_B,CONFIDENCE,LIFT,JACARDS_3M,ODDS_RATIO_TRAN,ODDS_RATIO_YEAR,CONFIDENCE_CR,LIFT_CR,JACARDS_3M_CR,ODDS_RATIO_TRAN_CR,ODDS_RATIO_YEAR_CR,CONFIDENCE_PASS_FAIL,LIFT_PASS_FAIL,JACARDS_3M_PASS_FAIL,ODDS_RATIO_TRAN_PASS_FAIL,ODDS_RATIO_YEAR_PASS_FAIL
                                         FROM(
                                        SELECT BIZTP_CD,AGE_DIV,GNDR_CD,A_PRDT_DCODE_CD,PRDT_DCODE_NM_A,B_PRDT_DCODE_CD,PRDT_DCODE_NM_B,CONFIDENCE,LIFT,JACARDS_3M,ODDS_RATIO_TRAN,ODDS_RATIO_YEAR,CONFIDENCE_CR,LIFT_CR,JACARDS_3M_CR,ODDS_RATIO_TRAN_CR,ODDS_RATIO_YEAR_CR,
                                               CASE WHEN CONFIDENCE >= CONFIDENCE_CR THEN 'PASS' ELSE 'FAIL' END AS CONFIDENCE_PASS_FAIL,
                                               CASE	WHEN LIFT >= LIFT_CR THEN 'PASS' ELSE 'FAIL' END AS LIFT_PASS_FAIL,
                                               CASE	WHEN JACARDS_3M >= JACARDS_3M_CR THEN 'PASS' ELSE 'FAIL' END AS JACARDS_3M_PASS_FAIL,
                                               CASE	WHEN ODDS_RATIO_TRAN >= ODDS_RATIO_TRAN_CR THEN 'PASS' ELSE 'FAIL' END AS ODDS_RATIO_TRAN_PASS_FAIL,
                                               CASE WHEN ODDS_RATIO_YEAR >= ODDS_RATIO_YEAR_CR THEN 'PASS' ELSE 'FAIL' END AS ODDS_RATIO_YEAR_PASS_FAIL
                                    FROM(
                                    SELECT BIZTP_CD,AGE_DIV,GNDR_CD,A_PRDT_DCODE_CD,PRDT_DCODE_NM_A,B_PRDT_DCODE_CD,PRDT_DCODE_NM_B,CONFIDENCE,LIFT,JACARDS_3M,ODDS_RATIO_TRAN,ODDS_RATIO_YEAR,AVG(CONFIDENCE*{FILTER_CRITERIA}) OVER() AS CONFIDENCE_CR, AVG(LIFT*{FILTER_CRITERIA}) OVER() AS LIFT_CR,AVG(JACARDS_3M*{FILTER_CRITERIA}) OVER() AS JACARDS_3M_CR,AVG(ODDS_RATIO_TRAN*{FILTER_CRITERIA}) OVER() AS ODDS_RATIO_TRAN_CR,AVG(ODDS_RATIO_YEAR*{FILTER_CRITERIA}) OVER() AS ODDS_RATIO_YEAR_CR	
                                    FROM
                                    (SELECT D.BIZTP_CD,D.AGE_DIV,D.GNDR_CD,D.A_PRDT_DCODE_CD,C.PRDT_DCODE_NM AS PRDT_DCODE_NM_A,A.PRDT_DCODE_NM AS PRDT_DCODE_NM_B,D.B_PRDT_DCODE_CD,D.SUPPORT,D.CONFIDENCE,D.LIFT,D.JACARDS_3M,D.ODDS_RATIO_TRAN,D.ODDS_RATIO_YEAR
                                    FROM CDS_AMT.TB_AMT_PRDT_PRDT_DNA_DATA AS D
                                    LEFT JOIN CDS_DW.TB_DW_PRDT_MASTR AS P
                                    ON D.BIZTP_CD=P.BIZTP_CD
                                    AND D.A_PRDT_DCODE_CD=P.PRDT_DCODE_CD 
                                    LEFT JOIN CDS_DW.TB_DW_PRDT_DCODE_CD AS C
                                    ON P.BIZTP_CD = C.BIZTP_CD 
                                    AND P.PRDT_DI_CD =C.PRDT_DI_CD 
                                    AND D.A_PRDT_DCODE_CD = C.PRDT_DCODE_CD
                                    LEFT JOIN (SELECT PRDT_DCODE_CD,PRDT_DCODE_NM,COUNT_RCIPT_NO,PERCENT_GRADE --A급 상품 추출
                                    FROM (
                                    SELECT PRDT_DCODE_CD,PRDT_DCODE_NM,COUNT_RCIPT_NO,
                                           CUME_DIST() OVER (ORDER BY COUNT_RCIPT_NO DESC) AS PERCENT_GRADE --RCIPT_NO 기준 Percentile 산출
                                    FROM (
                                    SELECT DISTINCT PRDT_DCODE_CD,PRDT_DCODE_NM,
                                           COUNT(DISTINCT RCIPT_NO) OVER (PARTITION BY PRDT_DCODE_CD) AS COUNT_RCIPT_NO --소분류 기준 영수증건수 COUNT
                                    FROM(
                                    SELECT CUST_ID,BSN_DT,RCIPT_NO,PRDT_CD,PRDT_DCODE_CD,PRDT_DCODE_NM,NUM_VISIT_MONTH --주이용 고객 산출
                                    FROM(
                                    SELECT R.CUST_ID,R.BSN_DT,R.RCIPT_NO,N.PRDT_CD,F.PRDT_DCODE_CD,F.PRDT_DCODE_NM,
                                           COUNT(DISTINCT R.BSN_DT) OVER(PARTITION BY R.CUST_ID) AS NUM_VISIT_MONTH
                                    FROM CDS_DW.TB_DW_RCIPT_DETAIL AS R
                                        LEFT JOIN CDS_DW.TB_DW_PRDT_MASTR AS P
                                        ON R.PRDT_CD = P.PRDT_CD
                                        AND R.BIZTP_CD=P.BIZTP_CD
                                        AND R.AFLCO_CD=P.AFLCO_CD
                                        LEFT JOIN CDS_DW.TB_DW_PRDT_DCODE_CD AS D
                                        ON R.BIZTP_CD=D.BIZTP_CD
                                        AND R.AFLCO_CD=D.AFLCO_CD
                                        AND P.PRDT_DCODE_CD = D.PRDT_DCODE_CD
                                        LEFT JOIN ({PEACOCK_PRODUCT}) AS N
                                        ON P.PRDT_CD = N.PRDT_CD
                                        LEFT JOIN CDS_DW.TB_DW_PRDT_DCODE_CD AS F
                                        ON P.PRDT_DCODE_CD=F.PRDT_DCODE_CD 
                                        WHERE P.AFLCO_CD ='001'
                                        AND P.BIZTP_CD ='10'
                                        AND D.AFLCO_CD ='001'
                                        AND D.BIZTP_CD ='10'
                                        AND N.PRDT_CD IS NOT NULL
                                        AND R.CUST_ID IS NOT NULL
                                        AND {DATE} --상품연관성 측정 시 A급 고객 추출 산정 기간 
                                        AND R.AFLCO_CD ='001'
                                        AND R.BIZTP_CD ='10'
                                        AND R.RL_SALE_TRGT_YN = 'Y'
                                        AND F.AFLCO_CD ='001'
                                        AND F.BIZTP_CD ='10'
                                        GROUP BY R.CUST_ID,R.BSN_DT,R.RCIPT_NO,N.PRDT_CD,F.PRDT_DCODE_CD,F.PRDT_DCODE_NM)
                                        WHERE NUM_VISIT_MONTH >= {VISIT_NUM})
                                        GROUP BY PRDT_DCODE_CD,PRDT_DCODE_NM,RCIPT_NO
                                        ORDER BY COUNT_RCIPT_NO DESC))
                                        WHERE PERCENT_GRADE <= {COUNT_RCIPT_NUM}) AS A --영수증 건수 기준 상위 70% A급 상품
                                        ON A.PRDT_DCODE_CD=D.B_PRDT_DCODE_CD
                                    WHERE D.AFLCO_CD ='001'
                                    AND D.YM_WCNT = '{YM_WCNT}'
                                    AND P.AFLCO_CD='001'
                                    AND P.BIZTP_CD ='10'
                                    AND C.AFLCO_CD ='001'
                                    AND C.BIZTP_CD ='10'
                                    AND D.AFLCO_CD ='001'
                                    AND D.BIZTP_CD ='10'
                                    AND P.PRDT_DI_CD IS NOT NULL
                                    AND CONFIDENCE IS NOT NULL
                                    AND D.AGE_DIV ='전체'
                                    AND D.GNDR_CD ='전체'
                                    AND A.PRDT_DCODE_CD IS NOT NULL
                                    GROUP BY D.BIZTP_CD,D.AGE_DIV,D.GNDR_CD,D.A_PRDT_DCODE_CD,C.PRDT_DCODE_NM,A.PRDT_DCODE_NM,D.B_PRDT_DCODE_CD,D.SUPPORT,D.CONFIDENCE,D.LIFT,D.JACARDS_3M,D.ODDS_RATIO_TRAN,D.ODDS_RATIO_YEAR)
                                    GROUP BY BIZTP_CD,AGE_DIV,GNDR_CD,A_PRDT_DCODE_CD,PRDT_DCODE_NM_A,B_PRDT_DCODE_CD,PRDT_DCODE_NM_B,CONFIDENCE,LIFT,JACARDS_3M,ODDS_RATIO_TRAN,ODDS_RATIO_YEAR
                                    ORDER BY B_PRDT_DCODE_CD))
                                    WHERE  CONFIDENCE_PASS_FAIL= 'PASS'  --상품 FILTER 기준 PASS OR FAIL 선정
                                    AND  LIFT_PASS_FAIL='PASS'
                                    AND  JACARDS_3M_PASS_FAIL  ='PASS'
                                    AND  ODDS_RATIO_TRAN_PASS_FAIL  ='PASS'
                                    AND  ODDS_RATIO_YEAR_PASS_FAIL ='PASS') AS N
                                    ON P.PRDT_DCODE_CD=N.A_PRDT_DCODE_CD --구매 코드와 A급 상품 코드 일치
    WHERE R.CUST_ID IS NOT NULL
    AND {DATE} --스코어 고객 산정기간
    AND R.AFLCO_CD ='001'
    AND R.BIZTP_CD ='10'
    AND R.RL_SALE_TRGT_YN = 'Y'
    ORDER BY CUST_ID)
    GROUP BY CUST_ID)))
    GROUP BY CUST_ID,F_TOTAL_SCORE,PERCENT_GRADE)
    WHERE PERCENT_GRADE < 0.9)  --상위 10% 구간 CRITERIA 선정
    GROUP BY CUST_ID,'{YM_WCNT}',F_TOTAL_SCORE,M_TOTAL_SCORE,PERCENT_GRADE
    ORDER BY F_TOTAL_SCORE DESC'''

    PEACOCK=query_SQL( query1 )
    
    del_sql = f'''DELETE FROM {result_tb}  WHERE YM_WCNT='{YM_WCNT}' '''
    execute_query(del_sql)
    insert_table(result_tb,PEACOCK)
    end_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    log_df = add_logs(module = '행사유사도(피코크)',mdl_type='운영',step='1.APPLY 테이블 생성 및 결과적재',qt='Insert',tt=result_tb,
                      td=target_dt,st=now_date,et=end_date)
    insert_table('CDS_AMT.TB_AMT_CAMPAIGN_ANL_LOG' ,log_df)
except Exception as e:
    error = str(e)
    logger.error(error)
    end_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    log_df = add_logs(module = '행사유사도(피코크)',mdl_type='운영',step='error',qt='error',tt='==== error occured ====',
                      td=datetime.now().strftime('%Y-%m-%d'),st=now_date,et=end_date,ec=1,es=error)
    insert_table('CDS_AMT.TB_AMT_CAMPAIGN_ANL_LOG' ,log_df)
