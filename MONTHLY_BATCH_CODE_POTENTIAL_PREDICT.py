import configparser
conf_dir = '/home/cdsadmin/AMT/src/conf/config.ini'
cfg      = configparser.ConfigParser(interpolation=configparser.ExtendedInterpolation())
cfg.read(conf_dir)
#---------------------------------------------------------
# 1) 테이블 생성 할 HANA DB서버 접속정보
global HOST,PORT,DB_ID,DB_PW
HOST = cfg['dbconnect']['host']
PORT = int(cfg['dbconnect']['port'])
DB_ID = cfg['dbconnect']['ID']
DB_PW = cfg['dbconnect']['PW']

# 2) 예측 대상 데이터 및 결과 테이블명
TB_NM1 = cfg['Potential']['APPLY_TB']
TB_NM2 = cfg['Potential']['RESULT_TB']

# 3) 학습용 모델 저장위치
DIR_NM =   cfg['Potential']['MODEL_DIR'] 
MODEL1_FILE =  cfg['Potential']['MODEL1_FILE']  #적합용 모델 pickle 파일 명칭 1개월 후 예측
MODEL2_FILE =  cfg['Potential']['MODEL2_FILE'] #적합용 모델 pickle 파일 명칭 2개월 후 예측
MODEL3_FILE =  cfg['Potential']['MODEL3_FILE'] #적합용 모델 pickle 파일 명칭 3개월 후 예측
#----------------------------------------------------------

#----------------------------------------------------------
# ■ logging
#----------------------------------------------------------
import logging
import logging.config
from datetime import datetime
PYTHON_IP = '10.253.79.23'
MN = 'MONTHLY_BATCH_CODE_POTENTIAL_PREDICT.py'
LG = 'POTENTIAL'
# logging.config.fileConfig(str(cfg['common']['log_config']),defaults={'date':datetime.now()})
logger     = logging.getLogger('aml_log')
fh = logging.FileHandler('/home/cdsadmin/AMT/src/logs/AMS/{:%Y%m%d}_AMS.log'.format(datetime.now()))
formatter = logging.Formatter(f'\nERR|CDS|{PYTHON_IP}|AMT|1등급|[ %(asctime)s ]|[{MN}]|{LG}|[ LineNo. : %(lineno)d ]| %(message)s')
fh.setFormatter(formatter)
logger.addHandler(fh)
###==========================================================================###
# default package 
import pandas as pd
import numpy as np
import warnings
warnings.filterwarnings("ignore")
import time,os,psutil
import IPython
from IPython.lib import kernel
import collections
import joblib
import pickle
from hdbcli import dbapi 
from IPython.core.display import display, Image
import multiprocessing
import sqlite3
from dateutil.relativedelta import relativedelta
import argparse


def DB_Connection() :
    conn=dbapi.connect(HOST,PORT,DB_ID,DB_PW) #1) HANA DB서버 접속정보 위에서 선언
    return conn

#테이블 생성하는 SQL실행
def execute_query(query):
    conn = DB_Connection() 
    cur = conn.cursor()
    query = query
    cur.execute(query)
    conn.commit()
    conn.close()
    print("%s 실행완료"%query)

### 서버에 생성한 Apply용 테이블 불러오기(multiprocessing)
## multiprocessing 준비
def timecheck(start):
    learn_time = time.time() - start
    train_time = round(learn_time/60)  
    print("소요시간 :",  round(learn_time,-1), "초", "  (",train_time, "분 )\n")
    return learn_time,train_time

def Datachunk_range(rownum_sql) :
    conn = DB_Connection()
    cnt = pd.read_sql(rownum_sql, conn)
    key = int(cnt.values)
    rm  = key % n_core
    ## Split row_num / n_core 
    arg_list_flt =[i for i in range(0,key,int(key/n_core))]
    ## last sequence add remainder 
    if rm > 0:
        arg_list_flt[len(arg_list_flt)-1] = arg_list_flt[len(arg_list_flt)-1]+rm
    else : 
        arg_list_flt.append(arg_list_flt[-1]+arg_list_flt[1])
    conn.close()        
    return arg_list_flt,rm

def SQL_worker(num):    
    b = int(arg_list.index(num))-1
    if (int(arg_list.index(num)) == 0) :
        result = None
    else :
        start_num = int(arg_list[b])+1
        end_num   = int(num)
        print('start_num : %s | end_num : %s'%(start_num,end_num))
        sql =f'''
                SELECT  *
                FROM (SELECT   ROW_NUMBER() OVER (ORDER BY CUST_ID ASC) AS ROW_NUM
                             , *
                      FROM  {db_name}) T1
                WHERE T1.ROW_NUM between {start_num} and {end_num}
              '''
        conn = DB_Connection()
        result = pd.read_sql(sql, conn)
        conn.close()
    return result

def multiprocesser(arg_list) : 
    print('\nstart_arg_list : %s \n'%arg_list)
    p = multiprocessing.Pool(processes = n_core)
    data = p.map(SQL_worker, arg_list) 
    p.close()
    p.join()
    result = pd.concat(data)
    return result   

def check_cpu_percent(num):
    cpu_cnt = len(psutil.Process().cpu_affinity())
    c_list = psutil.cpu_percent(percpu=True)
    available_cpu_cnt = len([i for i in c_list if i < 1])
    n_core = np.trunc(available_cpu_cnt/num)
    print('[ total_cpu_cnt ] : ',cpu_cnt)
    print('[ available_cpu_cnt ] : ',available_cpu_cnt)
    return int(n_core)

#적은 레벨을 가지고 있는 데이터 범주형 변수로 분류
def list_to_ch_lowlevel(df_train,unique):
    list_ch_lowlevel=[]
    col_train = list(set(list(df_train.columns)) - set(['CUST_ID']))
    for i in range(len(col_train)):  
        if len(df_train[col_train[i]].unique()) < unique:
            list_ch_lowlevel.append(col_train[i])
        else:
            pass
  #  print(list_ch_lowlevel)
    return(list_ch_lowlevel)

#숫자형으로 변환
def feature_to_int(df_train,feature_ch ):
    col_train = list(set(list(df_train.columns)) - set(['CUST_ID']))
    df_train[col_train] = df_train[col_train].astype('float64')
    df_train[feature_ch] = df_train[feature_ch].astype('int64')
    return(df_train)

### 모델 예측결과 과정 오류 확인
def mdl_error_check(model_predict):
    pred_df=data_modi1_raw[data_modi1_raw['BAIN_GRADE_CD'].isin(list(model_predict['BAIN_GRADE_CD'].unique()))]
    check1=len(model_predict)==len(pred_df)
    check2=model_predict['BAIN_GRADE_YM'][1]==YM_BAIN
    check3=pred_df.iloc[0]['BAIN_GRADE_YM']==YM_BAIN
    model_predict['PREDICT_PROB_GROUP'] = model_predict['PREDICT_PROB_GROUP'].fillna(0)
    check4=model_predict['PREDICT_PROB_GROUP'].sum() > 0
    
    if (check1 == 1 & check2 == 1 & check3 == 1 & check4 == 1):
        error_mdl=0
        error_msg='이상 없음'
    elif check1 == 0:
        error_mdl=1
        error_msg='오류1 : 대상 고객 데이터와 예측결과 고객 수 불일치'
    elif check2 == 0:
        error_mdl=2
        error_msg='오류2 : 예측결과 데이터가 설정한 버전이 아님'
    elif check3 == 0:
        error_mdl=3
        error_msg='오류3 : 불러온 데이터가 설정한 버전이 아님'
    else:
        error_mdl=4
        error_msg='오류4 : 예측 결과 이상, 모두 0'    
    return(error_mdl, error_msg)

## 모델 결과 SAP HANA DB 지정 테이블에 추가
def del_dup_insert_table(table_name,df):
    conn = DB_Connection()
    cur = conn.cursor()
    input_data = [tuple(x)for x in df.values]
    cols_len = '?,'*int(len(df.columns))
    cols_len = cols_len[:-1]
    
    query_del_dup = f'''DELETE FROM {TB_NM2} WHERE BAIN_GRADE_YM='{YM_BAIN}' '''
    execute_query(query_del_dup)
    
    query    = '''INSERT INTO %s VALUES(%s)'''%(table_name,cols_len)
    print(query)
    cur.executemany(query, input_data)
    conn.commit()
    conn.close()
    return print("%s 테이블 데이터 입력완료"%table_name) 

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


#■ ---------------------------------------------------------------------------------■
try : 
    parser = argparse.ArgumentParser(description = 'POTENTIAL MODEL')
    parser.add_argument('--YM_BAIN', help = '예측 시행년월')
    args = parser.parse_args()
    YM_BAIN = args.YM_BAIN
except : 
    YM_BAIN = None
    
if (YM_BAIN == None) | (YM_BAIN == 'None'):
    # 모델 기준 학습월 과거 시점 설정 : 1은 1개월 전,
    BF_MONTHS=1
    YM_BAIN = (datetime.now() + relativedelta(months=-BF_MONTHS)).strftime('%Y%m')
    print(YM_BAIN, '베인등급 기준 미래 3개월 이탈 예측을 진행합니다.')

#■ ---------------------------------------------------------------------------------■
 
# YM_BAIN = '202009' 

###  베인등급 산정대상 확인(시스템 실행일자 기준 전월)
###  시스템 저장 및 프로그램 불러오기 위치 설정
##   1. 이탈 예측 모형 : Apply용  데이터 생성 및 불러오기

#     - 학습에 사용한 모델의 학습 테이블에서 타겟변수만 제외한 형태로 생성

### 모델Apply용 테이블 생성 :  ★ 학습데이터 대상변수 항목 변경 시 함께 수정 필수 ★
#    - 영수증 및 DNA 테이블을 사용하여 SAP HANA 서버에 Apply용 테이블을 생성
#    - 기존 테이블을 삭제 후 생성하여, 최신 데이터만 적재
#    - 학습에 사용된 테이블과 같은 예측변수를 가지고 있어야 함.
#    - 불러오기는 multi processing 사용

#서버에 물리테이블 생성하는 SQL : 위에 설정한 Bain등급 기준월에 맞춰 사용하는 영수증 및 DNA기간 지정 됨.
sql1 = f'''
TRUNCATE TABLE {TB_NM1}
'''

sql2 = f'''
INSERT INTO {TB_NM1}  
(WITH POTENTIAL_TARGET AS (
SELECT '{YM_BAIN}' AS BAIN_GRADE_YM , YM_WCNT, A.CUST_ID,A.GRADE_CD  FROM  CDS_DW.TB_BAIN_MEMBR_GRADE A
JOIN 
(SELECT  CUST_ID, YM_WCNT FROM  CDS_AMT.TB_AMT_BIZTP_CUST_DNA_DATA  WHERE BIZTP_CD = '10' AND TO_CHAR(YM_WCNT ,'YYYYMM')  = to_char(ADD_MONTHS('{YM_BAIN}', +1),'YYYYMM') AND RFM_LV > 0) B
on A.CUST_ID = B.CUST_ID 
WHERE A.GRADE_YM = '{YM_BAIN}'  
),

RCIPT_DETAIL AS  (
			     SELECT B.CUST_ID
			     ,A.BSN_DT
			     ,D.PRDT_DI_CD
			     ,SUM(A.SALE_AMT)  AS SALE_AMT
				 FROM  CDS_DW.TB_DW_RCIPT_DETAIL A
				 JOIN POTENTIAL_TARGET B ON A.CUST_ID = B.CUST_ID
				 JOIN( SELECT PRDT_CD ,PRDT_DCODE_CD
				       FROM CDS_DW.TB_DW_PRDT_MASTR 
				       WHERE AFLCO_CD = '001' AND BIZTP_CD = '10') C ON A.PRDT_CD  = C.PRDT_CD
		         JOIN( SELECT PRDT_DCODE_CD,PRDT_DI_CD
				   	   FROM CDS_DW.TB_DW_PRDT_DCODE_CD
				   	   WHERE AFLCO_CD = '001' AND BIZTP_CD = '10') D ON C.PRDT_DCODE_CD  = D.PRDT_DCODE_CD 
				 WHERE TO_CHAR( BSN_DT,'YYYYMM') BETWEEN TO_CHAR(ADD_MONTHS('{YM_BAIN}',-5),'YYYYMM') AND TO_CHAR(ADD_MONTHS('{YM_BAIN}', +0),'YYYYMM')
				   AND A.AFLCO_CD ='001' 
				   AND A.BIZTP_CD ='10'
				   AND A.SALE_TRGT_YN    = 'Y'               
				   AND A.RL_SALE_TRGT_YN = 'Y'               
				   AND A.SALE_AMT > 0
				   AND A.CUST_ID IS NOT NULL
				 GROUP BY 
				 B.CUST_ID
				 ,A.BSN_DT
			     ,D.PRDT_DI_CD
			   ),
		   
SUMMARY_RCIPT_DETAIL   AS  (
				SELECT  CUST_ID
				      ,COUNT(DISTINCT BSN_DT)                                                  AS CNT_TOT_6M				      
 				      ,SUM(SALE_AMT)                                                           AS AMT_TOT_6M      
				      ,COUNT(DISTINCT CASE WHEN TO_CHAR(BSN_DT,'YYYYMM') BETWEEN TO_CHAR(ADD_MONTHS('{YM_BAIN}',-2),'YYYYMM') AND TO_CHAR(ADD_MONTHS('{YM_BAIN}',-0),'YYYYMM') THEN  BSN_DT ELSE NULL END) AS CNT_Q1  
				      ,COUNT(DISTINCT CASE WHEN TO_CHAR(BSN_DT,'YYYYMM') BETWEEN TO_CHAR(ADD_MONTHS('{YM_BAIN}',-5),'YYYYMM') AND TO_CHAR(ADD_MONTHS('{YM_BAIN}',-3),'YYYYMM') THEN  BSN_DT ELSE NULL END) AS CNT_Q2
				      ,SUM( CASE WHEN TO_CHAR(BSN_DT,'YYYYMM') BETWEEN TO_CHAR(ADD_MONTHS('{YM_BAIN}',-2),'YYYYMM') AND TO_CHAR(ADD_MONTHS('{YM_BAIN}',-0),'YYYYMM') THEN SALE_AMT ELSE 0 END) AS AMT_Q1
				      ,SUM( CASE WHEN TO_CHAR(BSN_DT,'YYYYMM') BETWEEN TO_CHAR(ADD_MONTHS('{YM_BAIN}',-5),'YYYYMM') AND TO_CHAR(ADD_MONTHS('{YM_BAIN}',-3),'YYYYMM') THEN  SALE_AMT ELSE 0 END) AS AMT_Q2
				      
				      ,COUNT(DISTINCT CASE WHEN TO_CHAR(BSN_DT,'YYYYMM') = TO_CHAR(ADD_MONTHS('{YM_BAIN}',-0),'YYYYMM') THEN  BSN_DT ELSE NULL END) AS CNT_BF0M --베인등급 기준 산정월 기준 당월 실적
				      ,SUM( CASE WHEN TO_CHAR(BSN_DT,'YYYYMM') = TO_CHAR(ADD_MONTHS('{YM_BAIN}',-0),'YYYYMM')THEN SALE_AMT ELSE 0 END) AS AMT_BF0M  --베인등급 기준 산정월 기준 당월 실적
				     
				FROM RCIPT_DETAIL 
				GROUP BY CUST_ID
)

SELECT
		DF0.BAIN_GRADE_YM
	  , DF0.YM_WCNT AS DNA_YM_WCNT
	  , DF0.CUST_ID
  	  , COALESCE(DF0.GRADE_CD,'99') AS BAIN_GRADE_CD
	  , t4.GRADE_NM
      
--2) 성장가능성1 : 이용규모 성장성연계 DNA, 누계 실적 및 RFM 증감율
	  , CASE WHEN COALESCE(CNT_Q1,0) > 0 AND COALESCE(CNT_Q2,0)=0 THEN 1 ELSE COALESCE(COALESCE(CNT_Q1,0)/NULLIF(CNT_Q2,0) - 1,0) END AS CNT_RT_Q1_Q2 
	  , CASE WHEN COALESCE(DF1.AMT_Q1/NULLIF(DF1.CNT_Q1,0),0) > 0 AND COALESCE(DF1.AMT_Q2/NULLIF(DF1.CNT_Q2,0),0)=0 THEN 1 ELSE  COALESCE(COALESCE(DF1.AMT_Q1/NULLIF(DF1.CNT_Q1,0),0)/(DF1.AMT_Q2/NULLIF(DF1.CNT_Q2,0)) - 1,0) END AS DAVG_AMT_RT_Q1_Q2 

--	  , CASE WHEN COALESCE(t1.PURCHS_CYCLE,0) > 0 AND COALESCE(t5_1.PURCHS_CYCLE_BF3M,0)=0 THEN 1 ELSE COALESCE(COALESCE(t1.PURCHS_CYCLE,0)/NULLIF(t5_1.PURCHS_CYCLE_BF3M,0) - 1,0) END AS PURCHS_CYCLE_RT_BF_0M_3M
	  , CASE WHEN COALESCE(t1.PURCHS_CYCLE,0) > 0 AND COALESCE(t5_2.PURCHS_CYCLE_BF5M,0)=0 THEN 1 ELSE COALESCE(COALESCE(t1.PURCHS_CYCLE,0)/NULLIF(t5_2.PURCHS_CYCLE_BF5M,0) - 1,0) END AS PURCHS_CYCLE_RT_BF_0M_5M
      , COALESCE(t1.PURCHS_VISIT_CHG_RT_AVG_6M,0) AS PURCHS_VISIT_CHG_RT_AVG_6M --연간 6개월간 구매횟수 증감율 평균
	  
/***************************************************************************************************/	  
/** 상품분류체계 변경 시 변경 부분1 시작 : 사용 항목 명칭 담당 코드에 맞게 삭제 및 추가**/     
		, CASE WHEN COALESCE(t1.RFM_LV,0) > 0 AND COALESCE(t5_1.RFM_LV_BF3M,0)=0 THEN 1 ELSE COALESCE(COALESCE(t1.RFM_LV,0)/NULLIF(t5_1.RFM_LV_BF3M,0) - 1,0) END AS RFM_LV_RT_BF_0M_3M
		, CASE WHEN COALESCE(t1.RFM_R_SCORE,0) > 0 AND COALESCE(t5_1.RFM_R_SCORE_BF3M,0)=0 THEN 1 ELSE COALESCE(COALESCE(t1.RFM_R_SCORE,0)/NULLIF(t5_1.RFM_R_SCORE_BF3M,0) - 1,0) END AS RFM_R_SCORE_RT_BF_0M_3M
--		, CASE WHEN COALESCE(t1.RFM_F_SCORE,0) > 0 AND COALESCE(t5_1.RFM_F_SCORE_BF3M,0)=0 THEN 1 ELSE COALESCE(COALESCE(t1.RFM_F_SCORE,0)/NULLIF(t5_1.RFM_F_SCORE_BF3M,0) - 1,0) END AS RFM_F_SCORE_RT_BF_0M_3M
		, CASE WHEN COALESCE(t1.RFM_M_SCORE,0) > 0 AND COALESCE(t5_1.RFM_M_SCORE_BF3M,0)=0 THEN 1 ELSE COALESCE(COALESCE(t1.RFM_M_SCORE,0)/NULLIF(t5_1.RFM_M_SCORE_BF3M,0) - 1,0) END AS RFM_M_SCORE_RT_BF_0M_3M

		, CASE WHEN COALESCE(t1.RFM_LV_DI_FRESH1,0) > 0 AND COALESCE(t5_1.RFM_LV_DI_FRESH1_BF3M,0)=0 THEN 1 ELSE COALESCE(COALESCE(t1.RFM_LV_DI_FRESH1,0)/NULLIF(t5_1.RFM_LV_DI_FRESH1_BF3M,0) - 1,0) END AS RFM_LV_DI_FRESH1_RT_BF_0M_3M
--		, CASE WHEN COALESCE(t1.RFM_LV_DI_FRESH2,0) > 0 AND COALESCE(t5_1.RFM_LV_DI_FRESH2_BF3M,0)=0 THEN 1 ELSE COALESCE(COALESCE(t1.RFM_LV_DI_FRESH2,0)/NULLIF(t5_1.RFM_LV_DI_FRESH2_BF3M,0) - 1,0) END AS RFM_LV_DI_FRESH2_RT_BF_0M_3M
--		, CASE WHEN COALESCE(t1.RFM_LV_DI_PEACOCK,0) > 0 AND COALESCE(t5_1.RFM_LV_DI_PEACOCK_BF3M,0)=0 THEN 1 ELSE COALESCE(COALESCE(t1.RFM_LV_DI_PEACOCK,0)/NULLIF(t5_1.RFM_LV_DI_PEACOCK_BF3M,0) - 1,0) END AS RFM_LV_DI_PEACOCK_RT_BF_0M_3M
--		, CASE WHEN COALESCE(t1.RFM_LV_DI_PRCS,0) > 0 AND COALESCE(t5_1.RFM_LV_DI_PRCS_BF3M,0)=0 THEN 1 ELSE COALESCE(COALESCE(t1.RFM_LV_DI_PRCS,0)/NULLIF(t5_1.RFM_LV_DI_PRCS_BF3M,0) - 1,0) END AS RFM_LV_DI_PRCS_RT_BF_0M_3M
-- 		, CASE WHEN COALESCE(t1.RFM_LV_DI_HNR,0) > 0 AND COALESCE(t5_1.RFM_LV_DI_HNR_BF3M,0)=0 THEN 1 ELSE COALESCE(COALESCE(t1.RFM_LV_DI_HNR,0)/NULLIF(t5_1.RFM_LV_DI_HNR_BF3M,0) - 1,0) END AS RFM_LV_DI_HNR_RT_BF_0M_3M
--		, CASE WHEN COALESCE(t1.RFM_LV_DI_LIVING,0) > 0 AND COALESCE(t5_1.RFM_LV_DI_LIVING_BF3M,0)=0 THEN 1 ELSE COALESCE(COALESCE(t1.RFM_LV_DI_LIVING,0)/NULLIF(t5_1.RFM_LV_DI_LIVING_BF3M,0) - 1,0) END AS RFM_LV_DI_LIVING_RT_BF_0M_3M
--		, CASE WHEN COALESCE(t1.RFM_LV_DI_MOLLYS,0) > 0 AND COALESCE(t5_1.RFM_LV_DI_MOLLYS_BF3M,0)=0 THEN 1 ELSE COALESCE(COALESCE(t1.RFM_LV_DI_MOLLYS,0)/NULLIF(t5_1.RFM_LV_DI_MOLLYS_BF3M,0) - 1,0) END AS RFM_LV_DI_MOLLYS_RT_BF_0M_3M
--		, CASE WHEN COALESCE(t1.RFM_LV_DI_ELEC_CULTR,0) > 0 AND COALESCE(t5_1.RFM_LV_DI_ELEC_CULTR_BF3M,0)=0 THEN 1 ELSE COALESCE(COALESCE(t1.RFM_LV_DI_ELEC_CULTR,0)/NULLIF(t5_1.RFM_LV_DI_ELEC_CULTR_BF3M,0) - 1,0) END AS RFM_LV_DI_ELEC_CULTR_RT_BF_0M_3M
--		, CASE WHEN COALESCE(t1.RFM_LV_DI_FSHN,0) > 0 AND COALESCE(t5_1.RFM_LV_DI_FSHN_BF3M,0)=0 THEN 1 ELSE COALESCE(COALESCE(t1.RFM_LV_DI_FSHN,0)/NULLIF(t5_1.RFM_LV_DI_FSHN_BF3M,0) - 1,0) END AS RFM_LV_DI_FSHN_RT_BF_0M_3M


		, CASE WHEN COALESCE(t1.RFM_R_SCORE,0) > 0 AND COALESCE(t5_2.RFM_R_SCORE_BF5M,0)=0 THEN 1 ELSE COALESCE(COALESCE(t1.RFM_R_SCORE,0)/NULLIF(t5_2.RFM_R_SCORE_BF5M,0) - 1,0) END AS RFM_R_SCORE_RT_BF_0M_5M
		, CASE WHEN COALESCE(t1.RFM_F_SCORE,0) > 0 AND COALESCE(t5_2.RFM_F_SCORE_BF5M,0)=0 THEN 1 ELSE COALESCE(COALESCE(t1.RFM_F_SCORE,0)/NULLIF(t5_2.RFM_F_SCORE_BF5M,0) - 1,0) END AS RFM_F_SCORE_RT_BF_0M_5M		
		, CASE WHEN COALESCE(t1.RFM_M_SCORE,0) > 0 AND COALESCE(t5_2.RFM_M_SCORE_BF5M,0)=0 THEN 1 ELSE COALESCE(COALESCE(t1.RFM_M_SCORE,0)/NULLIF(t5_2.RFM_M_SCORE_BF5M,0) - 1,0) END AS RFM_M_SCORE_RT_BF_0M_5M
	
		
--2) 성장가능성1 : 이용실적 합계 및 순위
				      
--		,COALESCE(t1.RFM_LV,0) AS RFM_LV
		,COALESCE(t1.RFM_R_SCORE,0) AS RFM_R_SCORE
		,COALESCE(t1.RFM_F_SCORE,0) AS RFM_F_SCORE
		,COALESCE(t1.RFM_M_SCORE,0) AS RFM_M_SCORE
		
--		,COALESCE(t1.RFM_LV_DI_FRESH1,0) AS RFM_LV_DI_FRESH1
--		,COALESCE(t1.RFM_LV_DI_FRESH2,0) AS RFM_LV_DI_FRESH2
--		,COALESCE(t1.RFM_LV_DI_PEACOCK,0) AS RFM_LV_DI_PEACOCK
		,COALESCE(t1.RFM_LV_DI_PRCS,0) AS RFM_LV_DI_PRCS
--		,COALESCE(t1.RFM_LV_DI_HNR,0) AS RFM_LV_DI_HNR
--		,COALESCE(t1.RFM_LV_DI_LIVING,0) AS RFM_LV_DI_LIVING
--		,COALESCE(t1.RFM_LV_DI_MOLLYS,0) AS RFM_LV_DI_MOLLYS		
--		,COALESCE(t1.RFM_LV_DI_ELEC_CULTR,0) AS RFM_LV_DI_ELEC_CULTR
--		,COALESCE(t1.RFM_LV_DI_FSHN,0) AS RFM_LV_DI_FSHN		
		
		
/** 상품분류체계 변경 시 변경 부분1 끝 : 사용 항목 명칭 담당 코드에 맞게 삭제 및 추가**/     
/***************************************************************************************************/	  
		
		,COALESCE(DF1.CNT_TOT_6M,0) AS CNT_TOT_6M
		,COALESCE(DF1.CNT_Q1,0) AS CNT_Q1
		,COALESCE(DF1.CNT_Q2,0) AS CNT_Q2 
		,COALESCE(DF1.CNT_BF0M,0) AS CNT_BF0M 
		
		
		,COALESCE(DF1.AMT_TOT_6M/NULLIF(DF1.CNT_TOT_6M,0),0) AS DAVG_AMT_TOT_6M	 		
--	,COALESCE(DF1.AMT_Q1/NULLIF(DF1.CNT_Q1,0),0) AS DAVG_AMT_Q1	 
		,COALESCE(DF1.AMT_Q2/NULLIF(DF1.CNT_Q2,0),0) AS DAVG_AMT_Q2	 
--		,COALESCE(DF1.AMT_BF0M/NULLIF(DF1.CNT_BF0M,0),0) AS DAVG_AMT_BF0M 
	
		,CASE WHEN t1.PURCHS_CYCLE_ELAPSE > 180 OR t1.PURCHS_CYCLE_ELAPSE IS NULL THEN 180 ELSE t1.PURCHS_CYCLE_ELAPSE END AS PURCHS_CYCLE_ELAPSE -- 구매경과일 전처리 이곳에서 완료, 베인등급 산정고객 대상이므로 구매경과일 최대값 180~181이어야 정상. 180로 일괄 처리.
  
----3) 지속적 사용원인2 : 고객 구매 요인 유형	  
      , COALESCE(t1.PURCHS_WKD ,0) AS PURCHS_WKD 
      
FROM
POTENTIAL_TARGET AS DF0

JOIN TB_AMT_BIZTP_CUST_DNA_DATA t1 ON DF0.CUST_ID = t1.CUST_ID AND t1.AFLCO_CD ='001' AND t1.BIZTP_CD ='10' AND TO_CHAR(t1.YM_WCNT ,'YYYYMMDD')= CONCAT( TO_CHAR(ADD_MONTHS('{YM_BAIN}',+1),'YYYYMM'),'01') 

LEFT JOIN CDS_DW.TB_BAIN_MEMBR_GRADE_CD t4 ON DF0.GRADE_CD = t4.GRADE_CD 

-- 고객의  과거DNA 변화 비교용 : 해당 DNA만 가져옴


/***************************************************************************************************/
/** 상품분류체계 변경 시 변경 부분2 시작 : 사용 항목 명칭 담당 코드에 맞게 삭제 및 추가**/     
LEFT JOIN 
(SELECT CUST_ID

				, RFM_LV AS RFM_LV_BF3M
				, RFM_R_SCORE AS RFM_R_SCORE_BF3M 				
				, RFM_F_SCORE AS RFM_F_SCORE_BF3M
				, RFM_M_SCORE AS RFM_M_SCORE_BF3M 				
				
				, RFM_LV_DI_FRESH1  AS RFM_LV_DI_FRESH1_BF3M 
				, RFM_LV_DI_FRESH2  AS RFM_LV_DI_FRESH2_BF3M 
				, RFM_LV_DI_PEACOCK AS RFM_LV_DI_PEACOCK_BF3M	
				, RFM_LV_DI_PRCS	AS RFM_LV_DI_PRCS_BF3M 									
				, RFM_LV_DI_HNR	    AS RFM_LV_DI_HNR_BF3M 									
				, RFM_LV_DI_LIVING	AS RFM_LV_DI_LIVING_BF3M 
				, RFM_LV_DI_MOLLYS  AS RFM_LV_DI_MOLLYS_BF3M
				, RFM_LV_DI_ELEC_CULTR	AS RFM_LV_DI_ELEC_CULTR_BF3M 	 
				, RFM_LV_DI_FSHN AS RFM_LV_DI_FSHN_BF3M 

			    , PURCHS_CYCLE AS PURCHS_CYCLE_BF3M
				FROM TB_AMT_BIZTP_CUST_DNA_DATA 
				WHERE  AFLCO_CD ='001' AND BIZTP_CD ='10'  AND TO_CHAR(YM_WCNT ,'YYYYMMDD')= CONCAT(TO_CHAR(ADD_MONTHS('{YM_BAIN}',+1-3),'YYYYMM'),'01')) t5_1 
				ON DF0.CUST_ID = t5_1.CUST_ID 
				
/** 상품분류체계 변경 시 변경 부분2 끝 : 사용 항목 명칭 담당 코드에 맞게 삭제 및 추가**/    
/***************************************************************************************************/				
				
-- 고객의  과거 5개월 전 DNA정보 				
LEFT JOIN 
(SELECT CUST_ID
--				, RFM_LV AS RFM_LV_BF5M
				, RFM_R_SCORE AS RFM_R_SCORE_BF5M 				
				, RFM_F_SCORE AS RFM_F_SCORE_BF5M
				, RFM_M_SCORE AS RFM_M_SCORE_BF5M 
				
				, PURCHS_CYCLE AS PURCHS_CYCLE_BF5M
				FROM TB_AMT_BIZTP_CUST_DNA_DATA  
				WHERE  AFLCO_CD ='001' AND BIZTP_CD ='10'  AND TO_CHAR(YM_WCNT ,'YYYYMMDD')= CONCAT(TO_CHAR(ADD_MONTHS('{YM_BAIN}',+1-5),'YYYYMM'),'01')) t5_2 
				ON DF0.CUST_ID = t5_2.CUST_ID

-- 고객의  과거 6개월 이력기준 
LEFT JOIN SUMMARY_RCIPT_DETAIL  DF1 ON DF0.CUST_ID = DF1.CUST_ID   
)
'''

try:
    now_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    d_format = "%Y%m"
    datetime.strptime(YM_BAIN, d_format)
    target_dt = str((datetime.strptime(YM_BAIN, '%Y%m')+relativedelta(months=1)).strftime('%Y-%m-%d'))
    
    log_df = add_logs(module = '개인화모델(Potential)',mdl_type='운영',step='0.예측시작',qt='Create',tt=TB_NM2,
                  td=target_dt,st=now_date,et=now_date)
    insert_table('CDS_AMT.TB_AMT_CAMPAIGN_ANL_LOG' ,log_df)
    
    # 1) 데이터 생성 쿼리 실행
    execute_query(sql1)
    execute_query(sql2)

    # 2) 예측대상 데이터 명칭

    db_name  = f'''{TB_NM1}'''
    n_core = 5
    cnt_sql  = f''' SELECT COUNT(1)
                    FROM {db_name}'''
    arg_list,rm = Datachunk_range(rownum_sql = cnt_sql)
    data_modi1_raw = multiprocesser(arg_list)

    # ===============================================================
    ## 2. 데이터 전처리
    #     - 불러온 데이터 이상여부 확인
    #     - 범주형과 숫자형 구분, 변경

    ### 모델 Apply용 사용변수 확인
    #    - 범주형 변수 설정이 필요하므로, 예측에 사용된 변수 목록을 가져옴 
    #    - 사용할 모델에서 불러오는 방식 사용
    #    - 3개 모델 동일 변수 사용하므로 1개 모델에서 목록 불러와서 확인
    #    - 적은 레벨을 가지고 있는 데이터 범주형 변수로 분류(21년 1월 기준 모델에서는 범주형 변수는 없으나 코드는 유지)
    with open(f'{DIR_NM}{MODEL1_FILE}', 'rb') as f1:
        model_AF1M = pickle.load(f1)

    with open(f'{DIR_NM}{MODEL2_FILE}', 'rb') as f2:
        model_AF2M = pickle.load(f2)

    with open(f'{DIR_NM}{MODEL3_FILE}', 'rb') as f3:
        model_AF3M = pickle.load(f3)

    col=model_AF1M.feature_names_
    print("잠재우수 예측모델 사용 변수 :" ,len(col) ,"개")

    feature_ch=list_to_ch_lowlevel(data_modi1_raw[col],20)

    ## 모델 적용
    ### 모델용 데이터 변환 : cat boost용
    X1_apply = feature_to_int(data_modi1_raw[col],list(set(feature_ch)&set(col)))

    ### 모델 적용 및 예측결과 테이블 생성
    model_prob1 =  pd.DataFrame(model_AF1M.predict_proba(X1_apply, thread_count=5)[:,1],columns=['PREDICT_PROB_AF1M']) # 평가 데이터 예측
    model_prob2 =  pd.DataFrame(model_AF2M.predict_proba(X1_apply, thread_count=5)[:,1],columns=['PREDICT_PROB_AF2M']) # 평가 데이터 예측
    model_prob3 =  pd.DataFrame(model_AF3M.predict_proba(X1_apply, thread_count=5)[:,1],columns=['PREDICT_PROB_AF3M']) # 평가 데이터 예측

    #bain_grade['LV_G'] = bain_grade['LV_UPGRADE_YN'].apply(lambda x : '2상승'  if (x > 0) &(lgb_predict1['LV_DWGRADE_YN']==0) > 1 else '3유지')

    model_prob =  pd.concat([model_prob1,model_prob2,model_prob3],axis=1)#.drop(['index'],axis=1)

    model_prob['PREDICT_PROB_AF1M_YN'] = model_prob['PREDICT_PROB_AF1M'].apply(lambda x : 1 if x > 0.4 else 0)
    model_prob['PREDICT_PROB_AF2M_YN'] = model_prob['PREDICT_PROB_AF2M'].apply(lambda x : 1 if x > 0.4 else 0)
    model_prob['PREDICT_PROB_AF3M_YN'] = model_prob['PREDICT_PROB_AF3M'].apply(lambda x : 1 if x > 0.4 else 0) 
    model_prob['PREDICT_PROB_GROUP'] = np.where(model_prob['PREDICT_PROB_AF1M_YN'] + model_prob['PREDICT_PROB_AF2M_YN'] +model_prob['PREDICT_PROB_AF3M_YN'] > 1 ,1,0)

    model_prob.drop(['PREDICT_PROB_AF1M_YN','PREDICT_PROB_AF2M_YN','PREDICT_PROB_AF3M_YN'],axis=1,inplace=True)

    #생성 모델의 테스트 데이터 예측값과 리뷰용 프로파일링 정보 저장
    model1_predict1 = data_modi1_raw[['BAIN_GRADE_YM','CUST_ID','BAIN_GRADE_CD']].reset_index().drop(['index'],axis=1)
    model1_predict2 = pd.concat([model1_predict1,model_prob],axis=1)
    mask_dt ='%Y-%m-%d'
    model1_predict2['DATA_CRTN_DT'] =datetime.now().strftime(mask_dt)


    error ,error_msg = mdl_error_check(model1_predict2)

    error_df = pd.DataFrame(columns=[['Model','error_code', 'error_msg']])
    error_df.loc[1] = 'potential',error ,error_msg 

    if error==0:
        del_dup_insert_table(TB_NM2,model1_predict2)
    end_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    log_df = add_logs(module = '개인화모델(Potential)',mdl_type='운영',step='1.APPLY 테이블 생성 및 결과적재',qt='Insert',tt=TB_NM2,
                  td=target_dt,st=now_date,et=end_date)
    insert_table('CDS_AMT.TB_AMT_CAMPAIGN_ANL_LOG' ,log_df)
    
except Exception as e:
    error    = str(e)
    logger.error(error)
    end_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    log_df   = add_logs(module = '개인화모델(Potential)',mdl_type='운영',step='error',qt='error',tt='==== error occured ====',
                  td=datetime.now().strftime('%Y-%m-%d'),st=now_date,et=end_date,ec=1,es=error)
    insert_table('CDS_AMT.TB_AMT_CAMPAIGN_ANL_LOG' ,log_df)
