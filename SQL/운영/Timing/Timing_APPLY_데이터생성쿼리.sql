TRUNCATE TABLE CDS_AMT.TB_AMT_TIMING_APPLY; 
INSERT INTO CDS_AMT.TB_AMT_TIMING_APPLY
        WITH BASE_DNA_CUST_TB AS (
                            SELECT DISTINCT  A.CUST_ID
                                           , '202008' AS BAIN_GRADE_YM
                                           , MAIN_PURCHS_DCODE 
                                           , PREFER_PURCHS_DCODE
                                           , COALESCE(TOP1_STR_DSTNC,620)      AS  TOP1_STR_DSTNC
                                           , COALESCE(TOP2_STR_DSTNC,620)      AS  TOP2_STR_DSTNC
                                           , COALESCE(CLDR_EVENT_PRE_UNITY,0)  AS  CLDR_EVENT_PRE_UNITY
                                           , COALESCE(CLDR_EVENT_PRE_TREDI,0)  AS  CLDR_EVENT_PRE_TREDI 
                                           , COALESCE(PURCHS_WEND_HLDY,0)      AS  PURCHS_WEND_HLDY
                                           , PURCHS_CYCLE 
                                           , PURCHS_CYCLE_CHG 
                                           , PURCHS_CYCLE_REGUL_YN 
                                           , PURCHS_CYCLE_ARRVL_RT 
                                           , PURCHS_VISIT_CHG_RT_AVG_6M 
                                           , COALESCE(RFM_LV,0)                AS RFM_LV
                                           , COALESCE(RFM_F_SCORE,0)           AS RFM_F_SCORE
                                           , COALESCE(RFM_M_SCORE,0)           AS RFM_M_SCORE
                                           /***************************** 상품분류체계 변경시 수정해야하는 부분 **********************************/
                                           , COALESCE(RFM_LV_DI_FRESH1,0)      AS RFM_LV_DI_FRESH1
                                           , COALESCE(RFM_LV_DI_FRESH2,0)      AS RFM_LV_DI_FRESH2
                                           , COALESCE(RFM_LV_DI_PEACOCK,0)     AS RFM_LV_DI_PEACOCK
                                           , COALESCE(RFM_LV_DI_PRCS,0)        AS RFM_LV_DI_PRCS
                                           , COALESCE(RFM_LV_DI_HNR,0)         AS RFM_LV_DI_HNR
                                           , COALESCE(RFM_LV_DI_LIVING,0)      AS RFM_LV_DI_LIVING
                                           , COALESCE(RFM_LV_DI_MOLLYS,0)      AS RFM_LV_DI_MOLLYS
                                           , COALESCE(RFM_LV_DI_ELEC_CULTR,0)  AS RFM_LV_DI_ELEC_CULTR
                                           , COALESCE(RFM_LV_DI_FSHN,0)        AS RFM_LV_DI_FSHN
                                           /*****************************************************************************************************/
                                           , DAVG_PURCHS_AMT_EXC_ELEC 
                            FROM CDS_AMT.TB_AMT_BIZTP_CUST_DNA_DATA A 
                            JOIN CDS_DW.TB_BAIN_MEMBR_GRADE B ON A.CUST_ID  = B.CUST_ID AND B.GRADE_YM ='202008' 
                            WHERE YM_WCNT ='20200901'
                              AND AFLCO_CD ='001'
                              AND BIZTP_CD ='10'
                              AND PURCHS_CYCLE IS NOT NULL 
                             ),
            CUST_PRDT_DNA_TABLE AS (
                                    SELECT      CUST_ID
                                              , PRDT_DCODE_CD
                                              , PRDT_DI_CD
                                              , PURCHS_CYCLE
                                              , CASE WHEN PURCHS_CYCLE_ELAPSE = 0 THEN 1 ELSE PURCHS_CYCLE_ELAPSE END   AS PURCHS_CYCLE_ELAPSE
                                              , DNA_LAST_VISIT_DIFF
                                              , RECENT_LAST_VISIT_DIFF
                                              , DNA_LAST_VISIT
                                              , RECENT_LAST_VISIT
                                              , LAST_VISIT
                                              , CASE WHEN PURCHS_CYCLE_ELAPSE = 0
                                                     THEN ROUND((1/PURCHS_CYCLE),2)              
                                                     ELSE ROUND((PURCHS_CYCLE_ELAPSE/PURCHS_CYCLE),2) 
                                                     END 														        AS PRDT_ARRVL_RT 
                                              , CASE WHEN PURCHS_CYCLE_ELAPSE = 0
                                                     THEN ABS(1-(ROUND((1/PURCHS_CYCLE),2))) 
                                                     ELSE ABS(1-(ROUND((PURCHS_CYCLE_ELAPSE/PURCHS_CYCLE),2)))     
                                                     END                                                                AS PRDT_ARRVL_RT_DIFF_ABS
                                    FROM (
                                            SELECT    A.CUST_ID
                                                    , A.PRDT_DCODE_CD
                                                    , B.PRDT_DI_CD
                                                    , A.PURCHS_CYCLE
                                                    , (CASE WHEN A.PURCHS_CYCLE_ELAPSE IS NULL THEN 183 ELSE A.PURCHS_CYCLE_ELAPSE END)     AS DNA_LAST_VISIT_DIFF
                                                    , DAYS_BETWEEN(MAX(C.BSN_DT),TO_CHAR('20200908','YYYY-MM-DD'))                        AS RECENT_LAST_VISIT_DIFF
                                                    , CASE WHEN MAX(C.BSN_DT) IS NULL
                                                           THEN (CASE WHEN A.PURCHS_CYCLE_ELAPSE IS NULL THEN 183 ELSE A.PURCHS_CYCLE_ELAPSE END)
                                                           ELSE DAYS_BETWEEN(MAX(C.BSN_DT),TO_CHAR('20200908','YYYY-MM-DD'))
                                                           END                                                                     AS PURCHS_CYCLE_ELAPSE
                                                    , ADD_DAYS(TO_CHAR('20200901','YYYY-MM-DD'),-PURCHS_CYCLE_ELAPSE)             AS DNA_LAST_VISIT
                                                    , MAX(C.BSN_DT)                                                                AS RECENT_LAST_VISIT
                                                    , CASE WHEN MAX(C.BSN_DT) IS NULL
                                                           THEN ADD_DAYS(TO_CHAR('20200908','YYYY-MM-DD'),-PURCHS_CYCLE_ELAPSE)
                                                           ELSE MAX(C.BSN_DT)
                                                           END              			                                          AS LAST_VISIT
                                                 -- A : 고객X상품 DNA 
                                            FROM CDS_AMT.TB_AMT_CUST_PRDT_DNA_DATA A 
                                                 -- B :(DW)소분류코드 마스터
                                            LEFT JOIN(SELECT PRDT_DCODE_CD,PRDT_DI_CD
                                                      FROM CDS_DW.TB_DW_PRDT_DCODE_CD
                                                      WHERE AFLCO_CD = '001' AND BIZTP_CD = '10') B ON A.PRDT_DCODE_CD  = B.PRDT_DCODE_CD 
                                                 -- C : 01일(DNA산정일) ~ 최근 영수증 데이터(7/14/21/28일씩 MOVING) 
                                            LEFT JOIN (
                                                     SELECT A.CUST_ID,A.BSN_DT,A.PRDT_CD,B.PRDT_DCODE_CD,C.PRDT_DI_CD,A.SALE_AMT
                                                     FROM  CDS_DW.TB_DW_RCIPT_DETAIL A
                                                     JOIN( SELECT PRDT_CD ,PRDT_DCODE_CD
                                                           FROM CDS_DW.TB_DW_PRDT_MASTR 
                                                           WHERE AFLCO_CD = '001' AND BIZTP_CD = '10') B ON A.PRDT_CD  = B.PRDT_CD
                                                     JOIN( SELECT PRDT_DCODE_CD,PRDT_DI_CD
                                                           FROM CDS_DW.TB_DW_PRDT_DCODE_CD
                                                           WHERE AFLCO_CD = '001' AND BIZTP_CD = '10') C ON B.PRDT_DCODE_CD  = C.PRDT_DCODE_CD 
                                                     WHERE A.BSN_DT BETWEEN TO_CHAR('20200901','YYYY-MM-DD') AND TO_CHAR('20200908','YYYY-MM-DD')
                                                       AND A.AFLCO_CD ='001' 
                                                       AND A.BIZTP_CD ='10'        
                                                       AND A.SALE_AMT > 0
                                                       AND A.CUST_ID IS NOT NULL
                                                   ) C ON A.CUST_ID = C.CUST_ID AND A.PRDT_DCODE_CD = C.PRDT_DCODE_CD
                                            WHERE    A.AFLCO_CD = '001' AND   A.BIZTP_CD  = '10'
                                              AND    A.YM_WCNT  = '20200901'
                                            GROUP BY  A.CUST_ID
                                                    , A.PRDT_DCODE_CD        
                                                    , B.PRDT_DI_CD
                                                    , A.PURCHS_CYCLE
                                                    , A.PURCHS_CYCLE_ELAPSE
                                        )

                            )
                            -- 미래 C1~C3기간 내 행사일수 (DT_MASTR테이블과 업태이벤트 마스터 )
                ,EVENT_CALENDAR AS (    
                                        SELECT  SUM(CASE WHEN WEEK_CAT IN ('W1') 	  THEN EVENT_DAYS_CNT END) AS C1_EVENT_DAYS_CNT
                                               ,SUM(CASE WHEN WEEK_CAT IN ('W2','W3') THEN EVENT_DAYS_CNT END) AS C2_EVENT_DAYS_CNT
                                               ,SUM(CASE WHEN WEEK_CAT NOT IN ('W1','W2','W3') THEN EVENT_DAYS_CNT END) AS C3_EVENT_DAYS_CNT
                                               ,SUM(CASE WHEN WEEK_CAT IN ('W1')	  THEN HOLI_DAYS_CNT  END) AS C1_HOLI_DAYS_CNT
                                               ,SUM(CASE WHEN WEEK_CAT IN ('W2','W3') THEN HOLI_DAYS_CNT  END) AS C2_HOLI_DAYS_CNT
                                               ,SUM(CASE WHEN WEEK_CAT NOT IN ('W1','W2','W3') THEN HOLI_DAYS_CNT  END) AS C3_HOLI_DAYS_CNT
                                               ,SUM(CASE WHEN WEEK_CAT IN ('W1')      THEN TREDI_DAYS_CNT END) AS C1_TREDI_DAYS_CNT
                                               ,SUM(CASE WHEN WEEK_CAT IN ('W2','W3') THEN TREDI_DAYS_CNT END) AS C2_TREDI_DAYS_CNT
                                               ,SUM(CASE WHEN WEEK_CAT NOT IN ('W1','W2','W3') THEN TREDI_DAYS_CNT END) AS C3_TREDI_DAYS_CNT
                                        FROM (
                                                   SELECT 1 AS IDX,WEEK_CAT,SUM(EVENT_YN) AS  EVENT_DAYS_CNT,SUM(HOLI_YN) AS HOLI_DAYS_CNT, SUM(TREDI_YN) AS  TREDI_DAYS_CNT
                                                   FROM (   
                                                         SELECT A.YR,A.YM,A.YWCNT,A.YMD,B.EVENT_KIND_CD,A.HOLI_KIND_CD
                                                               ,CASE WHEN A.YMD BETWEEN TO_CHAR('20200908','YYYYMMDD') AND ADD_DAYS(TO_CHAR('20200908','YYYYMMDD'),6)               THEN 'W1'
                                                                     WHEN A.YMD BETWEEN ADD_DAYS(TO_CHAR('20200908','YYYYMMDD'),7)  AND ADD_DAYS(TO_CHAR('20200908','YYYYMMDD'),13)  THEN 'W2'
                                                                     WHEN A.YMD BETWEEN ADD_DAYS(TO_CHAR('20200908','YYYYMMDD'),14) AND ADD_DAYS(TO_CHAR('20200908','YYYYMMDD'),20) THEN 'W3'
                                                                     WHEN A.YMD BETWEEN ADD_DAYS(TO_CHAR('20200908','YYYYMMDD'),21) AND ADD_DAYS(TO_CHAR('20200908','YYYYMMDD'),27) THEN 'W4'
                                                                     WHEN A.YMD BETWEEN ADD_DAYS(TO_CHAR('20200908','YYYYMMDD'),28) AND ADD_DAYS(TO_CHAR('20200908','YYYYMMDD'),34) THEN 'W5'
                                                                     ELSE NULL END AS WEEK_CAT
                                        --				       ,CONCAT('W',DENSE_RANK() OVER(ORDER BY A.YWCNT ASC)) AS WEEK_CAT
                                                               ,CASE WHEN EVENT_KIND_CD IS NOT NULL THEN 1 ELSE 0 END AS EVENT_YN
                                                               ,CASE WHEN HOLI_KIND_CD  NOT IN('02') AND HOLI_KIND_CD IS NOT NULL THEN 1 ELSE 0 END AS HOLI_YN
                                                               ,CASE WHEN EVENT_KIND_CD = '02'      THEN 1 ELSE 0 END AS TREDI_YN
                                                         FROM CDS_DW.TB_DW_DT_MASTR A 
                                                         LEFT JOIN CDS_DW.TB_DW_AFLCO_EVENT B ON A.YMD = B.YMD AND B.AFLCO_CD ='001' AND B.BIZTP_CD ='10'
                                                         WHERE TO_CHAR(A.YMD,'YYYYMMDD') BETWEEN '20200908' AND ADD_DAYS(TO_CHAR('20200908','YYYYMMDD'),34)
                                                         ORDER BY 4
                                                         )
                                                    WHERE WEEK_CAT IN ('W1','W2','W3','W4','W5')
                                                    GROUP BY WEEK_CAT
                                                    ORDER BY 1
                                             )  
                                        GROUP BY IDX
                                 )
        /* -- ★ MAIN실행 쿼리 시작 -- */
        SELECT    T1.CUST_ID                     
                 ,T1.BAIN_GRADE_YM               
                 ,T1.DNA_YMD                     
                 ,T1.PRED_YMD                    
                 ,T1.DAVG_PURCHS_AMT_EXC_ELEC    
                 ,T1.TOP1_STR_DSTNC              
                 ,T1.TOP2_STR_DSTNC              
                 ,T1.CLDR_EVENT_PRE_UNITY        
                 ,T1.CLDR_EVENT_PRE_TREDI        
                 ,T1.PURCHS_WEND_HLDY            
                 ,T1.PURCHS_CYCLE                
                 ,T1.PURCHS_CYCLE_CHG            
                 ,T1.PURCHS_CYCLE_REGUL_YN       
                 ,T1.PURCHS_CYCLE_ARRVL_RT       
                 ,T1.PURCHS_VISIT_CHG_RT_AVG_6M  
                 ,T1.RFM_LV                      
                 ,T1.RFM_F_SCORE                 
                 ,T1.RFM_M_SCORE
                 /***************************** 상품분류체계 변경시 수정해야하는 부분 **********************************/
                 ,T1.RFM_LV_DI_FRESH1            
                 ,T1.RFM_LV_DI_FRESH2            
                 ,T1.RFM_LV_DI_PEACOCK           
                 ,T1.RFM_LV_DI_PRCS          	  
                 ,T1.RFM_LV_DI_HNR          	  
                 ,T1.RFM_LV_DI_LIVING            
                 ,T1.RFM_LV_DI_MOLLYS            
                 ,T1.RFM_LV_DI_ELEC_CULTR        
                 ,T1.RFM_LV_DI_FSHN
                 /******************************************************************************************************/
                 ,T1.TOT_FREQUENCY_CNT           
                 ,ROUND((PW1_SUM_SALES_AMT+PW2_SUM_SALES_AMT+PW3_SUM_SALES_AMT+PW4_SUM_SALES_AMT+PW5_SUM_SALES_AMT)/NULLIF((PW1_FREQUENCY_CNT+PW2_FREQUENCY_CNT+PW3_FREQUENCY_CNT+PW4_FREQUENCY_CNT+PW5_FREQUENCY_CNT),0),0)  AS  RECENT_1M_SALES_INDEX
                 ,CASE WHEN COALESCE(T1.CNT_BF3M,0) > 0 AND COALESCE(T1.CNT_BF6M,0)=0 THEN 1 ELSE COALESCE(COALESCE(T1.CNT_BF3M,0)/NULLIF(T1.CNT_BF6M,0) - 1,0) END  AS  RECENT_3M_FREQUENCY_DIFF_RT
                 ,CASE WHEN C.CUST_ID IS NOT NULL THEN 1 ELSE 0 END                    AS EMT_MALL_VISIT_YN
                 ,CASE WHEN D.EMT_MALL_LAST_PUCHS_DATE IS NOT NULL AND DAYS_BETWEEN(D.EMT_MALL_LAST_PUCHS_DATE,T1.DNA_YMD) = 0
                       THEN 1
                       WHEN D.EMT_MALL_LAST_PUCHS_DATE IS NOT NULL AND DAYS_BETWEEN(D.EMT_MALL_LAST_PUCHS_DATE,T1.DNA_YMD) > 0
                       THEN DAYS_BETWEEN(D.EMT_MALL_LAST_PUCHS_DATE,T1.DNA_YMD)
                       ELSE NULL
                       END 							 									      AS EMT_MALL_LAST_PUCHS_DIFF
                 ,E.USEFL_POINT 
                 ,F.PRDT_ARRVL_RT_DIFF_ABS 													  AS MAIN_PRDT_ARRVL_RT_DIFF_ABS
                 ,G.PRDT_ARRVL_RT_DIFF_ABS                                                    AS PREFER_PRDT_ARRVL_RT_DIFF_ABS
                 ,H.MIN_PRDT_PURCHS_CYCLE
                 ,H.MAX_PRDT_PURCHS_CYCLE
                 /***************************** 상품분류체계 변경시 수정해야하는 부분 **********************************/
                 ,H.PRDT_DCODE10_CNT
                 ,H.PRDT_DCODE11_CNT
                 ,H.PRDT_DCODE20_CNT
                 ,H.PRDT_DCODE30_CNT
                 ,H.PRDT_DCODE40_CNT
                 ,H.PRDT_DCODE41_CNT
                 ,H.PRDT_DCODE42_CNT
                 ,H.PRDT_DCODE50_CNT
                 ,H.PRDT_DCODE60_CNT
                 /******************************************************************************************************/
                 ,DAYS_BETWEEN(I.ONLINE_AFLCO_LAST_VISIT,  TO_CHAR('20200908','YYYY-MM-DD'))     AS LAST_ONLINE_DIFF_DAYS
                 ,DAYS_BETWEEN(I.OFFLINE_AFLCO_LAST_VISIT, TO_CHAR('20200908','YYYY-MM-DD'))	 AS LAST_OFFLINE_DIFF_DAYS
                 ,DAYS_BETWEEN(I.OFFLINE_BIZTP_LAST_VISIT, TO_CHAR('20200908','YYYY-MM-DD'))	 AS LAST_OFFLINE_BIZTP_DIFF_DAYS
                 ,J.*
        FROM (
                SELECT    BASE.CUST_ID
                         -- 예측일(YMD) 에서 YM만 받아옴
                        , MAX(BASE.BAIN_GRADE_YM)              AS BAIN_GRADE_YM
                        , '20200901'                          AS DNA_YMD
                        , '20200908'                         AS PRED_YMD
                        , MAX(BASE.DAVG_PURCHS_AMT_EXC_ELEC)   AS DAVG_PURCHS_AMT_EXC_ELEC 
                        , MAX(BASE.MAIN_PURCHS_DCODE)          AS MAIN_PURCHS_DCODE
                        , MAX(BASE.PREFER_PURCHS_DCODE)        AS PREFER_PURCHS_DCODE
                        , MAX(BASE.TOP1_STR_DSTNC)             AS TOP1_STR_DSTNC
                        , MAX(BASE.TOP2_STR_DSTNC)             AS TOP2_STR_DSTNC
                        , MAX(BASE.CLDR_EVENT_PRE_UNITY)       AS CLDR_EVENT_PRE_UNITY 
                        , MAX(BASE.CLDR_EVENT_PRE_TREDI)       AS CLDR_EVENT_PRE_TREDI 
                        , MAX(BASE.PURCHS_WEND_HLDY)           AS PURCHS_WEND_HLDY
                        , MAX(BASE.PURCHS_CYCLE)               AS PURCHS_CYCLE  
                        , MAX(BASE.PURCHS_CYCLE_CHG)           AS PURCHS_CYCLE_CHG  
                        , MAX(BASE.PURCHS_CYCLE_REGUL_YN)      AS PURCHS_CYCLE_REGUL_YN 
                        -- , MAX(BASE.PURCHS_CYCLE_ARRVL_RT)      AS PURCHS_CYCLE_ARRVL_RT 
                        , (DAYS_BETWEEN(MAX(A.BSN_DT),TO_CHAR('20200908','YYYY-MM-DD')))/MAX(BASE.PURCHS_CYCLE) AS PURCHS_CYCLE_ARRVL_RT
                        , MAX(BASE.PURCHS_VISIT_CHG_RT_AVG_6M) AS PURCHS_VISIT_CHG_RT_AVG_6M
                        /***************************** 상품분류체계 변경시 수정해야하는 부분 **********************************/
                        , MAX(BASE.RFM_LV)                     AS RFM_LV
                        , MAX(BASE.RFM_F_SCORE)                AS RFM_F_SCORE 
                        , MAX(BASE.RFM_M_SCORE)                AS RFM_M_SCORE
                        , MAX(BASE.RFM_LV_DI_FRESH1)           AS RFM_LV_DI_FRESH1
                        , MAX(BASE.RFM_LV_DI_FRESH2)           AS RFM_LV_DI_FRESH2
                        , MAX(BASE.RFM_LV_DI_PEACOCK)          AS RFM_LV_DI_PEACOCK
                        , MAX(BASE.RFM_LV_DI_PRCS)             AS RFM_LV_DI_PRCS
                        , MAX(BASE.RFM_LV_DI_HNR)              AS RFM_LV_DI_HNR
                        , MAX(BASE.RFM_LV_DI_LIVING)           AS RFM_LV_DI_LIVING
                        , MAX(BASE.RFM_LV_DI_MOLLYS)           AS RFM_LV_DI_MOLLYS
                        , MAX(BASE.RFM_LV_DI_ELEC_CULTR)       AS RFM_LV_DI_ELEC_CULTR
                        , MAX(BASE.RFM_LV_DI_FSHN)             AS RFM_LV_DI_FSHN
                        /******************************************************************************************************/
                        -- 6개월 동안 전체 구매횟수
                        ,COUNT(DISTINCT CASE WHEN TO_CHAR(BSN_DT,'YYYYMMDD') BETWEEN TO_CHAR(ADD_DAYS('20200908',-184),'YYYYMMDD') AND TO_CHAR(ADD_DAYS('20200908',-1),'YYYYMMDD') THEN A.BSN_DT ELSE NULL END)  AS TOT_FREQUENCY_CNT
                        -- 최근 3개월 동안 전체상품 구매일수
                        ,COUNT(DISTINCT CASE WHEN TO_CHAR(BSN_DT,'YYYYMMDD') BETWEEN TO_CHAR(ADD_DAYS('20200908',-92),'YYYYMMDD')  AND TO_CHAR(ADD_DAYS('20200908',-1),'YYYYMMDD') THEN A.BSN_DT ELSE NULL END)  AS CNT_BF3M
                        -- 과거 3개월 동안 전체상품 구매일수  
                        ,COUNT(DISTINCT CASE WHEN TO_CHAR(BSN_DT,'YYYYMMDD') BETWEEN TO_CHAR(ADD_DAYS('20200908',-184),'YYYYMMDD') AND TO_CHAR(ADD_DAYS('20200908',-93),'YYYYMMDD')THEN A.BSN_DT ELSE NULL END)  AS CNT_BF6M
                        -- (6개월간 마지막 구매일) 
                        ,MAX(A.BSN_DT)                                               AS MAX_DT
                        -- (6개월간 최초 구매일) 
                        ,MIN(A.BSN_DT)                                               AS MIN_DT
                        -- (주차별 구매금액)
                        ,SUM(CASE WHEN B.WEEK_CAT = 'W1' THEN A.SALE_AMT ELSE 0 END) AS PW1_SUM_SALES_AMT
                        ,SUM(CASE WHEN B.WEEK_CAT = 'W2' THEN A.SALE_AMT ELSE 0 END) AS PW2_SUM_SALES_AMT
                        ,SUM(CASE WHEN B.WEEK_CAT = 'W3' THEN A.SALE_AMT ELSE 0 END) AS PW3_SUM_SALES_AMT
                        ,SUM(CASE WHEN B.WEEK_CAT = 'W4' THEN A.SALE_AMT ELSE 0 END) AS PW4_SUM_SALES_AMT
                        ,SUM(CASE WHEN B.WEEK_CAT = 'W5' THEN A.SALE_AMT ELSE 0 END) AS PW5_SUM_SALES_AMT
                        -- (주차별 방문횟수)
                        ,COUNT(DISTINCT CASE WHEN B.WEEK_CAT = 'W1' THEN  A.BSN_DT ELSE NULL END) AS PW1_FREQUENCY_CNT
                        ,COUNT(DISTINCT CASE WHEN B.WEEK_CAT = 'W2' THEN  A.BSN_DT ELSE NULL END) AS PW2_FREQUENCY_CNT
                        ,COUNT(DISTINCT CASE WHEN B.WEEK_CAT = 'W3' THEN  A.BSN_DT ELSE NULL END) AS PW3_FREQUENCY_CNT
                        ,COUNT(DISTINCT CASE WHEN B.WEEK_CAT = 'W4' THEN  A.BSN_DT ELSE NULL END) AS PW4_FREQUENCY_CNT
                        ,COUNT(DISTINCT CASE WHEN B.WEEK_CAT = 'W5' THEN  A.BSN_DT ELSE NULL END) AS PW5_FREQUENCY_CNT
                        -- * A : 영수증 상세
                FROM  CDS_DW.TB_DW_RCIPT_DETAIL A
                        -- * BASE : 베인등급 & DNA 구매주기 有 고객 정보 로드 ( CUST_ID를 KEY로 잡기위한 테이블 ) 
                JOIN  BASE_DNA_CUST_TB BASE  ON A.CUST_ID = BASE.CUST_ID
                        -- * B : 일자 마스터 ( 주차 매핑 ) 
                LEFT JOIN (
                            SELECT DISTINCT YWCNT,YMD,YM
                                           , CASE  WHEN YMD BETWEEN TO_CHAR(ADD_DAYS('20200908',-7 ),'YYYY-MM-DD') AND TO_CHAR(ADD_DAYS('20200908',-1 ),'YYYY-MM-DD') THEN 'W1'
                                                   WHEN YMD BETWEEN TO_CHAR(ADD_DAYS('20200908',-14),'YYYY-MM-DD') AND TO_CHAR(ADD_DAYS('20200908',-8 ),'YYYY-MM-DD') THEN 'W2'
                                                   WHEN YMD BETWEEN TO_CHAR(ADD_DAYS('20200908',-21),'YYYY-MM-DD') AND TO_CHAR(ADD_DAYS('20200908',-15),'YYYY-MM-DD') THEN 'W3'
                                                   WHEN YMD BETWEEN TO_CHAR(ADD_DAYS('20200908',-28),'YYYY-MM-DD') AND TO_CHAR(ADD_DAYS('20200908',-22),'YYYY-MM-DD') THEN 'W4'
                                             ELSE  'W5'
                                             END AS WEEK_CAT
                            FROM CDS_DW.TB_DW_DT_MASTR
                            WHERE YMD  BETWEEN TO_CHAR(ADD_DAYS('20200908',-35),'YYYY-MM-DD') AND TO_CHAR(ADD_DAYS('20200908',-1),'YYYY-MM-DD') 
                            ORDER BY 4
                          ) B ON B.YMD = A.BSN_DT
                WHERE  TO_CHAR(A.BSN_DT,'YYYYMMDD') BETWEEN TO_CHAR(ADD_DAYS('20200908',-184),'YYYYMMDD') AND TO_CHAR(ADD_DAYS('20200908', -1),'YYYYMMDD')
                  AND  A.AFLCO_CD        IN ('001')		  
                  AND  A.BIZTP_CD        IN ('10' )
                  AND  A.SALE_AMT > 0
                GROUP BY BASE.CUST_ID	
        ) T1
                -- * C : 이마트몰 최근 6개월 간 로그인 이력
        LEFT JOIN (
                SELECT DISTINCT CUST_NO AS CUST_ID
                FROM CDS_DW.TB_DW_EMT_MALL_VISIT
                WHERE TO_CHAR(LOGIN_DT,'YYYYMMDD') BETWEEN TO_CHAR(ADD_DAYS('20200908',-184),'YYYYMMDD') AND TO_CHAR(ADD_DAYS('20200908', -1),'YYYYMMDD')
                ) C ON T1.CUST_ID = C.CUST_ID
                --* D : 이마트몰 마지막 구매 경과일
        LEFT JOIN (
                SELECT  CUST_NO AS CUST_ID
                       ,MAX(TO_CHAR(SALE_DT)) AS EMT_MALL_LAST_PUCHS_DATE
                FROM CDS_DW.TB_DW_EMT_MALL_SALE_INFO
                WHERE TO_CHAR(SALE_DT,'YYYYMMDD') BETWEEN TO_CHAR(ADD_DAYS('20200908',-184),'YYYYMMDD') AND TO_CHAR(ADD_DAYS('20200908', -1),'YYYYMMDD')
                GROUP BY CUST_NO 
                ) D ON T1.CUST_ID = D.CUST_ID
                --* E : 가용 포인트 (익월 만료 포인트 X) 
        LEFT JOIN (
                SELECT CUST_ID ,USEFL_POINT --, NXH_EXTSH_PARNG_POINT 
                FROM CDS_DW.TB_DW_POINT_CUST_RMAIN_POINT
                ) E ON T1.CUST_ID = E.CUST_ID          	
                --* F : (WITH절) 주구매 상품 
        LEFT JOIN CUST_PRDT_DNA_TABLE F ON T1.CUST_ID = F.CUST_ID AND  T1.MAIN_PURCHS_DCODE   = F.PRDT_DCODE_CD  
                --* G : (WITH절) 선호 상품  
        LEFT JOIN CUST_PRDT_DNA_TABLE G ON T1.CUST_ID = G.CUST_ID AND  T1.PREFER_PURCHS_DCODE = G.PRDT_DCODE_CD 
                --* H : (WITH절) 구매주기 도래율 MIN/MAX, 담당별 구매주기 도래율 0.5이하 상품수 , 마지막 방문일 , 구매 경과일
        /***************************** 상품분류체계 변경 수정부분 2 **********************************/
        LEFT JOIN (
                  SELECT  CUST_ID
                        , MAX(LAST_VISIT)             AS LAST_VISIT
                        , MIN(PRDT_ARRVL_RT_DIFF_ABS) AS MIN_PRDT_PURCHS_CYCLE 
                        , MAX(PRDT_ARRVL_RT_DIFF_ABS) AS MAX_PRDT_PURCHS_CYCLE
                        , COUNT(CASE WHEN PRDT_DI_CD = '10' AND PRDT_ARRVL_RT_DIFF_ABS <= 0.5 THEN PRDT_DCODE_CD END) AS PRDT_DCODE10_CNT
                        , COUNT(CASE WHEN PRDT_DI_CD = '11' AND PRDT_ARRVL_RT_DIFF_ABS <= 0.5 THEN PRDT_DCODE_CD END) AS PRDT_DCODE11_CNT
                        , COUNT(CASE WHEN PRDT_DI_CD = '20' AND PRDT_ARRVL_RT_DIFF_ABS <= 0.5 THEN PRDT_DCODE_CD END) AS PRDT_DCODE20_CNT
                        , COUNT(CASE WHEN PRDT_DI_CD = '30' AND PRDT_ARRVL_RT_DIFF_ABS <= 0.5 THEN PRDT_DCODE_CD END) AS PRDT_DCODE30_CNT
                        , COUNT(CASE WHEN PRDT_DI_CD = '40' AND PRDT_ARRVL_RT_DIFF_ABS <= 0.5 THEN PRDT_DCODE_CD END) AS PRDT_DCODE40_CNT
                        , COUNT(CASE WHEN PRDT_DI_CD = '41' AND PRDT_ARRVL_RT_DIFF_ABS <= 0.5 THEN PRDT_DCODE_CD END) AS PRDT_DCODE41_CNT
                        , COUNT(CASE WHEN PRDT_DI_CD = '42' AND PRDT_ARRVL_RT_DIFF_ABS <= 0.5 THEN PRDT_DCODE_CD END) AS PRDT_DCODE42_CNT
                        , COUNT(CASE WHEN PRDT_DI_CD = '50' AND PRDT_ARRVL_RT_DIFF_ABS <= 0.5 THEN PRDT_DCODE_CD END) AS PRDT_DCODE50_CNT
                        , COUNT(CASE WHEN PRDT_DI_CD = '60' AND PRDT_ARRVL_RT_DIFF_ABS <= 0.5 THEN PRDT_DCODE_CD END) AS PRDT_DCODE60_CNT
                  FROM CUST_PRDT_DNA_TABLE 
                  GROUP BY CUST_ID 
                  ) H ON T1.CUST_ID = H.CUST_ID
                -- * I : 온/오프라인 관계사 	
        LEFT JOIN (    
                      /*001	20	트레이더스
                        001	40	노브랜드
                        002	70	에브리데이
                        003	00	e24
                        007	00	이커머스*/
                    SELECT CUST_ID
                           ,MAX(CASE WHEN AFLCO_CAT = 'ON' THEN BSN_DT ELSE NULL END) AS ONLINE_AFLCO_LAST_VISIT
                           ,MAX(CASE WHEN AFLCO_CAT = 'OFF' THEN BSN_DT ELSE NULL END) AS OFFLINE_AFLCO_LAST_VISIT
                           ,MAX(CASE WHEN AFLCO_CAT = 'OFF_BIZTP' THEN BSN_DT ELSE NULL END) AS OFFLINE_BIZTP_LAST_VISIT
                    FROM (
                          SELECT B.CUST_ID
                                 ,(CASE WHEN A.AFLCO_CD IN('007') THEN 'ON' 
                                        WHEN A.AFLCO_CD IN('002','003')THEN 'OFF'
                                        ELSE 'OFF_BIZTP'END) AS AFLCO_CAT
                                 ,MAX(BSN_DT) AS BSN_DT
                          FROM CDS_DW.TB_DW_RCIPT_HDER A
                          JOIN BASE_DNA_CUST_TB B ON A.CUST_ID = B.CUST_ID
                          WHERE A.AFLCO_CD IN('001','002','003','007') AND A.BIZTP_CD IN('20','40','70','00')
                          AND TO_CHAR(A.BSN_DT,'YYYYMMDD') BETWEEN TO_CHAR(ADD_DAYS('20200908',-184),'YYYYMMDD') AND TO_CHAR(ADD_DAYS('20200908', -1),'YYYYMMDD')
                          GROUP BY B.CUST_ID,(CASE WHEN A.AFLCO_CD IN('007') THEN 'ON' WHEN A.AFLCO_CD IN('002','003')THEN 'OFF' ELSE 'OFF_BIZTP'END)
                            )
                    GROUP BY CUST_ID
                      ) I ON T1.CUST_ID = I.CUST_ID
              -- * J : 미래 행사 일수 
        ,EVENT_CALENDAR J
 
INSERT INTO CDS_AMT.TB_AMT_CAMPAIGN_ANL_LOG VALUES(?,?,?,?,?,?,?,?,?,?)
CDS_AMT.TB_AMT_CAMPAIGN_ANL_LOG 테이블 데이터 입력완료
INSERT INTO CDS_AMT.TB_AMT_TIMING_APPLY
        WITH BASE_DNA_CUST_TB AS (
                            SELECT DISTINCT  A.CUST_ID
                                           , '202008' AS BAIN_GRADE_YM
                                           , MAIN_PURCHS_DCODE 
                                           , PREFER_PURCHS_DCODE
                                           , COALESCE(TOP1_STR_DSTNC,620)      AS  TOP1_STR_DSTNC
                                           , COALESCE(TOP2_STR_DSTNC,620)      AS  TOP2_STR_DSTNC
                                           , COALESCE(CLDR_EVENT_PRE_UNITY,0)  AS  CLDR_EVENT_PRE_UNITY
                                           , COALESCE(CLDR_EVENT_PRE_TREDI,0)  AS  CLDR_EVENT_PRE_TREDI 
                                           , COALESCE(PURCHS_WEND_HLDY,0)      AS  PURCHS_WEND_HLDY
                                           , PURCHS_CYCLE 
                                           , PURCHS_CYCLE_CHG 
                                           , PURCHS_CYCLE_REGUL_YN 
                                           , PURCHS_CYCLE_ARRVL_RT 
                                           , PURCHS_VISIT_CHG_RT_AVG_6M 
                                           , COALESCE(RFM_LV,0)                AS RFM_LV
                                           , COALESCE(RFM_F_SCORE,0)           AS RFM_F_SCORE
                                           , COALESCE(RFM_M_SCORE,0)           AS RFM_M_SCORE
                                           /***************************** 상품분류체계 변경시 수정해야하는 부분 **********************************/
                                           , COALESCE(RFM_LV_DI_FRESH1,0)      AS RFM_LV_DI_FRESH1
                                           , COALESCE(RFM_LV_DI_FRESH2,0)      AS RFM_LV_DI_FRESH2
                                           , COALESCE(RFM_LV_DI_PEACOCK,0)     AS RFM_LV_DI_PEACOCK
                                           , COALESCE(RFM_LV_DI_PRCS,0)        AS RFM_LV_DI_PRCS
                                           , COALESCE(RFM_LV_DI_HNR,0)         AS RFM_LV_DI_HNR
                                           , COALESCE(RFM_LV_DI_LIVING,0)      AS RFM_LV_DI_LIVING
                                           , COALESCE(RFM_LV_DI_MOLLYS,0)      AS RFM_LV_DI_MOLLYS
                                           , COALESCE(RFM_LV_DI_ELEC_CULTR,0)  AS RFM_LV_DI_ELEC_CULTR
                                           , COALESCE(RFM_LV_DI_FSHN,0)        AS RFM_LV_DI_FSHN
                                           /*****************************************************************************************************/
                                           , DAVG_PURCHS_AMT_EXC_ELEC 
                            FROM CDS_AMT.TB_AMT_BIZTP_CUST_DNA_DATA A 
                            JOIN CDS_DW.TB_BAIN_MEMBR_GRADE B ON A.CUST_ID  = B.CUST_ID AND B.GRADE_YM ='202008' 
                            WHERE YM_WCNT ='20200901'
                              AND AFLCO_CD ='001'
                              AND BIZTP_CD ='10'
                              AND PURCHS_CYCLE IS NOT NULL 
                             ),
            CUST_PRDT_DNA_TABLE AS (
                                    SELECT      CUST_ID
                                              , PRDT_DCODE_CD
                                              , PRDT_DI_CD
                                              , PURCHS_CYCLE
                                              , CASE WHEN PURCHS_CYCLE_ELAPSE = 0 THEN 1 ELSE PURCHS_CYCLE_ELAPSE END   AS PURCHS_CYCLE_ELAPSE
                                              , DNA_LAST_VISIT_DIFF
                                              , RECENT_LAST_VISIT_DIFF
                                              , DNA_LAST_VISIT
                                              , RECENT_LAST_VISIT
                                              , LAST_VISIT
                                              , CASE WHEN PURCHS_CYCLE_ELAPSE = 0
                                                     THEN ROUND((1/PURCHS_CYCLE),2)              
                                                     ELSE ROUND((PURCHS_CYCLE_ELAPSE/PURCHS_CYCLE),2) 
                                                     END 														        AS PRDT_ARRVL_RT 
                                              , CASE WHEN PURCHS_CYCLE_ELAPSE = 0
                                                     THEN ABS(1-(ROUND((1/PURCHS_CYCLE),2))) 
                                                     ELSE ABS(1-(ROUND((PURCHS_CYCLE_ELAPSE/PURCHS_CYCLE),2)))     
                                                     END                                                                AS PRDT_ARRVL_RT_DIFF_ABS
                                    FROM (
                                            SELECT    A.CUST_ID
                                                    , A.PRDT_DCODE_CD
                                                    , B.PRDT_DI_CD
                                                    , A.PURCHS_CYCLE
                                                    , (CASE WHEN A.PURCHS_CYCLE_ELAPSE IS NULL THEN 183 ELSE A.PURCHS_CYCLE_ELAPSE END)     AS DNA_LAST_VISIT_DIFF
                                                    , DAYS_BETWEEN(MAX(C.BSN_DT),TO_CHAR('20200908','YYYY-MM-DD'))                        AS RECENT_LAST_VISIT_DIFF
                                                    , CASE WHEN MAX(C.BSN_DT) IS NULL
                                                           THEN (CASE WHEN A.PURCHS_CYCLE_ELAPSE IS NULL THEN 183 ELSE A.PURCHS_CYCLE_ELAPSE END)
                                                           ELSE DAYS_BETWEEN(MAX(C.BSN_DT),TO_CHAR('20200908','YYYY-MM-DD'))
                                                           END                                                                     AS PURCHS_CYCLE_ELAPSE
                                                    , ADD_DAYS(TO_CHAR('20200901','YYYY-MM-DD'),-PURCHS_CYCLE_ELAPSE)             AS DNA_LAST_VISIT
                                                    , MAX(C.BSN_DT)                                                                AS RECENT_LAST_VISIT
                                                    , CASE WHEN MAX(C.BSN_DT) IS NULL
                                                           THEN ADD_DAYS(TO_CHAR('20200908','YYYY-MM-DD'),-PURCHS_CYCLE_ELAPSE)
                                                           ELSE MAX(C.BSN_DT)
                                                           END              			                                          AS LAST_VISIT
                                                 -- A : 고객X상품 DNA 
                                            FROM CDS_AMT.TB_AMT_CUST_PRDT_DNA_DATA A 
                                                 -- B :(DW)소분류코드 마스터
                                            LEFT JOIN(SELECT PRDT_DCODE_CD,PRDT_DI_CD
                                                      FROM CDS_DW.TB_DW_PRDT_DCODE_CD
                                                      WHERE AFLCO_CD = '001' AND BIZTP_CD = '10') B ON A.PRDT_DCODE_CD  = B.PRDT_DCODE_CD 
                                                 -- C : 01일(DNA산정일) ~ 최근 영수증 데이터(7/14/21/28일씩 MOVING) 
                                            LEFT JOIN (
                                                     SELECT A.CUST_ID,A.BSN_DT,A.PRDT_CD,B.PRDT_DCODE_CD,C.PRDT_DI_CD,A.SALE_AMT
                                                     FROM  CDS_DW.TB_DW_RCIPT_DETAIL A
                                                     JOIN( SELECT PRDT_CD ,PRDT_DCODE_CD
                                                           FROM CDS_DW.TB_DW_PRDT_MASTR 
                                                           WHERE AFLCO_CD = '001' AND BIZTP_CD = '10') B ON A.PRDT_CD  = B.PRDT_CD
                                                     JOIN( SELECT PRDT_DCODE_CD,PRDT_DI_CD
                                                           FROM CDS_DW.TB_DW_PRDT_DCODE_CD
                                                           WHERE AFLCO_CD = '001' AND BIZTP_CD = '10') C ON B.PRDT_DCODE_CD  = C.PRDT_DCODE_CD 
                                                     WHERE A.BSN_DT BETWEEN TO_CHAR('20200901','YYYY-MM-DD') AND TO_CHAR('20200908','YYYY-MM-DD')
                                                       AND A.AFLCO_CD ='001' 
                                                       AND A.BIZTP_CD ='10'        
                                                       AND A.SALE_AMT > 0
                                                       AND A.CUST_ID IS NOT NULL
                                                   ) C ON A.CUST_ID = C.CUST_ID AND A.PRDT_DCODE_CD = C.PRDT_DCODE_CD
                                            WHERE    A.AFLCO_CD = '001' AND   A.BIZTP_CD  = '10'
                                              AND    A.YM_WCNT  = '20200901'
                                            GROUP BY  A.CUST_ID
                                                    , A.PRDT_DCODE_CD        
                                                    , B.PRDT_DI_CD
                                                    , A.PURCHS_CYCLE
                                                    , A.PURCHS_CYCLE_ELAPSE
                                        )

                            )
                            -- 미래 C1~C3기간 내 행사일수 (DT_MASTR테이블과 업태이벤트 마스터 )
                ,EVENT_CALENDAR AS (    
                                        SELECT  SUM(CASE WHEN WEEK_CAT IN ('W1') 	  THEN EVENT_DAYS_CNT END) AS C1_EVENT_DAYS_CNT
                                               ,SUM(CASE WHEN WEEK_CAT IN ('W2','W3') THEN EVENT_DAYS_CNT END) AS C2_EVENT_DAYS_CNT
                                               ,SUM(CASE WHEN WEEK_CAT NOT IN ('W1','W2','W3') THEN EVENT_DAYS_CNT END) AS C3_EVENT_DAYS_CNT
                                               ,SUM(CASE WHEN WEEK_CAT IN ('W1')	  THEN HOLI_DAYS_CNT  END) AS C1_HOLI_DAYS_CNT
                                               ,SUM(CASE WHEN WEEK_CAT IN ('W2','W3') THEN HOLI_DAYS_CNT  END) AS C2_HOLI_DAYS_CNT
                                               ,SUM(CASE WHEN WEEK_CAT NOT IN ('W1','W2','W3') THEN HOLI_DAYS_CNT  END) AS C3_HOLI_DAYS_CNT
                                               ,SUM(CASE WHEN WEEK_CAT IN ('W1')      THEN TREDI_DAYS_CNT END) AS C1_TREDI_DAYS_CNT
                                               ,SUM(CASE WHEN WEEK_CAT IN ('W2','W3') THEN TREDI_DAYS_CNT END) AS C2_TREDI_DAYS_CNT
                                               ,SUM(CASE WHEN WEEK_CAT NOT IN ('W1','W2','W3') THEN TREDI_DAYS_CNT END) AS C3_TREDI_DAYS_CNT
                                        FROM (
                                                   SELECT 1 AS IDX,WEEK_CAT,SUM(EVENT_YN) AS  EVENT_DAYS_CNT,SUM(HOLI_YN) AS HOLI_DAYS_CNT, SUM(TREDI_YN) AS  TREDI_DAYS_CNT
                                                   FROM (   
                                                         SELECT A.YR,A.YM,A.YWCNT,A.YMD,B.EVENT_KIND_CD,A.HOLI_KIND_CD
                                                               ,CASE WHEN A.YMD BETWEEN TO_CHAR('20200908','YYYYMMDD') AND ADD_DAYS(TO_CHAR('20200908','YYYYMMDD'),6)               THEN 'W1'
                                                                     WHEN A.YMD BETWEEN ADD_DAYS(TO_CHAR('20200908','YYYYMMDD'),7)  AND ADD_DAYS(TO_CHAR('20200908','YYYYMMDD'),13)  THEN 'W2'
                                                                     WHEN A.YMD BETWEEN ADD_DAYS(TO_CHAR('20200908','YYYYMMDD'),14) AND ADD_DAYS(TO_CHAR('20200908','YYYYMMDD'),20) THEN 'W3'
                                                                     WHEN A.YMD BETWEEN ADD_DAYS(TO_CHAR('20200908','YYYYMMDD'),21) AND ADD_DAYS(TO_CHAR('20200908','YYYYMMDD'),27) THEN 'W4'
                                                                     WHEN A.YMD BETWEEN ADD_DAYS(TO_CHAR('20200908','YYYYMMDD'),28) AND ADD_DAYS(TO_CHAR('20200908','YYYYMMDD'),34) THEN 'W5'
                                                                     ELSE NULL END AS WEEK_CAT
                                        --				       ,CONCAT('W',DENSE_RANK() OVER(ORDER BY A.YWCNT ASC)) AS WEEK_CAT
                                                               ,CASE WHEN EVENT_KIND_CD IS NOT NULL THEN 1 ELSE 0 END AS EVENT_YN
                                                               ,CASE WHEN HOLI_KIND_CD  NOT IN('02') AND HOLI_KIND_CD IS NOT NULL THEN 1 ELSE 0 END AS HOLI_YN
                                                               ,CASE WHEN EVENT_KIND_CD = '02'      THEN 1 ELSE 0 END AS TREDI_YN
                                                         FROM CDS_DW.TB_DW_DT_MASTR A 
                                                         LEFT JOIN CDS_DW.TB_DW_AFLCO_EVENT B ON A.YMD = B.YMD AND B.AFLCO_CD ='001' AND B.BIZTP_CD ='10'
                                                         WHERE TO_CHAR(A.YMD,'YYYYMMDD') BETWEEN '20200908' AND ADD_DAYS(TO_CHAR('20200908','YYYYMMDD'),34)
                                                         ORDER BY 4
                                                         )
                                                    WHERE WEEK_CAT IN ('W1','W2','W3','W4','W5')
                                                    GROUP BY WEEK_CAT
                                                    ORDER BY 1
                                             )  
                                        GROUP BY IDX
                                 )
        /* -- ★ MAIN실행 쿼리 시작 -- */
        SELECT    T1.CUST_ID                     
                 ,T1.BAIN_GRADE_YM               
                 ,T1.DNA_YMD                     
                 ,T1.PRED_YMD                    
                 ,T1.DAVG_PURCHS_AMT_EXC_ELEC    
                 ,T1.TOP1_STR_DSTNC              
                 ,T1.TOP2_STR_DSTNC              
                 ,T1.CLDR_EVENT_PRE_UNITY        
                 ,T1.CLDR_EVENT_PRE_TREDI        
                 ,T1.PURCHS_WEND_HLDY            
                 ,T1.PURCHS_CYCLE                
                 ,T1.PURCHS_CYCLE_CHG            
                 ,T1.PURCHS_CYCLE_REGUL_YN       
                 ,T1.PURCHS_CYCLE_ARRVL_RT       
                 ,T1.PURCHS_VISIT_CHG_RT_AVG_6M  
                 ,T1.RFM_LV                      
                 ,T1.RFM_F_SCORE                 
                 ,T1.RFM_M_SCORE
                 /***************************** 상품분류체계 변경시 수정해야하는 부분 **********************************/
                 ,T1.RFM_LV_DI_FRESH1            
                 ,T1.RFM_LV_DI_FRESH2            
                 ,T1.RFM_LV_DI_PEACOCK           
                 ,T1.RFM_LV_DI_PRCS          	  
                 ,T1.RFM_LV_DI_HNR          	  
                 ,T1.RFM_LV_DI_LIVING            
                 ,T1.RFM_LV_DI_MOLLYS            
                 ,T1.RFM_LV_DI_ELEC_CULTR        
                 ,T1.RFM_LV_DI_FSHN
                 /******************************************************************************************************/
                 ,T1.TOT_FREQUENCY_CNT           
                 ,ROUND((PW1_SUM_SALES_AMT+PW2_SUM_SALES_AMT+PW3_SUM_SALES_AMT+PW4_SUM_SALES_AMT+PW5_SUM_SALES_AMT)/NULLIF((PW1_FREQUENCY_CNT+PW2_FREQUENCY_CNT+PW3_FREQUENCY_CNT+PW4_FREQUENCY_CNT+PW5_FREQUENCY_CNT),0),0)  AS  RECENT_1M_SALES_INDEX
                 ,CASE WHEN COALESCE(T1.CNT_BF3M,0) > 0 AND COALESCE(T1.CNT_BF6M,0)=0 THEN 1 ELSE COALESCE(COALESCE(T1.CNT_BF3M,0)/NULLIF(T1.CNT_BF6M,0) - 1,0) END  AS  RECENT_3M_FREQUENCY_DIFF_RT
                 ,CASE WHEN C.CUST_ID IS NOT NULL THEN 1 ELSE 0 END                    AS EMT_MALL_VISIT_YN
                 ,CASE WHEN D.EMT_MALL_LAST_PUCHS_DATE IS NOT NULL AND DAYS_BETWEEN(D.EMT_MALL_LAST_PUCHS_DATE,T1.DNA_YMD) = 0
                       THEN 1
                       WHEN D.EMT_MALL_LAST_PUCHS_DATE IS NOT NULL AND DAYS_BETWEEN(D.EMT_MALL_LAST_PUCHS_DATE,T1.DNA_YMD) > 0
                       THEN DAYS_BETWEEN(D.EMT_MALL_LAST_PUCHS_DATE,T1.DNA_YMD)
                       ELSE NULL
                       END 							 									      AS EMT_MALL_LAST_PUCHS_DIFF
                 ,E.USEFL_POINT 
                 ,F.PRDT_ARRVL_RT_DIFF_ABS 													  AS MAIN_PRDT_ARRVL_RT_DIFF_ABS
                 ,G.PRDT_ARRVL_RT_DIFF_ABS                                                    AS PREFER_PRDT_ARRVL_RT_DIFF_ABS
                 ,H.MIN_PRDT_PURCHS_CYCLE
                 ,H.MAX_PRDT_PURCHS_CYCLE
                 /***************************** 상품분류체계 변경시 수정해야하는 부분 **********************************/
                 ,H.PRDT_DCODE10_CNT
                 ,H.PRDT_DCODE11_CNT
                 ,H.PRDT_DCODE20_CNT
                 ,H.PRDT_DCODE30_CNT
                 ,H.PRDT_DCODE40_CNT
                 ,H.PRDT_DCODE41_CNT
                 ,H.PRDT_DCODE42_CNT
                 ,H.PRDT_DCODE50_CNT
                 ,H.PRDT_DCODE60_CNT
                 /******************************************************************************************************/
                 ,DAYS_BETWEEN(I.ONLINE_AFLCO_LAST_VISIT,  TO_CHAR('20200908','YYYY-MM-DD'))     AS LAST_ONLINE_DIFF_DAYS
                 ,DAYS_BETWEEN(I.OFFLINE_AFLCO_LAST_VISIT, TO_CHAR('20200908','YYYY-MM-DD'))	 AS LAST_OFFLINE_DIFF_DAYS
                 ,DAYS_BETWEEN(I.OFFLINE_BIZTP_LAST_VISIT, TO_CHAR('20200908','YYYY-MM-DD'))	 AS LAST_OFFLINE_BIZTP_DIFF_DAYS
                 ,J.*
        FROM (
                SELECT    BASE.CUST_ID
                         -- 예측일(YMD) 에서 YM만 받아옴
                        , MAX(BASE.BAIN_GRADE_YM)              AS BAIN_GRADE_YM
                        , '20200901'                          AS DNA_YMD
                        , '20200908'                         AS PRED_YMD
                        , MAX(BASE.DAVG_PURCHS_AMT_EXC_ELEC)   AS DAVG_PURCHS_AMT_EXC_ELEC 
                        , MAX(BASE.MAIN_PURCHS_DCODE)          AS MAIN_PURCHS_DCODE
                        , MAX(BASE.PREFER_PURCHS_DCODE)        AS PREFER_PURCHS_DCODE
                        , MAX(BASE.TOP1_STR_DSTNC)             AS TOP1_STR_DSTNC
                        , MAX(BASE.TOP2_STR_DSTNC)             AS TOP2_STR_DSTNC
                        , MAX(BASE.CLDR_EVENT_PRE_UNITY)       AS CLDR_EVENT_PRE_UNITY 
                        , MAX(BASE.CLDR_EVENT_PRE_TREDI)       AS CLDR_EVENT_PRE_TREDI 
                        , MAX(BASE.PURCHS_WEND_HLDY)           AS PURCHS_WEND_HLDY
                        , MAX(BASE.PURCHS_CYCLE)               AS PURCHS_CYCLE  
                        , MAX(BASE.PURCHS_CYCLE_CHG)           AS PURCHS_CYCLE_CHG  
                        , MAX(BASE.PURCHS_CYCLE_REGUL_YN)      AS PURCHS_CYCLE_REGUL_YN 
                        -- , MAX(BASE.PURCHS_CYCLE_ARRVL_RT)      AS PURCHS_CYCLE_ARRVL_RT 
                        , (DAYS_BETWEEN(MAX(A.BSN_DT),TO_CHAR('20200908','YYYY-MM-DD')))/MAX(BASE.PURCHS_CYCLE) AS PURCHS_CYCLE_ARRVL_RT
                        , MAX(BASE.PURCHS_VISIT_CHG_RT_AVG_6M) AS PURCHS_VISIT_CHG_RT_AVG_6M
                        /***************************** 상품분류체계 변경시 수정해야하는 부분 **********************************/
                        , MAX(BASE.RFM_LV)                     AS RFM_LV
                        , MAX(BASE.RFM_F_SCORE)                AS RFM_F_SCORE 
                        , MAX(BASE.RFM_M_SCORE)                AS RFM_M_SCORE
                        , MAX(BASE.RFM_LV_DI_FRESH1)           AS RFM_LV_DI_FRESH1
                        , MAX(BASE.RFM_LV_DI_FRESH2)           AS RFM_LV_DI_FRESH2
                        , MAX(BASE.RFM_LV_DI_PEACOCK)          AS RFM_LV_DI_PEACOCK
                        , MAX(BASE.RFM_LV_DI_PRCS)             AS RFM_LV_DI_PRCS
                        , MAX(BASE.RFM_LV_DI_HNR)              AS RFM_LV_DI_HNR
                        , MAX(BASE.RFM_LV_DI_LIVING)           AS RFM_LV_DI_LIVING
                        , MAX(BASE.RFM_LV_DI_MOLLYS)           AS RFM_LV_DI_MOLLYS
                        , MAX(BASE.RFM_LV_DI_ELEC_CULTR)       AS RFM_LV_DI_ELEC_CULTR
                        , MAX(BASE.RFM_LV_DI_FSHN)             AS RFM_LV_DI_FSHN
                        /******************************************************************************************************/
                        -- 6개월 동안 전체 구매횟수
                        ,COUNT(DISTINCT CASE WHEN TO_CHAR(BSN_DT,'YYYYMMDD') BETWEEN TO_CHAR(ADD_DAYS('20200908',-184),'YYYYMMDD') AND TO_CHAR(ADD_DAYS('20200908',-1),'YYYYMMDD') THEN A.BSN_DT ELSE NULL END)  AS TOT_FREQUENCY_CNT
                        -- 최근 3개월 동안 전체상품 구매일수
                        ,COUNT(DISTINCT CASE WHEN TO_CHAR(BSN_DT,'YYYYMMDD') BETWEEN TO_CHAR(ADD_DAYS('20200908',-92),'YYYYMMDD')  AND TO_CHAR(ADD_DAYS('20200908',-1),'YYYYMMDD') THEN A.BSN_DT ELSE NULL END)  AS CNT_BF3M
                        -- 과거 3개월 동안 전체상품 구매일수  
                        ,COUNT(DISTINCT CASE WHEN TO_CHAR(BSN_DT,'YYYYMMDD') BETWEEN TO_CHAR(ADD_DAYS('20200908',-184),'YYYYMMDD') AND TO_CHAR(ADD_DAYS('20200908',-93),'YYYYMMDD')THEN A.BSN_DT ELSE NULL END)  AS CNT_BF6M
                        -- (6개월간 마지막 구매일) 
                        ,MAX(A.BSN_DT)                                               AS MAX_DT
                        -- (6개월간 최초 구매일) 
                        ,MIN(A.BSN_DT)                                               AS MIN_DT
                        -- (주차별 구매금액)
                        ,SUM(CASE WHEN B.WEEK_CAT = 'W1' THEN A.SALE_AMT ELSE 0 END) AS PW1_SUM_SALES_AMT
                        ,SUM(CASE WHEN B.WEEK_CAT = 'W2' THEN A.SALE_AMT ELSE 0 END) AS PW2_SUM_SALES_AMT
                        ,SUM(CASE WHEN B.WEEK_CAT = 'W3' THEN A.SALE_AMT ELSE 0 END) AS PW3_SUM_SALES_AMT
                        ,SUM(CASE WHEN B.WEEK_CAT = 'W4' THEN A.SALE_AMT ELSE 0 END) AS PW4_SUM_SALES_AMT
                        ,SUM(CASE WHEN B.WEEK_CAT = 'W5' THEN A.SALE_AMT ELSE 0 END) AS PW5_SUM_SALES_AMT
                        -- (주차별 방문횟수)
                        ,COUNT(DISTINCT CASE WHEN B.WEEK_CAT = 'W1' THEN  A.BSN_DT ELSE NULL END) AS PW1_FREQUENCY_CNT
                        ,COUNT(DISTINCT CASE WHEN B.WEEK_CAT = 'W2' THEN  A.BSN_DT ELSE NULL END) AS PW2_FREQUENCY_CNT
                        ,COUNT(DISTINCT CASE WHEN B.WEEK_CAT = 'W3' THEN  A.BSN_DT ELSE NULL END) AS PW3_FREQUENCY_CNT
                        ,COUNT(DISTINCT CASE WHEN B.WEEK_CAT = 'W4' THEN  A.BSN_DT ELSE NULL END) AS PW4_FREQUENCY_CNT
                        ,COUNT(DISTINCT CASE WHEN B.WEEK_CAT = 'W5' THEN  A.BSN_DT ELSE NULL END) AS PW5_FREQUENCY_CNT
                        -- * A : 영수증 상세
                FROM  CDS_DW.TB_DW_RCIPT_DETAIL A
                        -- * BASE : 베인등급 & DNA 구매주기 有 고객 정보 로드 ( CUST_ID를 KEY로 잡기위한 테이블 ) 
                JOIN  BASE_DNA_CUST_TB BASE  ON A.CUST_ID = BASE.CUST_ID
                        -- * B : 일자 마스터 ( 주차 매핑 ) 
                LEFT JOIN (
                            SELECT DISTINCT YWCNT,YMD,YM
                                           , CASE  WHEN YMD BETWEEN TO_CHAR(ADD_DAYS('20200908',-7 ),'YYYY-MM-DD') AND TO_CHAR(ADD_DAYS('20200908',-1 ),'YYYY-MM-DD') THEN 'W1'
                                                   WHEN YMD BETWEEN TO_CHAR(ADD_DAYS('20200908',-14),'YYYY-MM-DD') AND TO_CHAR(ADD_DAYS('20200908',-8 ),'YYYY-MM-DD') THEN 'W2'
                                                   WHEN YMD BETWEEN TO_CHAR(ADD_DAYS('20200908',-21),'YYYY-MM-DD') AND TO_CHAR(ADD_DAYS('20200908',-15),'YYYY-MM-DD') THEN 'W3'
                                                   WHEN YMD BETWEEN TO_CHAR(ADD_DAYS('20200908',-28),'YYYY-MM-DD') AND TO_CHAR(ADD_DAYS('20200908',-22),'YYYY-MM-DD') THEN 'W4'
                                             ELSE  'W5'
                                             END AS WEEK_CAT
                            FROM CDS_DW.TB_DW_DT_MASTR
                            WHERE YMD  BETWEEN TO_CHAR(ADD_DAYS('20200908',-35),'YYYY-MM-DD') AND TO_CHAR(ADD_DAYS('20200908',-1),'YYYY-MM-DD') 
                            ORDER BY 4
                          ) B ON B.YMD = A.BSN_DT
                WHERE  TO_CHAR(A.BSN_DT,'YYYYMMDD') BETWEEN TO_CHAR(ADD_DAYS('20200908',-184),'YYYYMMDD') AND TO_CHAR(ADD_DAYS('20200908', -1),'YYYYMMDD')
                  AND  A.AFLCO_CD        IN ('001')		  
                  AND  A.BIZTP_CD        IN ('10' )
                  AND  A.SALE_AMT > 0
                GROUP BY BASE.CUST_ID	
        ) T1
                -- * C : 이마트몰 최근 6개월 간 로그인 이력
        LEFT JOIN (
                SELECT DISTINCT CUST_NO AS CUST_ID
                FROM CDS_DW.TB_DW_EMT_MALL_VISIT
                WHERE TO_CHAR(LOGIN_DT,'YYYYMMDD') BETWEEN TO_CHAR(ADD_DAYS('20200908',-184),'YYYYMMDD') AND TO_CHAR(ADD_DAYS('20200908', -1),'YYYYMMDD')
                ) C ON T1.CUST_ID = C.CUST_ID
                --* D : 이마트몰 마지막 구매 경과일
        LEFT JOIN (
                SELECT  CUST_NO AS CUST_ID
                       ,MAX(TO_CHAR(SALE_DT)) AS EMT_MALL_LAST_PUCHS_DATE
                FROM CDS_DW.TB_DW_EMT_MALL_SALE_INFO
                WHERE TO_CHAR(SALE_DT,'YYYYMMDD') BETWEEN TO_CHAR(ADD_DAYS('20200908',-184),'YYYYMMDD') AND TO_CHAR(ADD_DAYS('20200908', -1),'YYYYMMDD')
                GROUP BY CUST_NO 
                ) D ON T1.CUST_ID = D.CUST_ID
                --* E : 가용 포인트 (익월 만료 포인트 X) 
        LEFT JOIN (
                SELECT CUST_ID ,USEFL_POINT --, NXH_EXTSH_PARNG_POINT 
                FROM CDS_DW.TB_DW_POINT_CUST_RMAIN_POINT
                ) E ON T1.CUST_ID = E.CUST_ID          	
                --* F : (WITH절) 주구매 상품 
        LEFT JOIN CUST_PRDT_DNA_TABLE F ON T1.CUST_ID = F.CUST_ID AND  T1.MAIN_PURCHS_DCODE   = F.PRDT_DCODE_CD  
                --* G : (WITH절) 선호 상품  
        LEFT JOIN CUST_PRDT_DNA_TABLE G ON T1.CUST_ID = G.CUST_ID AND  T1.PREFER_PURCHS_DCODE = G.PRDT_DCODE_CD 
                --* H : (WITH절) 구매주기 도래율 MIN/MAX, 담당별 구매주기 도래율 0.5이하 상품수 , 마지막 방문일 , 구매 경과일
        /***************************** 상품분류체계 변경 수정부분 2 **********************************/
        LEFT JOIN (
                  SELECT  CUST_ID
                        , MAX(LAST_VISIT)             AS LAST_VISIT
                        , MIN(PRDT_ARRVL_RT_DIFF_ABS) AS MIN_PRDT_PURCHS_CYCLE 
                        , MAX(PRDT_ARRVL_RT_DIFF_ABS) AS MAX_PRDT_PURCHS_CYCLE
                        , COUNT(CASE WHEN PRDT_DI_CD = '10' AND PRDT_ARRVL_RT_DIFF_ABS <= 0.5 THEN PRDT_DCODE_CD END) AS PRDT_DCODE10_CNT
                        , COUNT(CASE WHEN PRDT_DI_CD = '11' AND PRDT_ARRVL_RT_DIFF_ABS <= 0.5 THEN PRDT_DCODE_CD END) AS PRDT_DCODE11_CNT
                        , COUNT(CASE WHEN PRDT_DI_CD = '20' AND PRDT_ARRVL_RT_DIFF_ABS <= 0.5 THEN PRDT_DCODE_CD END) AS PRDT_DCODE20_CNT
                        , COUNT(CASE WHEN PRDT_DI_CD = '30' AND PRDT_ARRVL_RT_DIFF_ABS <= 0.5 THEN PRDT_DCODE_CD END) AS PRDT_DCODE30_CNT
                        , COUNT(CASE WHEN PRDT_DI_CD = '40' AND PRDT_ARRVL_RT_DIFF_ABS <= 0.5 THEN PRDT_DCODE_CD END) AS PRDT_DCODE40_CNT
                        , COUNT(CASE WHEN PRDT_DI_CD = '41' AND PRDT_ARRVL_RT_DIFF_ABS <= 0.5 THEN PRDT_DCODE_CD END) AS PRDT_DCODE41_CNT
                        , COUNT(CASE WHEN PRDT_DI_CD = '42' AND PRDT_ARRVL_RT_DIFF_ABS <= 0.5 THEN PRDT_DCODE_CD END) AS PRDT_DCODE42_CNT
                        , COUNT(CASE WHEN PRDT_DI_CD = '50' AND PRDT_ARRVL_RT_DIFF_ABS <= 0.5 THEN PRDT_DCODE_CD END) AS PRDT_DCODE50_CNT
                        , COUNT(CASE WHEN PRDT_DI_CD = '60' AND PRDT_ARRVL_RT_DIFF_ABS <= 0.5 THEN PRDT_DCODE_CD END) AS PRDT_DCODE60_CNT
                  FROM CUST_PRDT_DNA_TABLE 
                  GROUP BY CUST_ID 
                  ) H ON T1.CUST_ID = H.CUST_ID
                -- * I : 온/오프라인 관계사 	
        LEFT JOIN (    
                      /*001	20	트레이더스
                        001	40	노브랜드
                        002	70	에브리데이
                        003	00	e24
                        007	00	이커머스*/
                    SELECT CUST_ID
                           ,MAX(CASE WHEN AFLCO_CAT = 'ON' THEN BSN_DT ELSE NULL END) AS ONLINE_AFLCO_LAST_VISIT
                           ,MAX(CASE WHEN AFLCO_CAT = 'OFF' THEN BSN_DT ELSE NULL END) AS OFFLINE_AFLCO_LAST_VISIT
                           ,MAX(CASE WHEN AFLCO_CAT = 'OFF_BIZTP' THEN BSN_DT ELSE NULL END) AS OFFLINE_BIZTP_LAST_VISIT
                    FROM (
                          SELECT B.CUST_ID
                                 ,(CASE WHEN A.AFLCO_CD IN('007') THEN 'ON' 
                                        WHEN A.AFLCO_CD IN('002','003')THEN 'OFF'
                                        ELSE 'OFF_BIZTP'END) AS AFLCO_CAT
                                 ,MAX(BSN_DT) AS BSN_DT
                          FROM CDS_DW.TB_DW_RCIPT_HDER A
                          JOIN BASE_DNA_CUST_TB B ON A.CUST_ID = B.CUST_ID
                          WHERE A.AFLCO_CD IN('001','002','003','007') AND A.BIZTP_CD IN('20','40','70','00')
                          AND TO_CHAR(A.BSN_DT,'YYYYMMDD') BETWEEN TO_CHAR(ADD_DAYS('20200908',-184),'YYYYMMDD') AND TO_CHAR(ADD_DAYS('20200908', -1),'YYYYMMDD')
                          GROUP BY B.CUST_ID,(CASE WHEN A.AFLCO_CD IN('007') THEN 'ON' WHEN A.AFLCO_CD IN('002','003')THEN 'OFF' ELSE 'OFF_BIZTP'END)
                            )
                    GROUP BY CUST_ID
                      ) I ON T1.CUST_ID = I.CUST_ID
              -- * J : 미래 행사 일수 
        ,EVENT_CALENDAR J