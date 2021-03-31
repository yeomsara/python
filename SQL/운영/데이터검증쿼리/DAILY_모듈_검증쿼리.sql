WITH DAILY_MODULE_CD_COMB_TEST AS (
SELECT 'DAILY'AS BATCH_CYCLE,ANL_DT,RECMD_FLAG ,'PRDT_RECMD'AS MODULE,'ALL_CUST_ID_CNT'  AS DNA_CD,COUNT(CASE WHEN CUST_ID IS NOT NULL THEN 1 ELSE NULL END)  AS DEC_VALUE FROM CDS_AMT.RECMD_RANK_DAILY   GROUP BY ANL_DT,RECMD_FLAG
UNION ALL SELECT 'DAILY'AS BATCH_CYCLE,ANL_DT,RECMD_FLAG ,'PRDT_RECMD'AS MODULE,'DT_CNT'  AS DNA_CD,SUM(CASE WHEN DT_CNT IS NOT NULL THEN 1 ELSE 0 END)  AS DEC_VALUE FROM CDS_AMT.RECMD_RANK_DAILY   GROUP BY ANL_DT,RECMD_FLAG
UNION ALL SELECT 'DAILY'AS BATCH_CYCLE,ANL_DT,RECMD_FLAG ,'PRDT_RECMD'AS MODULE,'DT_CNT_RT_MICROSEG_BASKET'  AS DNA_CD,SUM(CASE WHEN DT_CNT_RT_MICROSEG_BASKET IS NOT NULL THEN 1 ELSE 0 END)  AS DEC_VALUE FROM CDS_AMT.RECMD_RANK_DAILY   GROUP BY ANL_DT,RECMD_FLAG
UNION ALL SELECT 'DAILY'AS BATCH_CYCLE,ANL_DT,RECMD_FLAG ,'PRDT_RECMD'AS MODULE,'LIFT_MICROSEG_BASKET'  AS DNA_CD,SUM(CASE WHEN LIFT_MICROSEG_BASKET IS NOT NULL THEN 1 ELSE 0 END)  AS DEC_VALUE FROM CDS_AMT.RECMD_RANK_DAILY   GROUP BY ANL_DT,RECMD_FLAG
UNION ALL SELECT 'DAILY'AS BATCH_CYCLE,ANL_DT,RECMD_FLAG ,'PRDT_RECMD'AS MODULE,'LIFT_RANK_MICROSEG_BASKET'  AS DNA_CD,SUM(CASE WHEN LIFT_RANK_MICROSEG_BASKET IS NOT NULL THEN 1 ELSE 0 END)  AS DEC_VALUE FROM CDS_AMT.RECMD_RANK_DAILY   GROUP BY ANL_DT,RECMD_FLAG
UNION ALL SELECT 'DAILY'AS BATCH_CYCLE,ANL_DT,RECMD_FLAG ,'PRDT_RECMD'AS MODULE,'LIFT_RANK_TOP10_TOPIC'  AS DNA_CD,SUM(CASE WHEN LIFT_RANK_TOP10_TOPIC IS NOT NULL THEN 1 ELSE 0 END)  AS DEC_VALUE FROM CDS_AMT.RECMD_RANK_DAILY   GROUP BY ANL_DT,RECMD_FLAG
UNION ALL SELECT 'DAILY'AS BATCH_CYCLE,ANL_DT,RECMD_FLAG ,'PRDT_RECMD'AS MODULE,'MRR'  AS DNA_CD,SUM(CASE WHEN MRR IS NOT NULL THEN 1 ELSE 0 END)  AS DEC_VALUE FROM CDS_AMT.RECMD_RANK_DAILY   GROUP BY ANL_DT,RECMD_FLAG
UNION ALL SELECT 'DAILY'AS BATCH_CYCLE,ANL_DT,RECMD_FLAG ,'PRDT_RECMD'AS MODULE,'MRR_RANK'  AS DNA_CD,SUM(CASE WHEN MRR_RANK IS NOT NULL THEN 1 ELSE 0 END)  AS DEC_VALUE FROM CDS_AMT.RECMD_RANK_DAILY   GROUP BY ANL_DT,RECMD_FLAG
UNION ALL SELECT 'DAILY'AS BATCH_CYCLE,ANL_DT,RECMD_FLAG ,'PRDT_RECMD'AS MODULE,'PRDT_DCODE_CD'  AS DNA_CD,SUM(CASE WHEN PRDT_DCODE_CD IS NOT NULL THEN 1 ELSE 0 END)  AS DEC_VALUE FROM CDS_AMT.RECMD_RANK_DAILY   GROUP BY ANL_DT,RECMD_FLAG
UNION ALL SELECT 'DAILY'AS BATCH_CYCLE,ANL_DT,RECMD_FLAG ,'PRDT_RECMD'AS MODULE,'PRDT_DCODE_NM'  AS DNA_CD,SUM(CASE WHEN PRDT_DCODE_NM IS NOT NULL THEN 1 ELSE 0 END)  AS DEC_VALUE FROM CDS_AMT.RECMD_RANK_DAILY   GROUP BY ANL_DT,RECMD_FLAG
UNION ALL SELECT 'DAILY'AS BATCH_CYCLE,ANL_DT,RECMD_FLAG ,'PRDT_RECMD'AS MODULE,'PROB_WT'  AS DNA_CD,SUM(CASE WHEN PROB_WT IS NOT NULL THEN 1 ELSE 0 END)  AS DEC_VALUE FROM CDS_AMT.RECMD_RANK_DAILY   GROUP BY ANL_DT,RECMD_FLAG
UNION ALL SELECT 'DAILY'AS BATCH_CYCLE,ANL_DT,RECMD_FLAG ,'PRDT_RECMD'AS MODULE,'PROB_WT_RANK'  AS DNA_CD,SUM(CASE WHEN PROB_WT_RANK IS NOT NULL THEN 1 ELSE 0 END)  AS DEC_VALUE FROM CDS_AMT.RECMD_RANK_DAILY   GROUP BY ANL_DT,RECMD_FLAG
UNION ALL SELECT 'DAILY'AS BATCH_CYCLE,ANL_DT,RECMD_FLAG ,'PRDT_RECMD'AS MODULE,'RANK_MICROSEG_BASKET'  AS DNA_CD,SUM(CASE WHEN RANK_MICROSEG_BASKET IS NOT NULL THEN 1 ELSE 0 END)  AS DEC_VALUE FROM CDS_AMT.RECMD_RANK_DAILY   GROUP BY ANL_DT,RECMD_FLAG
)
SELECT BATCH_CYCLE,MODULE,ANL_DT,DNA_CD
       ,SUM(CASE WHEN RECMD_FLAG IN ('CROSS-SELL PRDT')THEN DEC_VALUE END) AS CROSS_SELL
       ,SUM(CASE WHEN RECMD_FLAG IN ('RE-SELL PRDT')THEN DEC_VALUE END) AS RE_SELL
FROM DAILY_MODULE_CD_COMB_TEST
WHERE 1=1
AND TO_CHAR(ANL_DT,'YYYYMMDD') IN ('20210225') -- 기준 일자입력
GROUP BY BATCH_CYCLE
		,MODULE
		,ANL_DT
		,DNA_CD
