Create Table CDS_AMT.TB_AMT_TIMING_RESULT (
			  CUST_ID                  NVARCHAR(20)  COMMENT '고객번호' 
			 ,PRED_YMD                 NVARCHAR(10)  COMMENT '예측수행일'
			 ,PREDICT_CLASS            NVARCHAR(1)   COMMENT '예측 클래스'
			 ,EVENT_CLASS              NVARCHAR(1)   COMMENT '이벤트 후처리 클래스'
			 ,C1_PROB                  DECIMAL       COMMENT 'C1 original 예측확률'
			 ,C2_PROB                  DECIMAL       COMMENT 'C2 original 예측확률'
			 ,C3_PROB                  DECIMAL       COMMENT 'C3 original 예측확률'
			 ,C1_EVENT_SOFTMAX_PROB    DECIMAL       COMMENT 'C1 이벤트 후처리 Softmax 예측확률'
			 ,C2_EVENT_SOFTMAX_PROB    DECIMAL       COMMENT 'C2 이벤트 후처리 Softmax 예측확률'
			 ,C3_EVENT_SOFTMAX_PROB    DECIMAL       COMMENT 'C3 이벤트 후처리 Softmax 예측확률'
             ,PRIMARY KEY(CUST_ID,PRED_YMD)
			 )

