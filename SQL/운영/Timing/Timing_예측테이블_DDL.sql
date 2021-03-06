CREATE COLUMN TABLE CDS_AMT.TB_AMT_TIMING_APPLY( 
			  CUST_ID                       NVARCHAR(10) NOT NULL COMMENT '고객번호'        
			 ,BAIN_GRADE_YM                 NVARCHAR(11) NOT NULL COMMENT '베인등급기준월'
			 ,DNA_YMD                       NVARCHAR(11) NOT NULL COMMENT 'DNA산정일'
			 ,PRED_YMD                      NVARCHAR(11) NOT NULL COMMENT '예측수행일'
			 ,DAVG_PURCHS_AMT_EXC_ELEC      DECIMAL               COMMENT '1년 구매 객단가(가전제외)' 
			 ,TOP1_STR_DSTNC                DECIMAL               COMMENT 'TOP1 점포와의 거리'
			 ,TOP2_STR_DSTNC                DECIMAL               COMMENT 'TOP2 점포와의 거리'
			 ,CLDR_EVENT_PRE_UNITY          DECIMAL               COMMENT '달력행사선호도-통합'
			 ,CLDR_EVENT_PRE_TREDI          DECIMAL               COMMENT '달력행사선호도-명절'
			 ,PURCHS_WEND_HLDY              DECIMAL               COMMENT '주말/공휴일 선호도'
			 ,PURCHS_CYCLE                  DECIMAL               COMMENT '구매주기'
			 ,PURCHS_CYCLE_CHG              DECIMAL               COMMENT '구매주기 변동성' 
			 ,PURCHS_CYCLE_REGUL_YN         TINYINT               COMMENT '구매주기 규칙성 여부'
			 ,PURCHS_CYCLE_ARRVL_RT         DECIMAL               COMMENT '구매주기 도래율'
			 ,PURCHS_VISIT_CHG_RT_AVG_6M    DECIMAL               COMMENT '연간 구매횟수 변화율 6개월 평균'
			 ,RFM_LV                        DECIMAL               COMMENT 'RFM 가치등급 순위 (R/F/M- M스코어의 가중치가 높음)(1~100)'  
			 ,RFM_F_SCORE                   DECIMAL               COMMENT 'RFM_F SCORE(구매횟수 순위)'
			 ,RFM_M_SCORE                   DECIMAL               COMMENT 'RFM_M_SCORE(구매금액 순위)'
 			 ,RFM_LV_DI_FRESH1              DECIMAL               COMMENT '신선1 담당 RFM가치등급 순위 (1~100)'  
 			 ,RFM_LV_DI_FRESH2              DECIMAL               COMMENT '신선2 담당 RFM가치등급 순위 (1~100)'
			 ,RFM_LV_DI_PEACOCK             DECIMAL               COMMENT '피코크 담당 RFM가치등급 순위 (1~100)'
			 ,RFM_LV_DI_PRCS          	    DECIMAL               COMMENT '가공 담당 RFM가치등급 순위 (1~100)'
			 ,RFM_LV_DI_HNR          	    DECIMAL               COMMENT '헬스일상 담당 RFM가치등급 순위 (1~100)'
			 ,RFM_LV_DI_LIVING              DECIMAL               COMMENT '리빙담당 RFM가치등급 순위 (1~100)'
			 ,RFM_LV_DI_MOLLYS              DECIMAL               COMMENT 'Mollys 담당 RFM가치등급 순위 (1~100)'
			 ,RFM_LV_DI_ELEC_CULTR          DECIMAL               COMMENT '가전문화 담당 RFM가치등급 순위 (1~100)'  
			 ,RFM_LV_DI_FSHN                DECIMAL               COMMENT '패션담당 RFM가치등급 순위 (1~100)'
			 ,TOT_FREQUENCY_CNT             DECIMAL               COMMENT '6개월 총 구매 횟수'
			 ,RECENT_1M_SALES_INDEX         DECIMAL               COMMENT '최근 1개월 구매지수 ( 최근 1개월 회당 객단가 / 1년 회당 객단가(가전제외))'
			 ,RECENT_3M_FREQUENCY_DIFF_RT   DECIMAL               COMMENT '과거 3개월(-3M~-6M)대비 최근3개월 방문횟수 증감율'
			 ,EMT_MALL_VISIT_YN             TINYINT               COMMENT '이마트몰 방문 여부'
			 ,EMT_MALL_LAST_PUCHS_DIFF	    DECIMAL               COMMENT '이마트몰 마지막 구매경과일' 
			 ,USEFL_POINT                   DECIMAL               COMMENT '가용포인트'
			 ,MAIN_PRDT_ARRVL_RT_DIFF_ABS   DECIMAL               COMMENT '주구매상품 구매주기 도래율'
             ,PREFER_PRDT_ARRVL_RT_DIFF_ABS DECIMAL               COMMENT '선호상품 구매주기 도래율'
			 ,MIN_PRDT_PURCHS_CYCLE         DECIMAL               COMMENT '(소분류) 구매주기 도래율 최소값'
			 ,MAX_PRDT_PURCHS_CYCLE         DECIMAL               COMMENT '(소분류) 구매주기 도래율 최대값'
			 ,PRDT_DCODE10_CNT              DECIMAL               COMMENT '구매주기 도래율 0.5미만인 신선1담당 상품수'
	         ,PRDT_DCODE11_CNT              DECIMAL               COMMENT '구매주기 도래율 0.5미만인 신선2담당 상품수'
			 ,PRDT_DCODE20_CNT              DECIMAL               COMMENT '구매주기 도래율 0.5미만인 피코크담당 상품수'
			 ,PRDT_DCODE30_CNT              DECIMAL               COMMENT '구매주기 도래율 0.5미만인 가공담당 상품수'
			 ,PRDT_DCODE40_CNT              DECIMAL               COMMENT '구매주기 도래율 0.5미만인 헬스&일상담당 상품수'
			 ,PRDT_DCODE41_CNT              DECIMAL               COMMENT '구매주기 도래율 0.5미만인 리빙담당 상품수' 
			 ,PRDT_DCODE42_CNT              DECIMAL               COMMENT '구매주기 도래율 0.5미만인 Mollys BM담당 상품수'
			 ,PRDT_DCODE50_CNT              DECIMAL               COMMENT '구매주기 도래율 0.5미만인 가전문화담당 상품수'
			 ,PRDT_DCODE60_CNT              DECIMAL               COMMENT '구매주기 도래율 0.5미만인 패션담당 상품수'
	         ,LAST_ONLINE_DIFF_DAYS         SMALLINT              COMMENT '마지막 온라인 관계사 구매 경과일'
			 ,LAST_OFFLINE_DIFF_DAYS        SMALLINT              COMMENT '마지막 오프라인 관계사 구매 경과일'
			 ,LAST_OFFLINE_BIZTP_DIFF_DAYS  SMALLINT              COMMENT '마지막 오프라인 업태 구매 경과일'  
			 ,C1_EVENT_DAYS_CNT             DECIMAL               COMMENT '미래 1주 달력이벤트 행사일수'
			 ,C2_EVENT_DAYS_CNT             DECIMAL               COMMENT '미래 2~3주 달력이벤트 행사일수'
			 ,C3_EVENT_DAYS_CNT             DECIMAL               COMMENT '미래 4~5주 달력이벤트 행사일수'
			 ,C1_HOLI_DAYS_CNT              DECIMAL               COMMENT '미래 1주 공휴일수'
			 ,C2_HOLI_DAYS_CNT              DECIMAL               COMMENT '미래 2~3주 공휴일수'
			 ,C3_HOLI_DAYS_CNT              DECIMAL               COMMENT '미래 4~5주 공휴일수'
			 ,C1_TREDI_DAYS_CNT             DECIMAL               COMMENT '미래 1주 명절 행사일수'
			 ,C2_TREDI_DAYS_CNT             DECIMAL               COMMENT '미래 2~3주 명절 행사일수'
			 ,C3_TREDI_DAYS_CNT             DECIMAL               COMMENT '미래 4~5주 명절 행사일수'
)
