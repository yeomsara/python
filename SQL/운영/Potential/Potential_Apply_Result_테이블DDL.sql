/*�� ��� : ���� Bain��� ���� ��, ����ġ ��� ���� ������ ���� �� �ű� ������ insert �� python�� �� ����*/
--DROP TABLE CDS_AMT.TB_AMT_RETENTION_APPLY;
CREATE COLUMN TABLE CDS_AMT.TB_AMT_POTENTIAL_APPLY 
(				
	BAIN_GRADE_YM	NVARCHAR(6)	NOT NULL  COMMENT	'���ε�� ���ؿ���'	
,	DNA_YM_WCNT	NVARCHAR(10)	NOT NULL  COMMENT	'DNA ���ؿ���'	
,	CUST_ID	NVARCHAR(20)	NOT NULL  COMMENT	'����ȣ'	
,	BAIN_GRADE_CD	NVARCHAR(2)	NOT NULL  COMMENT	'ȸ�����_���ε�� �ڵ�'	
,	GRADE_NM	NVARCHAR(100)	COMMENT	'ȸ�����_���ε�� �̸�'	
,	CNT_RT_Q1_Q2	DECIMAL(30,6)	COMMENT	'����Ƚ�� ������(�ֱ�3������ 4~6���� ��)'	
,	DAVG_AMT_RT_Q1_Q2	DECIMAL(30,6)	COMMENT	'ȸ�簴�ܰ� ������(�ֱ�3������ 4~6���� ��)'	
,	PURCHS_CYCLE_RT_BF_0M_5M	DECIMAL(30,6)	COMMENT	'�����ֱ� ������(����� 5������ DNA ��)'	
,	PURCHS_VISIT_CHG_RT_AVG_6M	DECIMAL(30,6)	COMMENT	'���� ����Ƚ�� ��ȭ�� 6���� ���'	
,	RFM_LV_RT_BF_0M_3M	DECIMAL(30,6)	COMMENT	'RFM ��ġ��� ������(����� 3������ DNA ��) '	
,	RFM_R_SCORE_RT_BF_0M_3M	DECIMAL(30,6)	COMMENT	'RFM R Score ������(����� 3������ DNA ��)'	
,	RFM_M_SCORE_RT_BF_0M_3M	DECIMAL(30,6)	COMMENT	'RFM M Score ������(����� 3������ DNA ��)'	
,	RFM_LV_DI_FRESH1_RT_BF_0M_3M	DECIMAL(30,6)	COMMENT	'RFM ��ġ���(�ż�1) ������(����� 3������ DNA ��)'	
,	RFM_R_SCORE_RT_BF_0M_5M	DECIMAL(30,6)	COMMENT	'RFM R Score ������(����� 5������ DNA ��)'	
,	RFM_F_SCORE_RT_BF_0M_5M	DECIMAL(30,6)	COMMENT	'RFM F Score ������(����� 5������ DNA ��)'	
,	RFM_M_SCORE_RT_BF_0M_5M	DECIMAL(30,6)	COMMENT	'RFM M Score ������(����� 5������ DNA ��)'	
,	RFM_R_SCORE	SMALLINT	COMMENT	'RFM R Score'	
,	RFM_F_SCORE	SMALLINT	COMMENT	'RFM F Score'	
,	RFM_M_SCORE	SMALLINT	COMMENT	'RFM M Score'	
,	RFM_LV_DI_PRCS	SMALLINT	COMMENT	'RFM ��ġ���(�����ϻ�)'	
,	CNT_TOT_6M	DECIMAL(30)	COMMENT	'����Ƚ���հ�_6������'	
,	CNT_Q1	DECIMAL(30)	COMMENT	'����Ƚ���հ�_�ֱ�3����'	
,	CNT_Q2	DECIMAL(30)	COMMENT	'����Ƚ���հ�_4~6������'	
,	CNT_BF0M	DECIMAL(30)	COMMENT	'����Ƚ���հ�_�ֱ�1����'	
,	DAVG_AMT_TOT_6M	DECIMAL(30,3)	COMMENT	'ȸ�簴�ܰ�_6������'	
,	DAVG_AMT_Q2	DECIMAL(30,3)	COMMENT	'ȸ�簴�ܰ�_4~6������'	
,	PURCHS_CYCLE_ELAPSE	SMALLINT	COMMENT	'���Ű����'	
,	PURCHS_WKD	DECIMAL(30,3)	COMMENT	'���� ���� ���⼺'	
					
, 	CONSTRAINT  TB_AMT_POTENTIAL_APPLY PRIMARY KEY (BAIN_GRADE_YM,DNA_YM_WCNT,CUST_ID,BAIN_GRADE_CD)		
)					
UNLOAD PRIORITY 5 AUTO MERGE COMMENT '(�м���Ʈ)Potential�� ���� ����������������������̺�' 