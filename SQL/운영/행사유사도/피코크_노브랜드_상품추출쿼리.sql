
--노브랜드
SELECT DISTINCT PRDT_CD,PRDT_NM
FROM
(
SELECT DISTINCT PRDT_CD,PRDT_NM
FROM CDS_DW.TB_DW_PRDT_MASTR
WHERE BRAND_CD='100108'
AND AFLCO_CD='001'
AND BIZTP_CD='10'

UNION ALL

SELECT DISTINCT PRDT_CD,PRDT_NM
FROM CDS_DW.TB_DW_PRDT_MASTR
WHERE (PRDT_NM LIKE('%노브랜드%')
OR PRDT_NM LIKE('%NO BRAND%')
OR PRDT_NM LIKE('%NBR%')
OR PRDT_NM LIKE('%NOBRAND%')
)
AND AFLCO_CD='001'
AND BIZTP_CD='10'

UNION ALL

SELECT DISTINCT PRDT_CD,PRDT_NM
FROM CDS_DW.TB_DW_PRDT_MASTR AS A
INNER JOIN CDS_DW.TB_DW_PRDT_DCODE_CD AS B ON A.PRDT_DCODE_CD=B.PRDT_DCODE_CD AND A.AFLCO_CD=B.AFLCO_CD AND A.BIZTP_CD=B.BIZTP_CD
WHERE PRDT_DCODE_NM LIKE ('N)%')
OR PRDT_DCODE_NM LIKE ('노브랜드%')
AND A.AFLCO_CD='001'
AND A.BIZTP_CD='10'
);

--피코크 
SELECT DISTINCT PRDT_CD,PRDT_NM
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
);