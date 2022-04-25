# -*- coding: utf-8 -*-
"""Untitled2.ipynb

Automatically generated by Colaboratory.

Original file is located at
    https://colab.research.google.com/drive/1JenGqWPpjOgCt95DlJGN21UKKjsf1Ah_
"""

# Commented out IPython magic to ensure Python compatibility.
# %sql
INSERT OVERWRITE SCHEMA_CURATED.TABLE_PAT
SELECT 
       DPF.PAT_ENC_CSN_ID
     , DPF.PAT_ID
     , PAT.PAT_MRN_ID
     , DPF.CONTACT_DATE
     , DPF.LINE
     , DPF.LOC_ID AS FCLTY_LOC_ID
     , DPF.STATUS_DTTM
     , DPF.DEST_DTTM
     , DPF.DP_FAC_STATUS_C
     , ZCDP.NAME AS REQ_STATUS         
     , POS.POS_NAME
     , DPF.PROV_ID
     , SER.PROV_NAME
     , DPF.CM_CT_OWNER_ID
     , DPF.DEPARTMENT_ID
     , PE.EFFECTIVE_DEPT_ID
     , DLS.DEPARTMENT_NAME
     , DLS.DEPARTMENT_ABBR
     , DLS.LOC_ID
     , DLS.LOC_NAME 
     , DLS.LOC_HOSP_PAR_ID
     , DLS.PARENT_LOC_NAME
     , SA.RPT_GRP_SIX
     , ZCCT.NAME AS CRDNTN_TYP_NM
  FROM SCHEMA_RAW.DP_FACILITY DPF
  LEFT JOIN SCHEMA_RAW.CLARITY_POS POS       ON DPF.LOC_ID = POS.POS_ID
  LEFT JOIN SCHEMA_RAW.CLARITY_SER SER       ON DPF.PROV_ID = SER.PROV_ID
  LEFT JOIN SCHEMA_RAW.ZC_DP_FAC_STATUS ZCDP ON DPF.DP_FAC_STATUS_C = ZCDP.DP_FAC_STATUS_C
  LEFT JOIN SCHEMA_RAW.ZC_DP_COORD_TYPE ZCCT ON DPF.COORD_TYPE_C  = ZCCT.DP_COORD_TYPE_C
 INNER JOIN SCHEMA_RAW.PAT_ENC PE            ON DPF.PAT_ENC_CSN_ID = PE.PAT_ENC_CSN_ID
 INNER JOIN SCHEMA_RAW.PATIENT PAT           ON DPF.PAT_ID = PAT.PAT_ID
  LEFT JOIN BI_CLARITY.MV_DEP_LOC_SA DLS     ON PE.EFFECTIVE_DEPT_ID = DLS.DEPARTMENT_ID
  LEFT JOIN SCHEMA_RAW.CLARITY_SA SA         ON SA.SERV_AREA_ID = DLS.SERV_AREA_ID
;