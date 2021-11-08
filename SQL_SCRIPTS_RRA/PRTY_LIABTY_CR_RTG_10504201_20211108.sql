/* #############################################################################################
#  File-name   	: PRTY_LIABTY_CR_RTG_10504201.sql
#  Part of     	: FSDM
#  Type        	: BTEQ - LDR
#  Job         	: LOAD PRTY_LIABTY_CR_RTG
#  -------------------------------------------------------------------------
#  Purpose     	: The purpose of this file is to load data into the PRTY_LIABTY_CR_RTG
#  Type			: SCD-TYPE-1
#  Data-source 	: &&ENV_STAGE
#  Target		: &&ENV_LDR
#  -------------------------------------------------------------------------
#  DB-Version  	: TD 16.20
#  OS-Version  	: SUSE Linux  
#  -------------------------------------------------------------------------
#  Project      : ARB - REMEDIATION
#  Sub project  : 
#  Author      	: ZC0025
#  Department  	: DATA MANAGEMENT
#  Version     	: 1.0
#  Date        	: 2021-11-08
#  -------------------------------------------------------------------------
#  History:
#
# DATE_AUTH    		Version  	OWNER    			    Description OF Change
# ----    		------   	--------   			   --------------------
# 2021-11-08   	1.0   		ZC0025	                 	FIRST DRAFT
 #############################################################################################
*/


/* #############################################################################################
# Opening the BTEQ Script
 #############################################################################################*/
/* #############################################################################################
# Loggin on
 #############################################################################################*/

.SET WIDTH 255
.SET PAGELENGTH 255


.RUN FILE=&&LOGON_PATH/&&LOGON_FILE_NAME;


/* #############################################################################################
# Registering Batch and Task in ETL Metadata via Procedure Calls 
# For Batch Registration Only Pass Workflow Name as Input Parameter.  
# For Task Registration, Pass Workflow Name & Command Task Name as Input Parameters. 
 #############################################################################################*/
CALL &&ENV_METADATA.INFA_BATCH_INSRT('&&WORKFLOW_NAME');
CALL &&ENV_METADATA.INFA_SESSION_INSRT('&&WORKFLOW_NAME','PRTY_LIABTY_CR_RTG_10504201');

/* #############################################################################################
# Volatile MAX_EXECUTION_SESSION_ID table creation
 #############################################################################################*/

CREATE VOLATILE TABLE MAX_EXECUTION_SESSION_ID
(
 EXECUTION_SESSION_ID INTEGER
)
PRIMARY INDEX ( EXECUTION_SESSION_ID )
ON COMMIT PRESERVE ROWS;
 
.IF ERRORCODE<>0 THEN .QUIT ERRORCODE;



/* #############################################################################################
# SAVING SESSION EXECUTION ID INTO METADATA TABLE
 #############################################################################################*/
INSERT INTO MAX_EXECUTION_SESSION_ID
SELECT COALESCE(MAX(A.SESSION_EXECUTION_ID),-1)
FROM 
&&ENV_METADATA.SESSION_EXECUTION A 
INNER JOIN
&&ENV_METADATA.INFA_SESSIONS B ON
A.SESSION_ID = B.SESSION_ID
WHERE B.SESSION_NAME = 'PRTY_LIABTY_CR_RTG_10504201';

/* #############################################################################################
# LOGGING Start Entry in STEPS_LOG table
 #############################################################################################*/

CALL &&ENV_METADATA.START_STEP('PRTY_LIABTY_CR_RTG_10504201','&&WORKFLOW_NAME',(SELECT EXECUTION_SESSION_ID FROM MAX_EXECUTION_SESSION_ID));

BT;

/* #############################################################################################
# TRUNCATING LDR TABLE
 #############################################################################################*/

DELETE &&ENV_LDR.PRTY_LIABTY_CR_RTG_10504201 ALL;

/* #############################################################################################
# LOADING INTO LDR TABLE 
 #############################################################################################*/


INSERT INTO &&ENV_LDR.PRTY_LIABTY_CR_RTG_10504201
(RPTG_PRTY_ID,OBLGR_PRTY_ID,PRTY_LIABTY_ID,INST_ID,CR_RPORT_DTTM,CLS_DT,CR_RPORT_BAL_AMT,THIRT_DY_DLNQT_CNT,SIXTY_DY_DLNQT_CNT,NINTY_DY_DLNQT_CNT,ONE_TWENTY_DY_DLNQT_CNT,CR_LMT_AMT,PROD_EXPRY_DT,PROD_STS_CD,MTHLY_PMT_AMT,CUR_PDUE_AMT,LAST_AMT_PD,LAST_PAY_DT,NXT_PAY_DT,PAY_AS_OF_DT,PMT_FREQ,SALRY_ASGNMT_IND,SCURTY_TYPE_CD,TNUR_DAYS_CNT,RCORD_ID,REC_IND,INSRT_SESS_ID,UPDT_SESS_ID,RECYCLE_FLAG,RECYCLE_REASON,SRC_TAB_KEY,SRC_TAB_NAME,SRC_COL_NAME,RECYCLE_CDC_TIMESTAMP)

SELECT
	SGK_PRTY.PRTY_ID AS RPTG_PRTY_ID,SGK_PRTY.PRTY_ID AS OBLGR_PRTY_ID,SGK_LIABTY.PRTY_LIABTY_ID AS PRTY_LIABTY_ID,INST_TYPE.INST_TYPE_CD AS INST_ID,ISSUE_DATE AS CR_RPORT_DTTM,CLOSED_DATE AS CLS_DT,ODG_BALANCE AS CR_RPORT_BAL_AMT,(CHAR_LENGTH(TRIM(PAY_STATUS_HST))  || '-' ||  CHAR_LENGTH(OREPLACE(TRIM(PAY_STATUS_HST), '2'))) AS THIRT_DY_DLNQT_CNT,(CHAR_LENGTH(TRIM(PAY_STATUS_HST))  || '-' ||  CHAR_LENGTH(OREPLACE(TRIM(PAY_STATUS_HST), '3'))) AS SIXTY_DY_DLNQT_CNT,(CHAR_LENGTH(TRIM(PAY_STATUS_HST))  || '-' ||  CHAR_LENGTH(OREPLACE(TRIM(PAY_STATUS_HST), '4'))) AS NINTY_DY_DLNQT_CNT,(CHAR_LENGTH(TRIM(PAY_STATUS_HST))  || '-' ||  CHAR_LENGTH(OREPLACE(TRIM(PAY_STATUS_HST), '5'))) +
(CHAR_LENGTH(TRIM(PAY_STATUS_HST))  || '-' ||  CHAR_LENGTH(OREPLACE(TRIM(PAY_STATUS_HST), '6'))) +
(CHAR_LENGTH(TRIM(PAY_STATUS_HST))  || '-' ||  CHAR_LENGTH(OREPLACE(TRIM(PAY_STATUS_HST), 'W'))) AS ONE_TWENTY_DY_DLNQT_CNT,PRODUCT_LIMIT  AS CR_LMT_AMT,PRODUCT_EXP_DATE AS PROD_EXPRY_DT,PRODUCT_STATUS AS PROD_STS_CD,INST_AMOUNT AS MTHLY_PMT_AMT,PAST_DUE_BALANCE AS CUR_PDUE_AMT,LAST_AMOUNT_PAID AS LAST_AMT_PD,LAST_PAY_DATE AS LAST_PAY_DT,NEXT_PAY_DATE AS NXT_PAY_DT,AS_OF_DATE AS PAY_AS_OF_DT,PAYMENT_FREQ AS PMT_FREQ,SALARY_ASSIGN  AS SALRY_ASGNMT_IND,SECURITY_TYPE  AS SCURTY_TYPE_CD,TENURE AS TNUR_DAYS_CNT
    ,10504201 AS RCORD_ID
	,'I' AS REC_IND
	,SESS.EXECUTION_SESSION_ID 	
	,SESS.EXECUTION_SESSION_ID
	,CASE WHEN (SGK_PRTY.PRTY_ID IS NULL OR SGK_PRTY.PRTY_ID IS NULL OR SGK_LIABTY.PRTY_LIABTY_ID IS NULL OR INST_TYPE.INST_TYPE_CD IS NULL) THEN 'Y' ELSE 'N' END AS RECYCLE_FLAG,                  
	(CASE WHEN SGK_PRTY.PRTY_ID  IS NULL THEN 'RPTG_PRTY_ID MISSED' ELSE 'RPTG_PRTY_ID MATCHED' END||'-'||CASE WHEN SGK_PRTY.PRTY_ID  IS NULL THEN 'OBLGR_PRTY_ID MISSED' ELSE 'OBLGR_PRTY_ID MATCHED' END||'-'||CASE WHEN SGK_LIABTY.PRTY_LIABTY_ID  IS NULL THEN 'PRTY_LIABTY_ID MISSED' ELSE 'PRTY_LIABTY_ID MATCHED' END||'-'||CASE WHEN INST_TYPE.INST_TYPE_CD  IS NULL THEN 'INST_ID MISSED' ELSE 'INST_ID MATCHED' END) AS RECYCLE_REASON,
	SRC.SRC_TAB_KEY AS SRC_TAB_KEY,                   
	SRC.SRC_TAB_NAME AS SRC_TAB_NAME,                  
	SRC.SRC_COL_NAME AS SRC_COL_NAME,                  
	SRC.CDC_TIMESTAMP AS RECYCLE_CDC_TIMESTAMP

FROM
	(
		SELECT * FROM
		
		(
			SELECT PRODUCT_LIMIT ,CLOSED_DATE,PAST_DUE_BALANCE,SECURITY_TYPE ,REC_CTR,ID_CREDITOR,C_TP_PROD_APPL,ACCOUNT_NUMBER,PG_RQS,AS_OF_DATE,SALARY_ASSIGN ,CD_NDG_FSC,ODG_BALANCE,ISSUE_DATE,PAY_STATUS_HST,TENURE,PRODUCT_EXP_DATE,NEXT_PAY_DATE,INST_AMOUNT,PRODUCT_STATUS,PAYMENT_FREQ,LAST_AMOUNT_PAID,LAST_PAY_DATE,CDC_TIMESTAMP
			,'No Info in SMX' AS SRC_TAB_KEY,'CTF-KSA_ICTBB08' AS SRC_TAB_NAME,	'PRODUCT_LIMIT ,CLOSED_DATE,PAST_DUE_BALANCE,SECURITY_TYPE ,REC_CTR,ID_CREDITOR,C_TP_PROD_APPL,ACCOUNT_NUMBER,PG_RQS,AS_OF_DATE,SALARY_ASSIGN ,CD_NDG_FSC,ODG_BALANCE,ISSUE_DATE,PAY_STATUS_HST,TENURE,PRODUCT_EXP_DATE,NEXT_PAY_DATE,INST_AMOUNT,PRODUCT_STATUS,PAYMENT_FREQ,LAST_AMOUNT_PAID,LAST_PAY_DATE,CDC_TIMESTAMP' AS SRC_COL_NAME
			FROM &&ENV_STAGE.CTF-KSA_ICTBB08 SRC 
			WEHRE REC_CTR IS NOT NULL AND TRIM(REC_CTR)<> '' AND ID_CREDITOR IS NOT NULL AND TRIM(ID_CREDITOR)<> ''  AND C_TP_PROD_APPL IS NOT NULL AND TRIM(C_TP_PROD_APPL)<> '' AND ACCOUNT_NUMBER IS NOT NULL AND TRIM(ACCOUNT_NUMBER)<> '' AND PG_RQS IS NOT NULL AND TRIM(PG_RQS)<> ''  '   
			
			UNION ALL
			
			SELECT PRODUCT_LIMIT ,CLOSED_DATE,PAST_DUE_BALANCE,SECURITY_TYPE ,REC_CTR,ID_CREDITOR,C_TP_PROD_APPL,ACCOUNT_NUMBER,PG_RQS,AS_OF_DATE,SALARY_ASSIGN ,CD_NDG_FSC,ODG_BALANCE,ISSUE_DATE,PAY_STATUS_HST,TENURE,PRODUCT_EXP_DATE,NEXT_PAY_DATE,INST_AMOUNT,PRODUCT_STATUS,PAYMENT_FREQ,LAST_AMOUNT_PAID,LAST_PAY_DATE,CDC_TIMESTAMP
			,'No Info in SMX' AS SRC_TAB_KEY,'CTF-KSA_ICTBB08' AS SRC_TAB_NAME,	'PRODUCT_LIMIT ,CLOSED_DATE,PAST_DUE_BALANCE,SECURITY_TYPE ,REC_CTR,ID_CREDITOR,C_TP_PROD_APPL,ACCOUNT_NUMBER,PG_RQS,AS_OF_DATE,SALARY_ASSIGN ,CD_NDG_FSC,ODG_BALANCE,ISSUE_DATE,PAY_STATUS_HST,TENURE,PRODUCT_EXP_DATE,NEXT_PAY_DATE,INST_AMOUNT,PRODUCT_STATUS,PAYMENT_FREQ,LAST_AMOUNT_PAID,LAST_PAY_DATE,CDC_TIMESTAMP' AS SRC_COL_NAME
			FROM &&ENV_STAGE_RECYCLE.CTF-KSA_ICTBB08_PRTY_LIABTY_CR_RTG_10504201 SRC 
			WEHRE REC_CTR IS NOT NULL AND TRIM(REC_CTR)<> '' AND ID_CREDITOR IS NOT NULL AND TRIM(ID_CREDITOR)<> ''  AND C_TP_PROD_APPL IS NOT NULL AND TRIM(C_TP_PROD_APPL)<> '' AND ACCOUNT_NUMBER IS NOT NULL AND TRIM(ACCOUNT_NUMBER)<> '' AND PG_RQS IS NOT NULL AND TRIM(PG_RQS)<> ''  '   
			
		) AA
        
		QUALIFY ROW_NUMBER() OVER ( PARTITION BY REC_CTR,ID_CREDITOR,C_TP_PROD_APPL,ACCOUNT_NUMBER,PG_RQS ORDER BY CDC_TIMESTAMP DESC)=1
	) SRC

	LEFT JOIN &&PRD_HLP.SGK_PRTY SGK_PRTY ON SGK_PRTY.NAT_KEY='SIMAH'
AND SGK_PRTY.TYPE_CD= 'CREDIT RATING AGENCY' 
AND INST_ID=(SELECT INST_TYPE_CD FROM INST_TYPE WHERE INST_TYPE_NAME='ALRAJHI BANK KSA') 
LEFT JOIN &&PRD_HLP.SGK_PRTY SGK_PRTY ON SGK_PRTY.NAT_KEY=TRIM(SRC.CD_NDG_FSC)  
AND SGK_PRTY.TYPE_CD ='CUSTOMER'
AND SGK_PRTY.INST_ID=(SELECT INST_TYPE_CD FROM INST_TYPE WHERE INST_TYPE_NAME='ALRAJHI BANK KSA')
LEFT JOIN &&PRD_HLP.SGK_LIABTY SGK_LIABTY ON SGK_LIABTY.NAT_KEY=TRIM(C_TP_PROD_APPL)||'-'||TRIM(ACCOUNT_NUMBER)||'-'||TRIM(ID_CREDITOR)||'-'||TRIM(PG_RQS)||'-'||TRIM(REC_CTR) AND SGK_LIABTY.TYPE_CD='CONSUMER LIABILITY' 
AND SGK_LIABTY.INST_ID=(SELECT INST_TYPE_CD FROM INST_TYPE WHERE INST_TYPE_NAME='ALRAJHI BANK KSA')
LEFT JOIN &&PRD_FSDM.INST_TYPE INST_TYPE ON INST_TYPE_NAME='ALRAJHI BANK KSA'
	
    CROSS JOIN
	MAX_EXECUTION_SESSION_ID SESS;


ET;

.IF ERRORCODE = 0 THEN .GOTO SUCCESS_STEP


/*#############################################################################################
# Logging Failure Entry in STEPS_LOG table in case failure
#############################################################################################*/
.IF ERRORCODE <> 0 THEN CALL &&ENV_METADATA.FAIL_STEP('PRTY_LIABTY_CR_RTG_10504201','&&WORKFLOW_NAME',(SELECT EXECUTION_SESSION_ID FROM MAX_EXECUTION_SESSION_ID));
.QUIT ERRORCODE;


.LABEL SUCCESS_STEP

/*#############################################################################################
#  Recycle Handling PROCEDURE is invoked to move rows to Recycle Table.
#  Two Input paramters should be passed to RECYCLE_HANDLING SP
#  First input argument should be LDR table name like 'PRTY_LIABTY_CR_RTG_10504201'
#  Second argument should be Job Type which can be 0,1, or 2
#  Third argument is an output argument which recieves status of Procedure Call
#############################################################################################*/
CALL &&ENV_METADATA.RECYCLE_HANDLING('PRTY_LIABTY_CR_RTG_10504201',1,P_RESULT);

.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;
 

/*#############################################################################################
# LOGGING Success Entry in STEPS_LOG table
#############################################################################################*/
CALL &&ENV_METADATA.END_STEP('PRTY_LIABTY_CR_RTG_10504201','&&WORKFLOW_NAME',(SELECT EXECUTION_SESSION_ID FROM MAX_EXECUTION_SESSION_ID),'SOURCE_TABLE_NAME',0,0,0,0,0,'LDR');
	
.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;
	


/*#############################################################################################
#  Closing the BTEQ Script, Session Closed and Logging off
#############################################################################################*/
  
.LOGOFF
.QUIT