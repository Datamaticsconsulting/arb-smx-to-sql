/* #############################################################################################
#  File-name   	: SGK_LIABTY_10504201.sql
#  Part of     	: FSDM
#  Type        	: BTEQ - LDR
#  Job         	: LOAD SGK_LIABTY
#  -------------------------------------------------------------------------
#  Purpose     	: The purpose of this file is to load data into the SGK_LIABTY
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
CALL &&ENV_METADATA.INFA_SESSION_INSRT('&&WORKFLOW_NAME','SGK_LIABTY_10504201');

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
WHERE B.SESSION_NAME = 'SGK_LIABTY_10504201';

/* #############################################################################################
# LOGGING Start Entry in STEPS_LOG table
 #############################################################################################*/

CALL &&ENV_METADATA.START_STEP('SGK_LIABTY_10504201','&&WORKFLOW_NAME',(SELECT EXECUTION_SESSION_ID FROM MAX_EXECUTION_SESSION_ID));

BT;

/* #############################################################################################
# TRUNCATING LDR TABLE
 #############################################################################################*/

DELETE &&ENV_LDR.SGK_LIABTY_10504201 ALL;

/* #############################################################################################
# LOADING INTO LDR TABLE 
 #############################################################################################*/


INSERT INTO &&ENV_LDR.SGK_LIABTY_10504201
(INST_ID,AGMT_IDNTFTN_TYPE_CD,AGMT_IDNTFTN_TYPE_NAME,AGMT_IDNTFTN_TYPE_DESC,INST_ID,PRTY_LIABTY_ID,NAT_KEY,RCORD_ID,REC_IND,INSRT_SESS_ID,UPDT_SESS_ID)

SELECT
	INST_TYPE.INST_TYPE_CD AS INST_ID,"GENERATE SGK FOR 'UNKNOWN'
GENERATE SGK BASED ON UNIQUE VALUE OF TRIM(AGMT_IDNFTN_TYPE_NAME)" AS AGMT_IDNTFTN_TYPE_CD,AGMT_IDNFTN_TYPE_NAME AS AGMT_IDNTFTN_TYPE_NAME,AGMT_IDNFTN_TYPE_NAME AS AGMT_IDNTFTN_TYPE_DESC,INST_TYPE.INST_TYPE_CD AS INST_ID,GENERATE SGK FOR NAT_KEY = 'UNKNOWN' AND TYPE_CD , INST_ID COMBINATION
GENERATE SGK BASED ON UNIQUE VALUE OF TRIM(ACCOUNT_NUMBER)||'-'||TRIM(PG_RQS)||'-'||TRIM(ID_CREDITOR)||'-'||TRIM(REC_CTR)||'-'||TRIM(C_TP_PROD_APPL) AND TYPE_CD , INST_ID COMBINATION AS PRTY_LIABTY_ID
    ,10504201 AS RCORD_ID
	,'I' AS REC_IND
	,SESS.EXECUTION_SESSION_ID 	
	,SESS.EXECUTION_SESSION_ID

FROM
	(
		SELECT * FROM
		
		(
			SELECT REC_CTR,ID_CREDITOR,C_TP_PROD_APPL,ACCOUNT_NUMBER,PG_RQS,CDC_TIMESTAMP
			,'No Info in SMX' AS SRC_TAB_KEY,'CTF-KSA_ICTBB08' AS SRC_TAB_NAME,	'REC_CTR,ID_CREDITOR,C_TP_PROD_APPL,ACCOUNT_NUMBER,PG_RQS,CDC_TIMESTAMP' AS SRC_COL_NAME
			FROM &&ENV_STAGE.CTF-KSA_ICTBB08 SRC 
			WEHRE REC_CTR IS NOT NULL AND TRIM(REC_CTR)<> '' AND ID_CREDITOR IS NOT NULL AND TRIM(ID_CREDITOR)<> ''  AND C_TP_PROD_APPL IS NOT NULL AND TRIM(C_TP_PROD_APPL)<> '' AND ACCOUNT_NUMBER IS NOT NULL AND TRIM(ACCOUNT_NUMBER)<> '' AND PG_RQS IS NOT NULL AND TRIM(PG_RQS)<> ''     
			
		) AA
        
		QUALIFY ROW_NUMBER() OVER ( PARTITION BY REC_CTR,ID_CREDITOR,C_TP_PROD_APPL,ACCOUNT_NUMBER,PG_RQS ORDER BY CDC_TIMESTAMP DESC)=1
	) SRC

	LEFT JOIN &&PRD_FSDM.INST_TYPE INST_TYPE ON INST_TYPE_NAME='ALRAJHI BANK KSA'
LEFT JOIN &&PRD_FSDM.INST_TYPE INST_TYPE ON INST_TYPE_NAME='ALRAJHI BANK KSA'
	
    CROSS JOIN
	MAX_EXECUTION_SESSION_ID SESS;


ET;

.IF ERRORCODE = 0 THEN .GOTO SUCCESS_STEP


/*#############################################################################################
# Logging Failure Entry in STEPS_LOG table in case failure
#############################################################################################*/
.IF ERRORCODE <> 0 THEN CALL &&ENV_METADATA.FAIL_STEP('SGK_LIABTY_10504201','&&WORKFLOW_NAME',(SELECT EXECUTION_SESSION_ID FROM MAX_EXECUTION_SESSION_ID));
.QUIT ERRORCODE;


.LABEL SUCCESS_STEP

/*#############################################################################################
#  Recycle Handling PROCEDURE is invoked to move rows to Recycle Table.
#  Two Input paramters should be passed to RECYCLE_HANDLING SP
#  First input argument should be LDR table name like 'SGK_LIABTY_10504201'
#  Second argument should be Job Type which can be 0,1, or 2
#  Third argument is an output argument which recieves status of Procedure Call
#############################################################################################*/
CALL &&ENV_METADATA.RECYCLE_HANDLING('SGK_LIABTY_10504201',1,P_RESULT);

.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;
 

/*#############################################################################################
# LOGGING Success Entry in STEPS_LOG table
#############################################################################################*/
CALL &&ENV_METADATA.END_STEP('SGK_LIABTY_10504201','&&WORKFLOW_NAME',(SELECT EXECUTION_SESSION_ID FROM MAX_EXECUTION_SESSION_ID),'SOURCE_TABLE_NAME',0,0,0,0,0,'LDR');
	
.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;
	


/*#############################################################################################
#  Closing the BTEQ Script, Session Closed and Logging off
#############################################################################################*/
  
.LOGOFF
.QUIT