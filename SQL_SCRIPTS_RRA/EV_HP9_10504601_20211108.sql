/* #############################################################################################
#  File-name   	: EV_HP9_10504601.sql
#  Part of     	: FSDM
#  Type        	: BTEQ - LDR
#  Job         	: LOAD EV_HP9
#  -------------------------------------------------------------------------
#  Purpose     	: The purpose of this file is to load data into the EV_HP9
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
CALL &&ENV_METADATA.INFA_SESSION_INSRT('&&WORKFLOW_NAME','EV_HP9_10504601');

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
WHERE B.SESSION_NAME = 'EV_HP9_10504601';

/* #############################################################################################
# LOGGING Start Entry in STEPS_LOG table
 #############################################################################################*/

CALL &&ENV_METADATA.START_STEP('EV_HP9_10504601','&&WORKFLOW_NAME',(SELECT EXECUTION_SESSION_ID FROM MAX_EXECUTION_SESSION_ID));

BT;

/* #############################################################################################
# TRUNCATING LDR TABLE
 #############################################################################################*/

DELETE &&ENV_LDR.EV_HP9_10504601 ALL;

/* #############################################################################################
# LOADING INTO LDR TABLE 
 #############################################################################################*/


INSERT INTO &&ENV_LDR.EV_HP9_10504601
(EV_ID,EV_SBTYPE_CD,EV_CTGY_CD,EV_TYPE_CD,INST_ID,EV_STRT_DTTM,EV_END_DTTM,EV_ACTVY_TYPE_CD,EV_RSN_CD,HOST_EV_NUM,RCORD_ID,REC_IND,INSRT_SESS_ID,UPDT_SESS_ID,RECYCLE_FLAG,RECYCLE_REASON,SRC_TAB_KEY,SRC_TAB_NAME,SRC_COL_NAME,RECYCLE_CDC_TIMESTAMP)

SELECT
	SGK_EV.EV_ID AS EV_ID,EV_SBTYPE.EV_SBTYPE_CD AS EV_SBTYPE_CD,EV_CTGY_TYPE.EV_CTGY_CD AS EV_CTGY_CD,EV_TYPE.EV_TYPE_CD AS EV_TYPE_CD,INST_TYPE.INST_TYPE_CD AS INST_ID,DT_RQS AS EV_STRT_DTTM,CAST ('9999-12-31 00:00:00' AS TIMESTAMP(0) ) AS EV_END_DTTM,NULL AS EV_ACTVY_TYPE_CD,NULL AS EV_RSN_CD,TRIM(PG_RQS) AS HOST_EV_NUM
    ,10504601 AS RCORD_ID
	,'I' AS REC_IND
	,SESS.EXECUTION_SESSION_ID 	
	,SESS.EXECUTION_SESSION_ID
	,CASE WHEN (SGK_EV.EV_ID IS NULL OR EV_SBTYPE.EV_SBTYPE_CD IS NULL OR EV_CTGY_TYPE.EV_CTGY_CD IS NULL OR EV_TYPE.EV_TYPE_CD IS NULL OR INST_TYPE.INST_TYPE_CD IS NULL) THEN 'Y' ELSE 'N' END AS RECYCLE_FLAG,                  
	(CASE WHEN SGK_EV.EV_ID  IS NULL THEN 'EV_ID MISSED' ELSE 'EV_ID MATCHED' END||'-'||CASE WHEN EV_SBTYPE.EV_SBTYPE_CD  IS NULL THEN 'EV_SBTYPE_CD MISSED' ELSE 'EV_SBTYPE_CD MATCHED' END||'-'||CASE WHEN EV_CTGY_TYPE.EV_CTGY_CD  IS NULL THEN 'EV_CTGY_CD MISSED' ELSE 'EV_CTGY_CD MATCHED' END||'-'||CASE WHEN EV_TYPE.EV_TYPE_CD  IS NULL THEN 'EV_TYPE_CD MISSED' ELSE 'EV_TYPE_CD MATCHED' END||'-'||CASE WHEN INST_TYPE.INST_TYPE_CD  IS NULL THEN 'INST_ID MISSED' ELSE 'INST_ID MATCHED' END) AS RECYCLE_REASON,
	SRC.SRC_TAB_KEY AS SRC_TAB_KEY,                   
	SRC.SRC_TAB_NAME AS SRC_TAB_NAME,                  
	SRC.SRC_COL_NAME AS SRC_COL_NAME,                  
	SRC.CDC_TIMESTAMP AS RECYCLE_CDC_TIMESTAMP

FROM
	(
		SELECT * FROM
		
		(
			SELECT PG_RQS,DT_RQS,CDC_TIMESTAMP
			,'No Info in SMX' AS SRC_TAB_KEY,'CTF-KSA_ICTBB01' AS SRC_TAB_NAME,	'PG_RQS,DT_RQS,CDC_TIMESTAMP' AS SRC_COL_NAME
			FROM &&ENV_STAGE.CTF-KSA_ICTBB01 SRC 
			WEHRE  PG_RQS IS NOT NULL AND TRIM(PG_RQS)<> '' '
			
			UNION ALL
			
			SELECT PG_RQS,DT_RQS,CDC_TIMESTAMP
			,'No Info in SMX' AS SRC_TAB_KEY,'CTF-KSA_ICTBB01' AS SRC_TAB_NAME,	'PG_RQS,DT_RQS,CDC_TIMESTAMP' AS SRC_COL_NAME
			FROM &&ENV_STAGE_RECYCLE.CTF-KSA_ICTBB01_EV_HP9_10504601 SRC 
			WEHRE  PG_RQS IS NOT NULL AND TRIM(PG_RQS)<> '' '
			
		) AA
        
		QUALIFY ROW_NUMBER() OVER ( PARTITION BY  PG_RQS ORDER BY CDC_TIMESTAMP DESC)=1
	) SRC

	LEFT JOIN &&PRD_HLP.SGK_EV SGK_EV ON SGK_EV.NAT_KEY=TRIM(SRC.PG_RQS) AND SGK_EV.TYPE_CD = 'LOAN APPLICATION INQUIRY EVENT' 
AND SGK_EV.INST_ID=(SELECT INST_TYPE_CD FROM INST_TYPE WHERE INST_TYPE_NAME='ALRAJHI BANK KSA')
LEFT JOIN &&PRD_FSDM.EV_SBTYPE EV_SBTYPE ON EV_SBTYPE_NAME='FINANCIAL EVENT'
LEFT JOIN &&PRD_FSDM.EV_CTGY_TYPE EV_CTGY_TYPE ON EV_CTGY_NAME='OTHER EVENT'
LEFT JOIN &&PRD_FSDM.EV_TYPE EV_TYPE ON EV_TYPE_NAME='LOAN APPLICATION INQUIRYEVENT'
LEFT JOIN &&PRD_FSDM.INST_TYPE INST_TYPE ON INST_TYPE_NAME='ALRAJHI BANK KSA'
	
    CROSS JOIN
	MAX_EXECUTION_SESSION_ID SESS;


ET;

.IF ERRORCODE = 0 THEN .GOTO SUCCESS_STEP


/*#############################################################################################
# Logging Failure Entry in STEPS_LOG table in case failure
#############################################################################################*/
.IF ERRORCODE <> 0 THEN CALL &&ENV_METADATA.FAIL_STEP('EV_HP9_10504601','&&WORKFLOW_NAME',(SELECT EXECUTION_SESSION_ID FROM MAX_EXECUTION_SESSION_ID));
.QUIT ERRORCODE;


.LABEL SUCCESS_STEP

/*#############################################################################################
#  Recycle Handling PROCEDURE is invoked to move rows to Recycle Table.
#  Two Input paramters should be passed to RECYCLE_HANDLING SP
#  First input argument should be LDR table name like 'EV_HP9_10504601'
#  Second argument should be Job Type which can be 0,1, or 2
#  Third argument is an output argument which recieves status of Procedure Call
#############################################################################################*/
CALL &&ENV_METADATA.RECYCLE_HANDLING('EV_HP9_10504601',1,P_RESULT);

.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;
 

/*#############################################################################################
# LOGGING Success Entry in STEPS_LOG table
#############################################################################################*/
CALL &&ENV_METADATA.END_STEP('EV_HP9_10504601','&&WORKFLOW_NAME',(SELECT EXECUTION_SESSION_ID FROM MAX_EXECUTION_SESSION_ID),'SOURCE_TABLE_NAME',0,0,0,0,0,'LDR');
	
.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;
	


/*#############################################################################################
#  Closing the BTEQ Script, Session Closed and Logging off
#############################################################################################*/
  
.LOGOFF
.QUIT