/* #############################################################################################
#  File-name   	: PRTY_11111111.sql
#  Part of     	: FSDM
#  Type        	: BTEQ - LDR
#  Job         	: LOAD PRTY
#  -------------------------------------------------------------------------
#  Purpose     	: The purpose of this file is to load data into the PRTY
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
CALL &&ENV_METADATA.INFA_SESSION_INSRT('&&WORKFLOW_NAME','PRTY_11111111');

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
WHERE B.SESSION_NAME = 'PRTY_11111111';

/* #############################################################################################
# LOGGING Start Entry in STEPS_LOG table
 #############################################################################################*/

CALL &&ENV_METADATA.START_STEP('PRTY_11111111','&&WORKFLOW_NAME',(SELECT EXECUTION_SESSION_ID FROM MAX_EXECUTION_SESSION_ID));

BT;

/* #############################################################################################
# TRUNCATING LDR TABLE
 #############################################################################################*/

DELETE &&ENV_LDR.PRTY_11111111 ALL;

/* #############################################################################################
# LOADING INTO LDR TABLE 
 #############################################################################################*/


INSERT INTO &&ENV_LDR.PRTY_11111111
(PRTY_ID,PRTY_SBTYPE_CD,PRTY_TYPE_CD,INST_ID,PRTY_DESC,RCORD_ID,REC_IND,INSRT_SESS_ID,UPDT_SESS_ID,RECYCLE_FLAG,RECYCLE_REASON,SRC_TAB_KEY,SRC_TAB_NAME,SRC_COL_NAME,RECYCLE_CDC_TIMESTAMP)

SELECT
	SGK_PRTY.PRTY_ID AS PRTY_ID,PRTY_SBTYPE.PRTY_SBTYPE_CD AS PRTY_SBTYPE_CD,PRTY_TYPE.PRTY_TYPE_CD AS PRTY_TYPE_CD,INST_TYPE.INST_TYPE_CD AS INST_ID,NULL AS PRTY_DESC
    ,11111111 AS RCORD_ID
	,'I' AS REC_IND
	,SESS.EXECUTION_SESSION_ID 	
	,SESS.EXECUTION_SESSION_ID
	,CASE WHEN (SGK_PRTY.PRTY_ID IS NULL OR PRTY_SBTYPE.PRTY_SBTYPE_CD IS NULL OR PRTY_TYPE.PRTY_TYPE_CD IS NULL OR INST_TYPE.INST_TYPE_CD IS NULL) THEN 'Y' ELSE 'N' END AS RECYCLE_FLAG,                  
	(CASE WHEN SGK_PRTY.PRTY_ID  IS NULL THEN 'PRTY_ID MISSED' ELSE 'PRTY_ID MATCHED' END||'-'||CASE WHEN PRTY_SBTYPE.PRTY_SBTYPE_CD  IS NULL THEN 'PRTY_SBTYPE_CD MISSED' ELSE 'PRTY_SBTYPE_CD MATCHED' END||'-'||CASE WHEN PRTY_TYPE.PRTY_TYPE_CD  IS NULL THEN 'PRTY_TYPE_CD MISSED' ELSE 'PRTY_TYPE_CD MATCHED' END||'-'||CASE WHEN INST_TYPE.INST_TYPE_CD  IS NULL THEN 'INST_ID MISSED' ELSE 'INST_ID MATCHED' END) AS RECYCLE_REASON,
	SRC.SRC_TAB_KEY AS SRC_TAB_KEY,                   
	SRC.SRC_TAB_NAME AS SRC_TAB_NAME,                  
	SRC.SRC_COL_NAME AS SRC_COL_NAME,                  
	SRC.CDC_TIMESTAMP AS RECYCLE_CDC_TIMESTAMP

FROM
	(
		SELECT * FROM
		
		(
			SELECT 
			,'No Info in SMX' AS SRC_TAB_KEY,'STATIC_' AS SRC_TAB_NAME,	'' AS SRC_COL_NAME
			FROM &&ENV_STAGE.STATIC_ SRC 
			
			
			UNION ALL
			
			SELECT 
			,'No Info in SMX' AS SRC_TAB_KEY,'STATIC_' AS SRC_TAB_NAME,	'' AS SRC_COL_NAME
			FROM &&ENV_STAGE_RECYCLE.STATIC__PRTY_11111111 SRC 
			
			
		) AA
        
		QUALIFY ROW_NUMBER() OVER (NOT REQUIRED)=1
	) SRC

	LEFT JOIN &&PRD_HLP.SGK_PRTY SGK_PRTY ON SGK_PRTY.NAT_KEY='SIMAH'
AND SGK_PRTY.TYPE_CD= 'CREDIT RATING AGENCY' 
AND INST_ID=(SELECT INST_TYPE_CD FROM INST_TYPE WHERE INST_TYPE_NAME='ALRAJHI BANK KSA') 
LEFT JOIN &&PRD_FSDM.PRTY_SBTYPE PRTY_SBTYPE ON PRTY_SBTYPE.PRTY_SBTYPE_NAME= 'ORGANIZATION'
LEFT JOIN &&PRD_FSDM.PRTY_TYPE PRTY_TYPE ON PRTY_TYPE_NAME='CREDIT RATING AGENCY' 
LEFT JOIN &&PRD_FSDM.INST_TYPE INST_TYPE ON INST_TYPE_NAME='ALRAJHI BANK KSA'
	
    CROSS JOIN
	MAX_EXECUTION_SESSION_ID SESS;


ET;

.IF ERRORCODE = 0 THEN .GOTO SUCCESS_STEP


/*#############################################################################################
# Logging Failure Entry in STEPS_LOG table in case failure
#############################################################################################*/
.IF ERRORCODE <> 0 THEN CALL &&ENV_METADATA.FAIL_STEP('PRTY_11111111','&&WORKFLOW_NAME',(SELECT EXECUTION_SESSION_ID FROM MAX_EXECUTION_SESSION_ID));
.QUIT ERRORCODE;


.LABEL SUCCESS_STEP

/*#############################################################################################
#  Recycle Handling PROCEDURE is invoked to move rows to Recycle Table.
#  Two Input paramters should be passed to RECYCLE_HANDLING SP
#  First input argument should be LDR table name like 'PRTY_11111111'
#  Second argument should be Job Type which can be 0,1, or 2
#  Third argument is an output argument which recieves status of Procedure Call
#############################################################################################*/
CALL &&ENV_METADATA.RECYCLE_HANDLING('PRTY_11111111',1,P_RESULT);

.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;
 

/*#############################################################################################
# LOGGING Success Entry in STEPS_LOG table
#############################################################################################*/
CALL &&ENV_METADATA.END_STEP('PRTY_11111111','&&WORKFLOW_NAME',(SELECT EXECUTION_SESSION_ID FROM MAX_EXECUTION_SESSION_ID),'SOURCE_TABLE_NAME',0,0,0,0,0,'LDR');
	
.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;
	


/*#############################################################################################
#  Closing the BTEQ Script, Session Closed and Logging off
#############################################################################################*/
  
.LOGOFF
.QUIT