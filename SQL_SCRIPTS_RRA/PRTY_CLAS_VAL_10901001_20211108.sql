/* #############################################################################################
#  File-name   	: PRTY_CLAS_VAL_10901001.sql
#  Part of     	: FSDM
#  Type        	: BTEQ - LDR
#  Job         	: LOAD PRTY_CLAS_VAL
#  -------------------------------------------------------------------------
#  Purpose     	: The purpose of this file is to load data into the PRTY_CLAS_VAL
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
CALL &&ENV_METADATA.INFA_SESSION_INSRT('&&WORKFLOW_NAME','PRTY_CLAS_VAL_10901001');

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
WHERE B.SESSION_NAME = 'PRTY_CLAS_VAL_10901001';

/* #############################################################################################
# LOGGING Start Entry in STEPS_LOG table
 #############################################################################################*/

CALL &&ENV_METADATA.START_STEP('PRTY_CLAS_VAL_10901001','&&WORKFLOW_NAME',(SELECT EXECUTION_SESSION_ID FROM MAX_EXECUTION_SESSION_ID));

BT;

/* #############################################################################################
# TRUNCATING LDR TABLE
 #############################################################################################*/

DELETE &&ENV_LDR.PRTY_CLAS_VAL_10901001 ALL;

/* #############################################################################################
# LOADING INTO LDR TABLE 
 #############################################################################################*/


INSERT INTO &&ENV_LDR.PRTY_CLAS_VAL_10901001
(PRTY_CLAS_CD,PRTY_CLAS_VAL_DESC_EN,PRTY_CLAS_VAL_DESC_AR,INST_ID,PRTY_CLAS_VAL_CD,PRTY_CLAS_VAL_STRT_DTTM,PRTY_CLAS_VAL_END_DTTM,PRTY_CLAS_VAL_NAME,RCORD_ID,REC_IND,INSRT_SESS_ID,UPDT_SESS_ID,RECYCLE_FLAG,RECYCLE_REASON,SRC_TAB_KEY,SRC_TAB_NAME,SRC_COL_NAME,RECYCLE_CDC_TIMESTAMP)

SELECT
	PRTY_CLAS_SCHM_TYPE.PRTY_CLAS_CD AS PRTY_CLAS_CD,HLP_ICSR1I02.DZ AS PRTY_CLAS_VAL_DESC_EN,HLP_ICSR1I02.DZ AS PRTY_CLAS_VAL_DESC_AR,INST_TYPE.INST_TYPE_CD AS INST_ID,"GENERATE SGK FOR 'UNKNOWN'
GENERATE SGK FOR UNIQUE VALUE OF TRIM(C_TIT_STU)" AS PRTY_CLAS_VAL_CD,CDC_TIMESTAMP AS PRTY_CLAS_VAL_STRT_DTTM,CAST ('9999-12-31 00:00:00' AS TIMESTAMP(0) ) AS PRTY_CLAS_VAL_END_DTTM,TRIM(C_TIT_STU) AS PRTY_CLAS_VAL_NAME
    ,10901001 AS RCORD_ID
	,'I' AS REC_IND
	,SESS.EXECUTION_SESSION_ID 	
	,SESS.EXECUTION_SESSION_ID
	,CASE WHEN (PRTY_CLAS_SCHM_TYPE.PRTY_CLAS_CD IS NULL OR HLP_ICSR1I02.DZ IS NULL OR HLP_ICSR1I02.DZ IS NULL OR INST_TYPE.INST_TYPE_CD IS NULL) THEN 'Y' ELSE 'N' END AS RECYCLE_FLAG,                  
	(CASE WHEN PRTY_CLAS_SCHM_TYPE.PRTY_CLAS_CD  IS NULL THEN 'PRTY_CLAS_CD MISSED' ELSE 'PRTY_CLAS_CD MATCHED' END||'-'||CASE WHEN HLP_ICSR1I02.DZ  IS NULL THEN 'PRTY_CLAS_VAL_DESC_EN MISSED' ELSE 'PRTY_CLAS_VAL_DESC_EN MATCHED' END||'-'||CASE WHEN HLP_ICSR1I02.DZ  IS NULL THEN 'PRTY_CLAS_VAL_DESC_AR MISSED' ELSE 'PRTY_CLAS_VAL_DESC_AR MATCHED' END||'-'||CASE WHEN INST_TYPE.INST_TYPE_CD  IS NULL THEN 'INST_ID MISSED' ELSE 'INST_ID MATCHED' END) AS RECYCLE_REASON,
	SRC.SRC_TAB_KEY AS SRC_TAB_KEY,                   
	SRC.SRC_TAB_NAME AS SRC_TAB_NAME,                  
	SRC.SRC_COL_NAME AS SRC_COL_NAME,                  
	SRC.CDC_TIMESTAMP AS RECYCLE_CDC_TIMESTAMP

FROM
	(
		SELECT * FROM
		
		(
			SELECT CDC_TIMESTAMP,C_TIT_STU,ID_DZ,CDC_TIMESTAMP
			,'No Info in SMX' AS SRC_TAB_KEY,'RBS-KSA_ICSR1J32' AS SRC_TAB_NAME,	'CDC_TIMESTAMP,C_TIT_STU,ID_DZ,CDC_TIMESTAMP' AS SRC_COL_NAME
			FROM &&ENV_STAGE.RBS-KSA_ICSR1J32 SRC 
			WEHRE C_TIT_STU IS NOT NULL AND TRIM(C_TIT_STU)<> '' 
			
			UNION ALL
			
			SELECT CDC_TIMESTAMP,C_TIT_STU,ID_DZ,CDC_TIMESTAMP
			,'No Info in SMX' AS SRC_TAB_KEY,'RBS-KSA_ICSR1J32' AS SRC_TAB_NAME,	'CDC_TIMESTAMP,C_TIT_STU,ID_DZ,CDC_TIMESTAMP' AS SRC_COL_NAME
			FROM &&ENV_STAGE_RECYCLE.RBS-KSA_ICSR1J32_PRTY_CLAS_VAL_10901001 SRC 
			WEHRE C_TIT_STU IS NOT NULL AND TRIM(C_TIT_STU)<> '' 
			
		) AA
        
		QUALIFY ROW_NUMBER() OVER ( PARTITION BY C_TIT_STU ORDER BY CDC_TIMESTAMP DESC)=1
	) SRC

	LEFT JOIN &&PRD_FSDM.PRTY_CLAS_SCHM_TYPE PRTY_CLAS_SCHM_TYPE ON PRTY_CLAS_NAME='EDUCATIONAL DEGREE'
LEFT JOIN &&PRD_HLP.HLP_ICSR1I02 HLP_ICSR1I02 ON HLP_ICSR1I02.ID_DZ=TRIM(SRC.ID_DZ) AND HLP_ICSR1I02.C_LNG='EN'
LEFT JOIN &&PRD_HLP.HLP_ICSR1I02 HLP_ICSR1I02 ON HLP_ICSR1I02.ID_DZ=TRIM(SRC.ID_DZ) AND HLP_ICSR1I02.C_LNG='AR'
LEFT JOIN &&PRD_FSDM.INST_TYPE INST_TYPE ON INST_TYPE_NAME='ALRAJHI BANK KSA'
	
    CROSS JOIN
	MAX_EXECUTION_SESSION_ID SESS;


ET;

.IF ERRORCODE = 0 THEN .GOTO SUCCESS_STEP


/*#############################################################################################
# Logging Failure Entry in STEPS_LOG table in case failure
#############################################################################################*/
.IF ERRORCODE <> 0 THEN CALL &&ENV_METADATA.FAIL_STEP('PRTY_CLAS_VAL_10901001','&&WORKFLOW_NAME',(SELECT EXECUTION_SESSION_ID FROM MAX_EXECUTION_SESSION_ID));
.QUIT ERRORCODE;


.LABEL SUCCESS_STEP

/*#############################################################################################
#  Recycle Handling PROCEDURE is invoked to move rows to Recycle Table.
#  Two Input paramters should be passed to RECYCLE_HANDLING SP
#  First input argument should be LDR table name like 'PRTY_CLAS_VAL_10901001'
#  Second argument should be Job Type which can be 0,1, or 2
#  Third argument is an output argument which recieves status of Procedure Call
#############################################################################################*/
CALL &&ENV_METADATA.RECYCLE_HANDLING('PRTY_CLAS_VAL_10901001',1,P_RESULT);

.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;
 

/*#############################################################################################
# LOGGING Success Entry in STEPS_LOG table
#############################################################################################*/
CALL &&ENV_METADATA.END_STEP('PRTY_CLAS_VAL_10901001','&&WORKFLOW_NAME',(SELECT EXECUTION_SESSION_ID FROM MAX_EXECUTION_SESSION_ID),'SOURCE_TABLE_NAME',0,0,0,0,0,'LDR');
	
.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;
	


/*#############################################################################################
#  Closing the BTEQ Script, Session Closed and Logging off
#############################################################################################*/
  
.LOGOFF
.QUIT