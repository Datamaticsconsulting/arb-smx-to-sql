/* #############################################################################################
#  File-name   	: MRTL_STS_TYPE_10504301.sql
#  Part of     	: FSDM
#  Type        	: BTEQ - LDR
#  Job         	: LOAD MRTL_STS_TYPE
#  -------------------------------------------------------------------------
#  Purpose     	: The purpose of this file is to load data into the MRTL_STS_TYPE
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
CALL &&ENV_METADATA.INFA_SESSION_INSRT('&&WORKFLOW_NAME','MRTL_STS_TYPE_10504301');

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
WHERE B.SESSION_NAME = 'MRTL_STS_TYPE_10504301';

/* #############################################################################################
# LOGGING Start Entry in STEPS_LOG table
 #############################################################################################*/

CALL &&ENV_METADATA.START_STEP('MRTL_STS_TYPE_10504301','&&WORKFLOW_NAME',(SELECT EXECUTION_SESSION_ID FROM MAX_EXECUTION_SESSION_ID));

BT;

/* #############################################################################################
# TRUNCATING LDR TABLE
 #############################################################################################*/

DELETE &&ENV_LDR.MRTL_STS_TYPE_10504301 ALL;

/* #############################################################################################
# LOADING INTO LDR TABLE 
 #############################################################################################*/


INSERT INTO &&ENV_LDR.MRTL_STS_TYPE_10504301
(INST_ID,MRTL_STS_CD,MRTL_STS_NAME,MRTL_STS_DESC,RCORD_ID,REC_IND,INSRT_SESS_ID,UPDT_SESS_ID)

SELECT
	INST_TYPE.INST_TYPE_CD AS INST_ID,"GENERATE SGK FOR 'UNKNOWN'
GENERATE SGK BASED ON UNIQUE VALUE OF TRIM(C_ST_CIV_APPL )" AS MRTL_STS_CD,TRIM(C_ST_CIV_APPL ) AS MRTL_STS_NAME,TRIM(C_ST_CIV_APPL ) AS MRTL_STS_DESC
    ,10504301 AS RCORD_ID
	,'I' AS REC_IND
	,SESS.EXECUTION_SESSION_ID 	
	,SESS.EXECUTION_SESSION_ID

FROM
	(
		SELECT * FROM
		
		(
			SELECT C_ST_CIV_APPL 
			,'No Info in SMX' AS SRC_TAB_KEY,'CTF-KSA_ICTBB05' AS SRC_TAB_NAME,	'C_ST_CIV_APPL ' AS SRC_COL_NAME
			FROM &&ENV_STAGE.CTF-KSA_ICTBB05 SRC 
			
			
		) AA
        
		QUALIFY ROW_NUMBER() OVER (C_ST_CIV_APPL  IS NOT NULL AND TRIM(C_ST_CIV_APPL )<> '' \
QUALIFY OVER PARTITION BY C_ST_CIV_APPL  AND ORDER BY CDC_TIMESTAMP)=1
	) SRC

	LEFT JOIN &&PRD_FSDM.INST_TYPE INST_TYPE ON INST_TYPE_NAME='ALRAJHI BANK KSA'
	
    CROSS JOIN
	MAX_EXECUTION_SESSION_ID SESS;


ET;

.IF ERRORCODE = 0 THEN .GOTO SUCCESS_STEP


/*#############################################################################################
# Logging Failure Entry in STEPS_LOG table in case failure
#############################################################################################*/
.IF ERRORCODE <> 0 THEN CALL &&ENV_METADATA.FAIL_STEP('MRTL_STS_TYPE_10504301','&&WORKFLOW_NAME',(SELECT EXECUTION_SESSION_ID FROM MAX_EXECUTION_SESSION_ID));
.QUIT ERRORCODE;


.LABEL SUCCESS_STEP

/*#############################################################################################
#  Recycle Handling PROCEDURE is invoked to move rows to Recycle Table.
#  Two Input paramters should be passed to RECYCLE_HANDLING SP
#  First input argument should be LDR table name like 'MRTL_STS_TYPE_10504301'
#  Second argument should be Job Type which can be 0,1, or 2
#  Third argument is an output argument which recieves status of Procedure Call
#############################################################################################*/
CALL &&ENV_METADATA.RECYCLE_HANDLING('MRTL_STS_TYPE_10504301',1,P_RESULT);

.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;
 

/*#############################################################################################
# LOGGING Success Entry in STEPS_LOG table
#############################################################################################*/
CALL &&ENV_METADATA.END_STEP('MRTL_STS_TYPE_10504301','&&WORKFLOW_NAME',(SELECT EXECUTION_SESSION_ID FROM MAX_EXECUTION_SESSION_ID),'SOURCE_TABLE_NAME',0,0,0,0,0,'LDR');
	
.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;
	


/*#############################################################################################
#  Closing the BTEQ Script, Session Closed and Logging off
#############################################################################################*/
  
.LOGOFF
.QUIT