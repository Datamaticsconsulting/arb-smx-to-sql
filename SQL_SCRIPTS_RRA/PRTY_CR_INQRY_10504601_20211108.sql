/* #############################################################################################
#  File-name   	: PRTY_CR_INQRY_10504601.sql
#  Part of     	: FSDM
#  Type        	: BTEQ - LDR
#  Job         	: LOAD PRTY_CR_INQRY
#  -------------------------------------------------------------------------
#  Purpose     	: The purpose of this file is to load data into the PRTY_CR_INQRY
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
CALL &&ENV_METADATA.INFA_SESSION_INSRT('&&WORKFLOW_NAME','PRTY_CR_INQRY_10504601');

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
WHERE B.SESSION_NAME = 'PRTY_CR_INQRY_10504601';

/* #############################################################################################
# LOGGING Start Entry in STEPS_LOG table
 #############################################################################################*/

CALL &&ENV_METADATA.START_STEP('PRTY_CR_INQRY_10504601','&&WORKFLOW_NAME',(SELECT EXECUTION_SESSION_ID FROM MAX_EXECUTION_SESSION_ID));

BT;

/* #############################################################################################
# TRUNCATING LDR TABLE
 #############################################################################################*/

DELETE &&ENV_LDR.PRTY_CR_INQRY_10504601 ALL;

/* #############################################################################################
# LOADING INTO LDR TABLE 
 #############################################################################################*/


INSERT INTO &&ENV_LDR.PRTY_CR_INQRY_10504601
(OBLGR_PRTY_ID,INQRY_PRTY_ID,CR_INQRY_CURY_CD,RQSTG_EV_ID,INST_ID,CR_INQRY_DTTM,CR_INQRY_CR_AMT,RCORD_ID,REC_IND,INSRT_SESS_ID,UPDT_SESS_ID,RECYCLE_FLAG,RECYCLE_REASON,SRC_TAB_KEY,SRC_TAB_NAME,SRC_COL_NAME,RECYCLE_CDC_TIMESTAMP)

SELECT
	SGK_PRTY.PRTY_ID AS OBLGR_PRTY_ID,SGK_PRTY.PRTY_ID AS INQRY_PRTY_ID,HLP_REF_TYPE.VAL AS CR_INQRY_CURY_CD,SGK_EV.EV_ID AS RQSTG_EV_ID,INST_TYPE.INST_TYPE_CD AS INST_ID,DT_RQS AS CR_INQRY_DTTM,AMOUNT AS CR_INQRY_CR_AMT
    ,10504601 AS RCORD_ID
	,'I' AS REC_IND
	,SESS.EXECUTION_SESSION_ID 	
	,SESS.EXECUTION_SESSION_ID
	,CASE WHEN (SGK_PRTY.PRTY_ID IS NULL OR SGK_PRTY.PRTY_ID IS NULL OR HLP_REF_TYPE.VAL IS NULL OR SGK_EV.EV_ID IS NULL OR INST_TYPE.INST_TYPE_CD IS NULL) THEN 'Y' ELSE 'N' END AS RECYCLE_FLAG,                  
	(CASE WHEN SGK_PRTY.PRTY_ID  IS NULL THEN 'OBLGR_PRTY_ID MISSED' ELSE 'OBLGR_PRTY_ID MATCHED' END||'-'||CASE WHEN SGK_PRTY.PRTY_ID  IS NULL THEN 'INQRY_PRTY_ID MISSED' ELSE 'INQRY_PRTY_ID MATCHED' END||'-'||CASE WHEN HLP_REF_TYPE.VAL  IS NULL THEN 'CR_INQRY_CURY_CD MISSED' ELSE 'CR_INQRY_CURY_CD MATCHED' END||'-'||CASE WHEN SGK_EV.EV_ID  IS NULL THEN 'RQSTG_EV_ID MISSED' ELSE 'RQSTG_EV_ID MATCHED' END||'-'||CASE WHEN INST_TYPE.INST_TYPE_CD  IS NULL THEN 'INST_ID MISSED' ELSE 'INST_ID MATCHED' END) AS RECYCLE_REASON,
	SRC.SRC_TAB_KEY AS SRC_TAB_KEY,                   
	SRC.SRC_TAB_NAME AS SRC_TAB_NAME,                  
	SRC.SRC_COL_NAME AS SRC_COL_NAME,                  
	SRC.CDC_TIMESTAMP AS RECYCLE_CDC_TIMESTAMP

FROM
	(
		SELECT * FROM
		
		(
			SELECT PG_RQS,NU_PTL,DT_RQS,CD_MTC_UTE,AMOUNT,CD_DVS,CDC_TIMESTAMP
			,'No Info in SMX' AS SRC_TAB_KEY,'CTF-KSA_ICTBB01' AS SRC_TAB_NAME,	'PG_RQS,NU_PTL,DT_RQS,CD_MTC_UTE,AMOUNT,CD_DVS,CDC_TIMESTAMP' AS SRC_COL_NAME
			FROM &&ENV_STAGE.CTF-KSA_ICTBB01 SRC 
			WEHRE NU_PTL IS NOT NULL AND TRIM(NU_PTL)<> '' AND CD_MTC_UTE IS NOT NULL AND TRIM(CD_MTC_UTE)<> '' AND DT_RQS IS NOT NULL AND TRIM(DT_RQS)<> '' 
			
			UNION ALL
			
			SELECT PG_RQS,NU_PTL,DT_RQS,CD_MTC_UTE,AMOUNT,CD_DVS,CDC_TIMESTAMP
			,'No Info in SMX' AS SRC_TAB_KEY,'CTF-KSA_ICTBB01' AS SRC_TAB_NAME,	'PG_RQS,NU_PTL,DT_RQS,CD_MTC_UTE,AMOUNT,CD_DVS,CDC_TIMESTAMP' AS SRC_COL_NAME
			FROM &&ENV_STAGE_RECYCLE.CTF-KSA_ICTBB01_PRTY_CR_INQRY_10504601 SRC 
			WEHRE NU_PTL IS NOT NULL AND TRIM(NU_PTL)<> '' AND CD_MTC_UTE IS NOT NULL AND TRIM(CD_MTC_UTE)<> '' AND DT_RQS IS NOT NULL AND TRIM(DT_RQS)<> '' 
			
		) AA
        
		QUALIFY ROW_NUMBER() OVER ( PARTITION BY NU_PTL, CD_MTC_UTE, DT_RQS ORDER BY CDC_TIMESTAMP DESC)=1
	) SRC

	LEFT JOIN &&PRD_HLP.SGK_PRTY SGK_PRTY ON SGK_PRTY.NAT_KEY=(SELECT CD_NDG_FSC FROM HLP_ICTBF03 HLP WHERE HLP.NU_PTL = TRIM(SRC.NU_PTL) AND HLP.CD_TIP_LNK = '000' )
AND SGK_PRTY.TYPE_CD= 'CUSTOMER' 
AND INST_ID=(SELECT INST_TYPE_CD FROM INST_TYPE WHERE INST_TYPE_NAME='ALRAJHI BANK KSA') 
LEFT JOIN &&PRD_HLP.SGK_PRTY SGK_PRTY ON SGK_PRTY.NAT_KEY=TRIM(SRC.CD_MTC_UTE)  
AND SGK_PRTY.TYPE_CD ='USER'
AND SGK_PRTY.INST_ID=(SELECT INST_TYPE_CD FROM INST_TYPE WHERE INST_TYPE_NAME='ALRAJHI BANK KSA')
LEFT JOIN &&PRD_HLP.HLP_REF_TYPE HLP_REF_TYPE ON COALESCE(TRIM(SRC.CD_DVS),'UNKNOWN')=HLP.SRC_CD AND HLP.TYPE_NAME='CURRENCY ISO  CODE' AND HLP.SRC_IDNTFTN='CTF' AND INST_NAME='ARB KSA'
LEFT JOIN &&PRD_HLP.SGK_EV SGK_EV ON SGK_EV.NAT_KEY=TRIM(SRC.PG_RQS) AND SGK_EV.TYPE_CD='LOAN APPLICATION INQUIRY EVENT' 
AND SGK_EV.INST_ID=(SELECT INST_TYPE_CD FROM INST_TYPE WHERE INST_TYPE_NAME='ALRAJHI BANK KSA')
LEFT JOIN &&PRD_FSDM.INST_TYPE INST_TYPE ON INST_TYPE_NAME='ALRAJHI BANK KSA'
	
    CROSS JOIN
	MAX_EXECUTION_SESSION_ID SESS;


ET;

.IF ERRORCODE = 0 THEN .GOTO SUCCESS_STEP


/*#############################################################################################
# Logging Failure Entry in STEPS_LOG table in case failure
#############################################################################################*/
.IF ERRORCODE <> 0 THEN CALL &&ENV_METADATA.FAIL_STEP('PRTY_CR_INQRY_10504601','&&WORKFLOW_NAME',(SELECT EXECUTION_SESSION_ID FROM MAX_EXECUTION_SESSION_ID));
.QUIT ERRORCODE;


.LABEL SUCCESS_STEP

/*#############################################################################################
#  Recycle Handling PROCEDURE is invoked to move rows to Recycle Table.
#  Two Input paramters should be passed to RECYCLE_HANDLING SP
#  First input argument should be LDR table name like 'PRTY_CR_INQRY_10504601'
#  Second argument should be Job Type which can be 0,1, or 2
#  Third argument is an output argument which recieves status of Procedure Call
#############################################################################################*/
CALL &&ENV_METADATA.RECYCLE_HANDLING('PRTY_CR_INQRY_10504601',1,P_RESULT);

.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;
 

/*#############################################################################################
# LOGGING Success Entry in STEPS_LOG table
#############################################################################################*/
CALL &&ENV_METADATA.END_STEP('PRTY_CR_INQRY_10504601','&&WORKFLOW_NAME',(SELECT EXECUTION_SESSION_ID FROM MAX_EXECUTION_SESSION_ID),'SOURCE_TABLE_NAME',0,0,0,0,0,'LDR');
	
.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;
	


/*#############################################################################################
#  Closing the BTEQ Script, Session Closed and Logging off
#############################################################################################*/
  
.LOGOFF
.QUIT