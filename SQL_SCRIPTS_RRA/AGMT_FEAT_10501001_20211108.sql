/* #############################################################################################
#  File-name   	: AGMT_FEAT_10501001.sql
#  Part of     	: FSDM
#  Type        	: BTEQ - LDR
#  Job         	: LOAD AGMT_FEAT
#  -------------------------------------------------------------------------
#  Purpose     	: The purpose of this file is to load data into the AGMT_FEAT
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
CALL &&ENV_METADATA.INFA_SESSION_INSRT('&&WORKFLOW_NAME','AGMT_FEAT_10501001');

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
WHERE B.SESSION_NAME = 'AGMT_FEAT_10501001';

/* #############################################################################################
# LOGGING Start Entry in STEPS_LOG table
 #############################################################################################*/

CALL &&ENV_METADATA.START_STEP('AGMT_FEAT_10501001','&&WORKFLOW_NAME',(SELECT EXECUTION_SESSION_ID FROM MAX_EXECUTION_SESSION_ID));

BT;

/* #############################################################################################
# TRUNCATING LDR TABLE
 #############################################################################################*/

DELETE &&ENV_LDR.AGMT_FEAT_10501001 ALL;

/* #############################################################################################
# LOADING INTO LDR TABLE 
 #############################################################################################*/


INSERT INTO &&ENV_LDR.AGMT_FEAT_10501001
(AGMT_ID,FEAT_ID,AGMT_FEAT_ROLE_CD,INST_ID,AGMT_FEAT_STRT_DTTM,AGMT_FEAT_END_DTTM,AGMT_FEAT_AMT,AGMT_FEAT_RATE,AGMT_FEAT_QTY,AGMT_FEAT_NUM,AGMT_FEAT_DT,RCORD_ID,REC_IND,INSRT_SESS_ID,UPDT_SESS_ID,RECYCLE_FLAG,RECYCLE_REASON,SRC_TAB_KEY,SRC_TAB_NAME,SRC_COL_NAME,RECYCLE_CDC_TIMESTAMP)

SELECT
	SGK_AGMT.AGMT_ID AS AGMT_ID,FEAT.FEAT_ID AS FEAT_ID,.AGMT_FEAT_ROLE_CD AS AGMT_FEAT_ROLE_CD,INST_TYPE.INST_TYPE_CD AS INST_ID,COALESCE(DT_APER,CDC_TIMESTAMP) AS AGMT_FEAT_STRT_DTTM,CAST ('9999-12-31 00:00:00' AS TIMESTAMP(0) ) AS AGMT_FEAT_END_DTTM,NULL AS AGMT_FEAT_AMT,NULL AS AGMT_FEAT_RATE,NULL AS AGMT_FEAT_QTY,TRIM(NU_MM_DUR_PRES) AS AGMT_FEAT_NUM,NULL AS AGMT_FEAT_DT
    ,10501001 AS RCORD_ID
	,'I' AS REC_IND
	,SESS.EXECUTION_SESSION_ID 	
	,SESS.EXECUTION_SESSION_ID
	,CASE WHEN (SGK_AGMT.AGMT_ID IS NULL OR FEAT.FEAT_ID IS NULL OR .AGMT_FEAT_ROLE_CD IS NULL OR INST_TYPE.INST_TYPE_CD IS NULL) THEN 'Y' ELSE 'N' END AS RECYCLE_FLAG,                  
	(CASE WHEN SGK_AGMT.AGMT_ID  IS NULL THEN 'AGMT_ID MISSED' ELSE 'AGMT_ID MATCHED' END||'-'||CASE WHEN FEAT.FEAT_ID  IS NULL THEN 'FEAT_ID MISSED' ELSE 'FEAT_ID MATCHED' END||'-'||CASE WHEN .AGMT_FEAT_ROLE_CD  IS NULL THEN 'AGMT_FEAT_ROLE_CD MISSED' ELSE 'AGMT_FEAT_ROLE_CD MATCHED' END||'-'||CASE WHEN INST_TYPE.INST_TYPE_CD  IS NULL THEN 'INST_ID MISSED' ELSE 'INST_ID MATCHED' END) AS RECYCLE_REASON,
	SRC.SRC_TAB_KEY AS SRC_TAB_KEY,                   
	SRC.SRC_TAB_NAME AS SRC_TAB_NAME,                  
	SRC.SRC_COL_NAME AS SRC_COL_NAME,                  
	SRC.CDC_TIMESTAMP AS RECYCLE_CDC_TIMESTAMP

FROM
	(
		SELECT * FROM
		
		(
			SELECT NU_RAPP,DT_APER,CDC_TIMESTAMP,NU_MM_DUR_PRES,CDC_TIMESTAMP
			,'No Info in SMX' AS SRC_TAB_KEY,'CTF-KSA_MDTB051' AS SRC_TAB_NAME,	'NU_RAPP,DT_APER,CDC_TIMESTAMP,NU_MM_DUR_PRES,CDC_TIMESTAMP' AS SRC_COL_NAME
			FROM &&ENV_STAGE.CTF-KSA_MDTB051 SRC 
			WEHRE NU_RAPP IS NOT NULL AND TRIM(NU_RAPP)<> '''
			
			UNION ALL
			
			SELECT NU_RAPP,DT_APER,CDC_TIMESTAMP,NU_MM_DUR_PRES,CDC_TIMESTAMP
			,'No Info in SMX' AS SRC_TAB_KEY,'CTF-KSA_MDTB051' AS SRC_TAB_NAME,	'NU_RAPP,DT_APER,CDC_TIMESTAMP,NU_MM_DUR_PRES,CDC_TIMESTAMP' AS SRC_COL_NAME
			FROM &&ENV_STAGE_RECYCLE.CTF-KSA_MDTB051_AGMT_FEAT_10501001 SRC 
			WEHRE NU_RAPP IS NOT NULL AND TRIM(NU_RAPP)<> '''
			
		) AA
        
		QUALIFY ROW_NUMBER() OVER ( PARTITION BY  NU_RAPP,NU_MM_DUR_PRES,CDC_TIMESTAMP ORDER BY CDC_TIMESTAMP DESC)=1
	) SRC

	LEFT JOIN &&PRD_HLP.SGK_AGMT SGK_AGMT ON SGK_AGMT.NAT_KEY=TRIM(SRC.NU_RAPP)
AND SGK_AGMT.TYPE_CD='RETAIL LOAN ACCOUNT' 
AND SGK_AGMT.INST_ID=(SELECT INST_TYPE_CD FROM INST_TYPE WHERE INST_TYPE_NAME='ALRAJHI BANK KSA')
LEFT JOIN &&PRD_FSDM.FEAT FEAT ON TRIM(COALESCE(SRC.NU_MM_DUR_PRES),'UNKNOWN')=FEAT_NAME AND FEAT_CLASFCN_CD=(SELECT FEAT_CLASFCN_CD FROM FEAT_CLASFCN_TYPE WHERE FEAT_CLASFCN_NAME='LOAN DURATION MONTHS')
LEFT JOIN &&PRD_FSDM.  ON AGMT_FEAT_ROLE_NAME='RETAIL LOAN ACCOUNT LOAN DURATION MONTHS'
LEFT JOIN &&PRD_FSDM.INST_TYPE INST_TYPE ON INST_TYPE_NAME='ALRAJHI BANK KSA'
	
    CROSS JOIN
	MAX_EXECUTION_SESSION_ID SESS;


ET;

.IF ERRORCODE = 0 THEN .GOTO SUCCESS_STEP


/*#############################################################################################
# Logging Failure Entry in STEPS_LOG table in case failure
#############################################################################################*/
.IF ERRORCODE <> 0 THEN CALL &&ENV_METADATA.FAIL_STEP('AGMT_FEAT_10501001','&&WORKFLOW_NAME',(SELECT EXECUTION_SESSION_ID FROM MAX_EXECUTION_SESSION_ID));
.QUIT ERRORCODE;


.LABEL SUCCESS_STEP

/*#############################################################################################
#  Recycle Handling PROCEDURE is invoked to move rows to Recycle Table.
#  Two Input paramters should be passed to RECYCLE_HANDLING SP
#  First input argument should be LDR table name like 'AGMT_FEAT_10501001'
#  Second argument should be Job Type which can be 0,1, or 2
#  Third argument is an output argument which recieves status of Procedure Call
#############################################################################################*/
CALL &&ENV_METADATA.RECYCLE_HANDLING('AGMT_FEAT_10501001',1,P_RESULT);

.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;
 

/*#############################################################################################
# LOGGING Success Entry in STEPS_LOG table
#############################################################################################*/
CALL &&ENV_METADATA.END_STEP('AGMT_FEAT_10501001','&&WORKFLOW_NAME',(SELECT EXECUTION_SESSION_ID FROM MAX_EXECUTION_SESSION_ID),'SOURCE_TABLE_NAME',0,0,0,0,0,'LDR');
	
.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;
	


/*#############################################################################################
#  Closing the BTEQ Script, Session Closed and Logging off
#############################################################################################*/
  
.LOGOFF
.QUIT