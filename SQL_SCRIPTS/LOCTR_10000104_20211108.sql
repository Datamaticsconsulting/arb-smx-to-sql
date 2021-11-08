/* #############################################################################################
#  File-name   	: LOCTR_10000104.sql
#  Part of     	: FSDM
#  Type        	: BTEQ - LDR
#  Job         	: LOAD LOCTR
#  -------------------------------------------------------------------------
#  Purpose     	: The purpose of this file is to load data into the LOCTR
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
CALL &&ENV_METADATA.INFA_SESSION_INSRT('&&WORKFLOW_NAME','LOCTR_10000104');

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
WHERE B.SESSION_NAME = 'LOCTR_10000104';

/* #############################################################################################
# LOGGING Start Entry in STEPS_LOG table
 #############################################################################################*/

CALL &&ENV_METADATA.START_STEP('LOCTR_10000104','&&WORKFLOW_NAME',(SELECT EXECUTION_SESSION_ID FROM MAX_EXECUTION_SESSION_ID));

BT;

/* #############################################################################################
# TRUNCATING LDR TABLE
 #############################################################################################*/

DELETE &&ENV_LDR.LOCTR_10000104 ALL;

/* #############################################################################################
# LOADING INTO LDR TABLE 
 #############################################################################################*/


INSERT INTO &&ENV_LDR.LOCTR_10000104
(INST_ID,CLCTN_STS_TYPE_CD,CLCTN_STS_TYPE_NAME,CLCTN_STS_TYPE_DESC,ACCS_DVC_ID,ACCS_DVC_SBTYPE_CD,INST_ID,ACCS_DVC_VAL,PRTY_ID,LOCTR_ID,LOCTR_USGE_TYPE_CD,INST_ID,PRTY_LOCTR_STRT_DTTM,PRTY_LOCTR_END_DTTM,LOCTR_ID,LOCTR_SBTYPE_CD,INST_ID,HOST_LOCTR_NUM,RCORD_ID,REC_IND,INSRT_SESS_ID,UPDT_SESS_ID)

SELECT
	INST_TYPE.INST_TYPE_CD AS INST_ID,"GENERATE SGK FOR 'UNKNOWN'
GENERATE SGK BASED ON UNIQUE VALUE OF TRIM(DELINQ_DAYS)" AS CLCTN_STS_TYPE_CD,DELINQ_DAYS AS CLCTN_STS_TYPE_NAME, (CASE 
WHEN (DELINQ_DAYS)  < 0 THEN 'CURRENT'
WHEN (DELINQ_DAYS) BETWEEN  1 AND 30 THEN '1-30 DPD'
WHEN (DELINQ_DAYS) >  30 THEN '30+ DPD'
WHEN (DELINQ_DAYS) >  90 THEN '90+ DPD'
) ELSE 'UNKNOWN' AS CLCTN_STS_TYPE_DESC,SGK_ACCS_DVC.ACCS_DVC_ID AS ACCS_DVC_ID,ACCS_DVC_SBTYPE.ACCS_DVC_SBTYPE_CD AS ACCS_DVC_SBTYPE_CD,INST_TYPE.INST_TYPE_CD AS INST_ID,TRIM(SERNO) AS ACCS_DVC_VAL,SGK_PRTY.PRTY_ID AS PRTY_ID,SGK_LOCTR.LOCTR_ID AS LOCTR_ID,LOCTR_USGE_TYPE.LOCTR_USGE_CD AS LOCTR_USGE_TYPE_CD,INST_TYPE.INST_TYPE_CD AS INST_ID,COALESCE(DATA_CENS,CDC_TIMESTAMP) AS PRTY_LOCTR_STRT_DTTM,CAST ('9999-12-31 00:00:00' AS TIMESTAMP(0) ) AS PRTY_LOCTR_END_DTTM,SGK_LOCTR.LOCTR_ID AS LOCTR_ID,LOCTR_SBTYPE.LOCTR_SBTYPE_CD AS LOCTR_SBTYPE_CD,INST_TYPE.INST_TYPE_CD AS INST_ID
    ,10000104 AS RCORD_ID
	,'I' AS REC_IND
	,SESS.EXECUTION_SESSION_ID 	
	,SESS.EXECUTION_SESSION_ID

FROM
	(
		SELECT * FROM
		
		(
			SELECT N_CIV, VIA,CDC_TIMESTAMP
			,'No Info in SMX' AS SRC_TAB_KEY,'CIF-KSA_SR00W200' AS SRC_TAB_NAME,	'N_CIV, VIA,CDC_TIMESTAMP' AS SRC_COL_NAME
			FROM &&ENV_STAGE.CIF-KSA_SR00W200 SRC 
			WEHRE N_CIV IS NOT NULL AND TRIM(N_CIV)<> '' OR VIA IS NOT NULL AND TRIM(VIA)<> '' 
			
		) AA
        
		QUALIFY ROW_NUMBER() OVER ( PARTITION BY N_CIV,VIA ORDER BY CDC_TIMESTAMP DESC)=1
	) SRC

	LEFT JOIN &&PRD_FSDM.INST_TYPE INST_TYPE ON INST_TYPE_NAME='ALRAJHI BANK KSA'
LEFT JOIN &&PRD_HLP.SGK_ACCS_DVC SGK_ACCS_DVC ON SGK_ACCS_DVC.NAT_KEY=TRIM(SRC.SERNO)  
AND SGK_ACCS_DVC.TYPE_CD='PRIME BACKOFFICE CARD'
AND SGK_ACCS_DVC.INST_ID=(SELECT INST_TYPE_CD FROM INST_TYPE WHERE INST_TYPE_NAME='ALRAJHI BANK KSA')
LEFT JOIN &&PRD_FSDM.ACCS_DVC_SBTYPE ACCS_DVC_SBTYPE ON ACCS_DVC_SBTYPE_NAME='CARD'
LEFT JOIN &&PRD_FSDM.INST_TYPE INST_TYPE ON INST_TYPE_NAME='ALRAJHI BANK KSA'
LEFT JOIN &&PRD_HLP.SGK_PRTY SGK_PRTY ON SGK_PRTY.NAT_KEY=TRIM(SRC.COD_CLIENTE) 
AND SGK_PRTY.TYPE_CD = 'CUSTOMER' 
AND INST_ID=(SELECT INST_TYPE_CD FROM INST_TYPE WHERE INST_TYPE_NAME='ALRAJHI BANK KSA') 
LEFT JOIN &&PRD_HLP.SGK_LOCTR SGK_LOCTR ON SGK_LOCTR.NAT_KEY=TRIM(COALESCE(SRC.COMUNE),'UNKNOWN') 
AND SGK_LOCTR.TYPE_CD = 'CITY' 
AND INST_ID=(SELECT INST_TYPE_CD FROM INST_TYPE WHERE INST_TYPE_NAME='ALRAJHI BANK KSA') 
LEFT JOIN &&PRD_FSDM.LOCTR_USGE_TYPE LOCTR_USGE_TYPE ON LOCTR_USGE_NAME='CUSTOMER CITY'
LEFT JOIN &&PRD_FSDM.INST_TYPE INST_TYPE ON INST_TYPE_NAME='ALRAJHI BANK KSA'
LEFT JOIN &&PRD_HLP.SGK_LOCTR SGK_LOCTR ON SGK_LOCTR.NAT_KEY=TRIM(COALESCE(VIA),'')||'-'||TRIM(COALESCE(N_CIV),'') 
AND SGK_LOCTR.TYPE_CD=(SELECT LOCTR_SBTYPE_CD FROM LOCTR_SBTYPE WHERE LOCTR_SBTYPE_NAME='STREET ADDRESS') 
AND SGK_LOCTR.INST_ID=(SELECT INST_TYPE_CD FROM INST_TYPE WHERE INST_TYPE_NAME='ALRAJHI BANK KSA')
LEFT JOIN &&PRD_FSDM.LOCTR_SBTYPE LOCTR_SBTYPE ON LOCTR_SBTYPE_NAME='ADDRESS'
LEFT JOIN &&PRD_FSDM.INST_TYPE INST_TYPE ON INST_TYPE_NAME='ALRAJHI BANK KSA'
	
    CROSS JOIN
	MAX_EXECUTION_SESSION_ID SESS;


ET;

.IF ERRORCODE = 0 THEN .GOTO SUCCESS_STEP


/*#############################################################################################
# Logging Failure Entry in STEPS_LOG table in case failure
#############################################################################################*/
.IF ERRORCODE <> 0 THEN CALL &&ENV_METADATA.FAIL_STEP('LOCTR_10000104','&&WORKFLOW_NAME',(SELECT EXECUTION_SESSION_ID FROM MAX_EXECUTION_SESSION_ID));
.QUIT ERRORCODE;


.LABEL SUCCESS_STEP

/*#############################################################################################
#  Recycle Handling PROCEDURE is invoked to move rows to Recycle Table.
#  Two Input paramters should be passed to RECYCLE_HANDLING SP
#  First input argument should be LDR table name like 'LOCTR_10000104'
#  Second argument should be Job Type which can be 0,1, or 2
#  Third argument is an output argument which recieves status of Procedure Call
#############################################################################################*/
CALL &&ENV_METADATA.RECYCLE_HANDLING('LOCTR_10000104',1,P_RESULT);

.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;
 

/*#############################################################################################
# LOGGING Success Entry in STEPS_LOG table
#############################################################################################*/
CALL &&ENV_METADATA.END_STEP('LOCTR_10000104','&&WORKFLOW_NAME',(SELECT EXECUTION_SESSION_ID FROM MAX_EXECUTION_SESSION_ID),'SOURCE_TABLE_NAME',0,0,0,0,0,'LDR');
	
.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;
	


/*#############################################################################################
#  Closing the BTEQ Script, Session Closed and Logging off
#############################################################################################*/
  
.LOGOFF
.QUIT