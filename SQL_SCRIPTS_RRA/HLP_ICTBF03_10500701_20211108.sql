/* #############################################################################################
#  File-name   	: HLP_ICTBF03_10500701.sql
#  Part of     	: FSDM
#  Type        	: BTEQ - LDR
#  Job         	: LOAD HLP_ICTBF03
#  -------------------------------------------------------------------------
#  Purpose     	: The purpose of this file is to load data into the HLP_ICTBF03
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
CALL &&ENV_METADATA.INFA_SESSION_INSRT('&&WORKFLOW_NAME','HLP_ICTBF03_10500701');

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
WHERE B.SESSION_NAME = 'HLP_ICTBF03_10500701';

/* #############################################################################################
# LOGGING Start Entry in STEPS_LOG table
 #############################################################################################*/

CALL &&ENV_METADATA.START_STEP('HLP_ICTBF03_10500701','&&WORKFLOW_NAME',(SELECT EXECUTION_SESSION_ID FROM MAX_EXECUTION_SESSION_ID));

BT;

/* #############################################################################################
# TRUNCATING LDR TABLE
 #############################################################################################*/

DELETE &&ENV_LDR.HLP_ICTBF03_10500701 ALL;

/* #############################################################################################
# LOADING INTO LDR TABLE 
 #############################################################################################*/


INSERT INTO &&ENV_LDR.HLP_ICTBF03_10500701
(INST_ID,CD_ISI,NU_PTL,PG_PTL,CD_NDG_FSC,CD_TIP_LNK,NM_CUS_FSC,CD_TIP_CUS,CD_CUS_ROL,DT_ELI_CUS,CD_MTC_UTE,TM_AGG_UTE,C_CUST_SEG,F_REFUSED ,F_TRADE_IN,CD_ASSET_CLASS,CD_ASSET_TYPE,RCORD_ID,REC_IND,INSRT_SESS_ID,UPDT_SESS_ID)

SELECT
	INST_TYPE.INST_TYPE_CD AS INST_ID,CD_ISI AS CD_ISI,NU_PTL AS NU_PTL,PG_PTL AS PG_PTL,CD_NDG_FSC AS CD_NDG_FSC,CD_TIP_LNK AS CD_TIP_LNK,NM_CUS_FSC AS NM_CUS_FSC,CD_TIP_CUS AS CD_TIP_CUS,CD_CUS_ROL AS CD_CUS_ROL,DT_ELI_CUS AS DT_ELI_CUS,CD_MTC_UTE AS CD_MTC_UTE,TM_AGG_UTE AS TM_AGG_UTE,C_CUST_SEG AS C_CUST_SEG,F_REFUSED  AS F_REFUSED ,F_TRADE_IN AS F_TRADE_IN,NULL AS CD_ASSET_CLASS,NULL AS CD_ASSET_TYPE
    ,10500701 AS RCORD_ID
	,'I' AS REC_IND
	,SESS.EXECUTION_SESSION_ID 	
	,SESS.EXECUTION_SESSION_ID

FROM
	(
		SELECT * FROM
		
		(
			SELECT DT_ELI_CUS,CD_ISI,CD_MTC_UTE,F_REFUSED ,CD_TIP_CUS,C_CUST_SEG,TM_AGG_UTE,CD_CUS_ROL,CD_ASSET_CLASS,CD_NDG_FSC,PG_PTL,CD_ASSET_TYPE,CD_TIP_LNK,NU_PTL,NM_CUS_FSC,F_TRADE_IN
			,'No Info in SMX' AS SRC_TAB_KEY,'CTF-KSA_ICTBF03' AS SRC_TAB_NAME,	'DT_ELI_CUS,CD_ISI,CD_MTC_UTE,F_REFUSED ,CD_TIP_CUS,C_CUST_SEG,TM_AGG_UTE,CD_CUS_ROL,CD_ASSET_CLASS,CD_NDG_FSC,PG_PTL,CD_ASSET_TYPE,CD_TIP_LNK,NU_PTL,NM_CUS_FSC,F_TRADE_IN' AS SRC_COL_NAME
			FROM &&ENV_STAGE.CTF-KSA_ICTBF03 SRC 
			
			
		) AA
        
		QUALIFY ROW_NUMBER() OVER (CD_NDG_FSC IS NOT NULL AND CD_NDG_FSC IS <>'' AND CD_ISI IS NOT NULL AND CD_ISI IS <>'' AND NU_PTL IS NOT NULL AND NU_PTL IS <>'' AND PG_PTL IS NOT NULL AND PG_PTL IS <>'' CD_TP_LNK IS NOT NULL AND CD_TP_LNK IS <>''\
QUALIFY OVER PARTITION BY CD_ISI,NU_PTL,PG_PTL,CD_NDG_FSC,CD_TP_LNK AND ORDER BY CDC_TIMESTAMP)=1
	) SRC

	LEFT JOIN &&PRD_FSDM.INST_TYPE INST_TYPE ON INST_TYPE_NAME='ALRAJHI BANK KSA'
	
    CROSS JOIN
	MAX_EXECUTION_SESSION_ID SESS;


ET;

.IF ERRORCODE = 0 THEN .GOTO SUCCESS_STEP


/*#############################################################################################
# Logging Failure Entry in STEPS_LOG table in case failure
#############################################################################################*/
.IF ERRORCODE <> 0 THEN CALL &&ENV_METADATA.FAIL_STEP('HLP_ICTBF03_10500701','&&WORKFLOW_NAME',(SELECT EXECUTION_SESSION_ID FROM MAX_EXECUTION_SESSION_ID));
.QUIT ERRORCODE;


.LABEL SUCCESS_STEP

/*#############################################################################################
#  Recycle Handling PROCEDURE is invoked to move rows to Recycle Table.
#  Two Input paramters should be passed to RECYCLE_HANDLING SP
#  First input argument should be LDR table name like 'HLP_ICTBF03_10500701'
#  Second argument should be Job Type which can be 0,1, or 2
#  Third argument is an output argument which recieves status of Procedure Call
#############################################################################################*/
CALL &&ENV_METADATA.RECYCLE_HANDLING('HLP_ICTBF03_10500701',1,P_RESULT);

.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;
 

/*#############################################################################################
# LOGGING Success Entry in STEPS_LOG table
#############################################################################################*/
CALL &&ENV_METADATA.END_STEP('HLP_ICTBF03_10500701','&&WORKFLOW_NAME',(SELECT EXECUTION_SESSION_ID FROM MAX_EXECUTION_SESSION_ID),'SOURCE_TABLE_NAME',0,0,0,0,0,'LDR');
	
.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;
	


/*#############################################################################################
#  Closing the BTEQ Script, Session Closed and Logging off
#############################################################################################*/
  
.LOGOFF
.QUIT