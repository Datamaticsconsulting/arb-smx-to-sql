/* #############################################################################################
#  File-name   	: LIABTY_TYPE_10504201.sql
#  Part of     	: FSDM
#  Type        	: BTEQ - LDR
#  Job         	: LOAD LIABTY_TYPE
#  -------------------------------------------------------------------------
#  Purpose     	: The purpose of this file is to load data into the LIABTY_TYPE
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
CALL &&ENV_METADATA.INFA_SESSION_INSRT('&&WORKFLOW_NAME','LIABTY_TYPE_10504201');

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
WHERE B.SESSION_NAME = 'LIABTY_TYPE_10504201';

/* #############################################################################################
# LOGGING Start Entry in STEPS_LOG table
 #############################################################################################*/

CALL &&ENV_METADATA.START_STEP('LIABTY_TYPE_10504201','&&WORKFLOW_NAME',(SELECT EXECUTION_SESSION_ID FROM MAX_EXECUTION_SESSION_ID));

BT;

/* #############################################################################################
# TRUNCATING LDR TABLE
 #############################################################################################*/

DELETE &&ENV_LDR.LIABTY_TYPE_10504201 ALL;

/* #############################################################################################
# LOADING INTO LDR TABLE 
 #############################################################################################*/


INSERT INTO &&ENV_LDR.LIABTY_TYPE_10504201
(INST_ID,LIABTY_TYPE_CD,LIABTY_TYPE_NAME,LIABTY_TYPE_DESC,RCORD_ID,REC_IND,INSRT_SESS_ID,UPDT_SESS_ID)

SELECT
	INST_TYPE.INST_TYPE_CD AS INST_ID,"GENERATE SGK FOR 'UNKNOWN'
GENERATE SGK BASED ON UNIQUE VALUE OF TRIM(C_TP_PROD_APPL)" AS LIABTY_TYPE_CD,C_TP_PROD_APPL AS LIABTY_TYPE_NAME,CASE WHEN TRIM(C_TP_PROD_APPL) = CHC THEN 'Charge Card'
WHEN TRIM(C_TP_PROD_APPL) = CRC THEN  'Credit Card'
WHEN TRIM(C_TP_PROD_APPL) = CDC THEN  'Consumer Durable Card'
WHEN TRIM(C_TP_PROD_APPL) = TOD THEN  'Unauthorized Over Draft'
WHEN TRIM(C_TP_PROD_APPL) = POD THEN  'Authorized Over Draft'
WHEN TRIM(C_TP_PROD_APPL) = PLN THEN  'Personal Finance'
WHEN TRIM(C_TP_PROD_APPL) = RPLN THEN  'Restructured Personal Finance'
WHEN TRIM(C_TP_PROD_APPL) = TPLN THEN  'Top up Personal Finance'
WHEN TRIM(C_TP_PROD_APPL) = MTG THEN  'Mortgage'
WHEN TRIM(C_TP_PROD_APPL) = RMTG THEN  'Restructured Mortgage'
WHEN TRIM(C_TP_PROD_APPL) = CDL THEN  'Consumer Durables Loans'
WHEN TRIM(C_TP_PROD_APPL) = MSKN THEN  'Government product as Mortgage'
WHEN TRIM(C_TP_PROD_APPL) = RMSKN THEN  'Restructured Muskun'
WHEN TRIM(C_TP_PROD_APPL) = MGLD THEN  'Margin Lending'
WHEN TRIM(C_TP_PROD_APPL) = SFB THEN  'Secured Finance Program'
WHEN TRIM(C_TP_PROD_APPL) = SME THEN  'Small Medium Enterprise'
WHEN TRIM(C_TP_PROD_APPL) = RERA THEN  'Real Estate Rentals Agreement'
WHEN TRIM(C_TP_PROD_APPL) = STFM THEN  'Stock Finance Murabaha'
WHEN TRIM(C_TP_PROD_APPL) = EDUF THEN  'Education Finance'
WHEN TRIM(C_TP_PROD_APPL) = VLS THEN  'Car Lease Agreement'
WHEN TRIM(C_TP_PROD_APPL) = VIN THEN  'Car Installment Agreement'
WHEN TRIM(C_TP_PROD_APPL) = VRA THEN  'Car Rentals Agreement'
WHEN TRIM(C_TP_PROD_APPL) = MBL THEN  'Mobile Phone'
WHEN TRIM(C_TP_PROD_APPL) = LND THEN  'Landline Phone'
WHEN TRIM(C_TP_PROD_APPL) = VCLM THEN  'Vehicle Claim'
WHEN TRIM(C_TP_PROD_APPL) = CHCK THEN  'Bouncing Cheques'
END AS LIABTY_TYPE_DESC
    ,10504201 AS RCORD_ID
	,'I' AS REC_IND
	,SESS.EXECUTION_SESSION_ID 	
	,SESS.EXECUTION_SESSION_ID

FROM
	(
		SELECT * FROM
		
		(
			SELECT C_TP_PROD_APPL
			,'No Info in SMX' AS SRC_TAB_KEY,'CTF-KSA_ICTBB08' AS SRC_TAB_NAME,	'C_TP_PROD_APPL' AS SRC_COL_NAME
			FROM &&ENV_STAGE.CTF-KSA_ICTBB08 SRC 
			
			
		) AA
        
		QUALIFY ROW_NUMBER() OVER ('C_TP_PROD_APPL IS NOT NULL AND TRIM(C_TP_PROD_APPL)<> '' \
QUALIFY OVER PARTITION BYC_TP_PROD_APPL AND ORDER BY CDC_TIMESTAMP)=1
	) SRC

	LEFT JOIN &&PRD_FSDM.INST_TYPE INST_TYPE ON INST_TYPE_NAME='ALRAJHI BANK KSA'
	
    CROSS JOIN
	MAX_EXECUTION_SESSION_ID SESS;


ET;

.IF ERRORCODE = 0 THEN .GOTO SUCCESS_STEP


/*#############################################################################################
# Logging Failure Entry in STEPS_LOG table in case failure
#############################################################################################*/
.IF ERRORCODE <> 0 THEN CALL &&ENV_METADATA.FAIL_STEP('LIABTY_TYPE_10504201','&&WORKFLOW_NAME',(SELECT EXECUTION_SESSION_ID FROM MAX_EXECUTION_SESSION_ID));
.QUIT ERRORCODE;


.LABEL SUCCESS_STEP

/*#############################################################################################
#  Recycle Handling PROCEDURE is invoked to move rows to Recycle Table.
#  Two Input paramters should be passed to RECYCLE_HANDLING SP
#  First input argument should be LDR table name like 'LIABTY_TYPE_10504201'
#  Second argument should be Job Type which can be 0,1, or 2
#  Third argument is an output argument which recieves status of Procedure Call
#############################################################################################*/
CALL &&ENV_METADATA.RECYCLE_HANDLING('LIABTY_TYPE_10504201',1,P_RESULT);

.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;
 

/*#############################################################################################
# LOGGING Success Entry in STEPS_LOG table
#############################################################################################*/
CALL &&ENV_METADATA.END_STEP('LIABTY_TYPE_10504201','&&WORKFLOW_NAME',(SELECT EXECUTION_SESSION_ID FROM MAX_EXECUTION_SESSION_ID),'SOURCE_TABLE_NAME',0,0,0,0,0,'LDR');
	
.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;
	


/*#############################################################################################
#  Closing the BTEQ Script, Session Closed and Logging off
#############################################################################################*/
  
.LOGOFF
.QUIT