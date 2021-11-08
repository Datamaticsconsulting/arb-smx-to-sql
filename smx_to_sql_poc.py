import utils
from string import Template
from datetime import datetime, timedelta
import subprocess

# For reading configs
from configparser import ConfigParser
import yaml

import pandas as pd

import re
import sys


#Read config-sah file
print('reading config file')
#sqltemplateinfo = config_object['SQL']
#pd_dir = config_object['PD_PATH']


with open("config.yaml", "r") as stream:
    try:
        configs = yaml.safe_load(stream)
        pd_dir = configs["PD_PATH"]["SQL"]
        config_rules = configs["RULES_SMX"]
        sqlsptemplateinfo = configs["SQL_TEMPLATES"]["SP_CALL_TEMPLATE"]
        mainpath = configs["PD_PATH"]["MAIN"]
        jobspath = configs["PD_PATH"]["JOBS"]
        jobsstatuspath = configs["PD_PATH"]["JOBS_STATUS"]
        smxpath = configs["PD_PATH"]["SMX"]
        print('reading config file end', type(config_rules))
    except yaml.YAMLError as exc:
        print(exc)

df_write = pd.DataFrame({"Table Name": [""], "RecordID": [""], "Dev Status": [""], "Comments": [""]})


def translate_excel_to_sql(df, x, y):    
    sel_statements = []
    recycle_inds = []
    recycle_flags = []
    join_conditions = []
    tgt_columns = []
    rc_ind = False
    rc_ind_val = set(df["Recycle Indicator"])
    rc_ind_val.remove("-")
    print(rc_ind_val)
    if ("".join(rc_ind_val) == 'Y'):
        sqltemplateinfo = configs["SQL_TEMPLATES"]["RECYCLE"]
        rc_ind = True
    else:
        sqltemplateinfo = configs["SQL_TEMPLATES"]["RECYCLE_NO"]

    def eval_rule(rule):
        return eval(rule)

    def get_join_and_select_statements(df):
        #print(df)
        for index, row in df.iterrows():
            rule = row["Rules"]
            source_col = row["Source Column"]
            tgt_column = row["Column Name"]
            sel_stat = ''
            recycle_ind = ''
            try:    
                environment = "&&" + row["Parent Database/Schema"]
            except:
                environment = "&&" + "-"
            find_cond = config_rules["RULE_1"]["COND"]
            code_result = compile(find_cond, "<string>", "eval")
            if eval(code_result):
                #print(eval(code_result))
                join_table = eval(compile(config_rules["RULE_1"]["TABLE"], "<string>", "eval"))
                #print(find_cond)
                join_condition = eval(compile(config_rules["RULE_1"]["JOIN_COND"], "<string>", "eval"))
                source_statement = eval(compile(config_rules["RULE_1"]["SRC_STATEMENT"], "<string>", "eval"))
                sel_stat = eval(compile(config_rules["RULE_1"]["SELECT_STATEMENT"], "<string>", "eval"))
                #print(join_condition)
                ## Handling multiple join columns
                if ("<<SOURCE_COLUMN2>>" in join_condition) and ("ON" in join_condition):
                    join_cols = set(row["Source Column"].split(","))
                    i = 0
                    for col in join_cols:
                        i = i + 1
                        #print(col.strip(), i)
                        join_condition = join_condition.replace("<<SOURCE_COLUMN"+str(i)+">>", "SRC." + col.strip())
                else:
                    #print(join_condition)
                    join_condition = join_condition.replace("<<SOURCE_COLUMN>>","SRC." + source_col).replace("<<SOURCE_COLUMN1>>","SRC." + source_col)
                #print(rule)
                if rc_ind:
                    recycle_flag = eval(compile(config_rules["RULE_1"]["RECYCLE_FLAG"], "<string>", "eval"))
                    recycle_inds.append(source_statement + "IS NULL")
                    recycle_flags.append(recycle_flag)
                join_conditions.append(join_condition)
                sel_statements.append(sel_stat)
                #print(sel_stat)
                tgt_columns.append(tgt_column)

    def get_select_statements(df):    
        for index, row in df.iterrows():
            rule = row["Rules"]
            #print(rule)
            sel_stat = ''
            source_statement = ''
            source_col = row["Source Column"]
            tgt_column = row["Column Name"]
            try:    
                environment = "&&" + row["Parent Database/Schema"]
            except:
                environment = "&&" + "-"
            all_cols = ''
            rule1 = ''
            for rule_config in config_rules:
                #print(config_rules[rule_config]["COND"])
                find_cond = config_rules[rule_config]["COND"]
                code_result = compile(find_cond, "<string>", "eval")
                #print(eval(code_result))
                if (config_rules[rule_config]["RULE_TYPE"] != "JOIN") and ("WHERE" not in rule):
                    #print(eval(code_result))
                    if eval(code_result):
                        #print("The rule is ", rule)
                        if ("<<SOURCE_COLUMN2>>" in rule) and (not "WHERE" in rule):
                            #print(rule)
                            all_cols = set(row["Source Column"].split(","))
                            i = 0
                            rule1 = rule
                            for col in all_cols:
                                i = i + 1
                                #print(col.strip(), i)
                                rule1 = rule1.replace("<<SOURCE_COLUMN"+str(i)+">>", col.strip())
                        #print(rule)
                        sel_stat = eval(compile(config_rules[rule_config]["SELECT_STATEMENT"], "<string>", "eval"))
                        sel_statements.append(sel_stat)
                        tgt_columns.append(tgt_column)
                """else:
                    if(find_cond):
                        print("Inside JOIN", compile(config_rules[rule_config]["TABLE"], "<string>", "eval"))
                        join_table = eval(compile(config_rules[rule_config]["TABLE"], "<string>", "eval"))
                        join_condition = eval(compile(config_rules[rule_config]["JOIN_COND"], "<string>", "eval"))
                        source_statement = eval(compile(config_rules[rule_config]["SRC_STATEMENT"], "<string>", "eval"))
                        sel_stat = eval(compile(config_rules[rule_config]["SELECT_STATEMENT"], "<string>", "eval"))
                        recycle_flag = eval(compile(config_rules[rule_config]["RECYCLE_FLAG"], "<string>", "eval"))
                        recycle_inds.append(source_statement + "IS NULL")
                        recycle_flags.append(recycle_flag)
                        join_conditions.append(join_condition)"""
                
                
                

    ## GET Source Column Names ##
    src_columns = set(df["Source Column"])
    src_columns.remove("-")
    src_columns = ",".join(src_columns)
    ## GET Source Column Names END##

    ## GET TGT Table, SRC System and Record Id ##
    src_system = utils.get_unique_val_df(df, "Source System", "-")
    tgt_table = utils.get_unique_val_df(df, "Table Name", "-")
    rec_id = utils.get_unique_val_df(df, "Record Id", "-").replace(".0", "")
    ## GET TGT Table, SRC System and Record Id END##

    ## GET Source Table Name ##
    src_table = src_system + "_" + utils.get_unique_val_df(df, "Source Table/File", "-")
    ## GET Source Table Name END##

    ## GET Where and Qulaify Names ##
    where_and_qualify = df["Rules"].iloc[0]
    if "/" in where_and_qualify:
        where_condition = "WEHRE " + where_and_qualify.split('/')[0]
        qualify_condition = where_and_qualify.split('/')[1].split("OVER")[1].replace(' AND ', ' ')
        remaining_sel = qualify_condition[qualify_condition.find("BY"):]
        remaining_sel = remaining_sel.split("ORDER BY")[1]
        remaining_sel = remaining_sel[:remaining_sel.find(" DESC")].replace(" ", ",")
        src_columns = src_columns+remaining_sel
    else:
        where_condition = ''
        qualify_condition = where_and_qualify
    ## GET Where and Qulaify Names END##



    ## GET SELECT statements, recycle_indicators and recycle_flags ##
    get_join_and_select_statements(df)
    get_select_statements(df)
    #print(tgt_columns)
    #print(sel_statements)
    #print(recycle_inds)
    #print(recycle_flags)
    #print(join_conditions)
    #sel_statements = list(filter(None, sel_statements))
    ## GET SELECT statements, recycle_indicators and recycle_flags END##


    ## GET TGT Table Column Names and RECYCLE Table##
    tgt_columns_str = ",".join(tgt_columns)
    tgt_columns_str = tgt_columns_str+",RCORD_ID,REC_IND,INSRT_SESS_ID,UPDT_SESS_ID"

    if (rc_ind):
        tgt_columns_str = tgt_columns_str + ",RECYCLE_FLAG,RECYCLE_REASON,SRC_TAB_KEY,SRC_TAB_NAME,SRC_COL_NAME,RECYCLE_CDC_TIMESTAMP"
    recycle_src_table = src_table + "_" + tgt_table + "_" + rec_id    
    ## GET TGT Table Column Names and RECYCLE Table END##


    rec_template_path = sqltemplateinfo
    content = utils.read_file_content(rec_template_path)

    insert_table = tgt_table + "_" + rec_id
    sel_statements_str = ",".join(sel_statements)
    if any(sel_statements.count(x) > 1 for x in sel_statements):
        sel_statements_str = sel_statements_str.replace(","+sel_statements[-1], "", 1)
    #print(",".join(sel_statements))
    SQL_TEMPLATE = Template(content)
    SQL_TEMPLATE = SQL_TEMPLATE.substitute(INSERT_TABLE = insert_table, SEL_STATEMENTS = sel_statements_str, TGT_COLUMNS = tgt_columns_str, REC_ID = rec_id, RECYCLE_IND = " OR ".join(recycle_inds), RECYCLE_FLAGS = "||'-'||".join(recycle_flags), SRC_COLUMNS = src_columns, SRC_TABLE = src_table, WHERE_COND = where_condition,RECYCLE_SRC_TABLE = recycle_src_table, QUALIFY_COND = qualify_condition, JOIN_CONDITIONS = "\n".join(join_conditions))

    content = utils.read_file_content(sqlsptemplateinfo)
    SQL_SP_TEMPLATE = Template(content)
    jobname = insert_table + "_LDR"
    ldrtablename = tgt_table + "_" + rec_id
    currtime = datetime.now().strftime("%Y-%m-%d")
    #print(currtime)
    SQL_SP_TEMPLATE = SQL_SP_TEMPLATE.substitute(JOB_NAME = insert_table, TGT_TABLENAME = tgt_table, CURRENT_TIME = currtime,LDR_TABLENAME = ldrtablename, SELECT_INSERT = SQL_TEMPLATE)


    #print(SQL_TEMPLATE)
    ## Write Final SQL to file ##
    filename = "\\" + tgt_table + "_" + rec_id + '_'
    wfilepath = pd_dir+filename 
    utils.write_file_content(wfilepath, SQL_SP_TEMPLATE, ".sql")
    ## Write Final SQL to file END## """
    #print("File writing successful for ", tgt_table)


#df = utils.read_excel_as_df("SMX.xlsx", "SGK_LOCTR", 10000403)
df_main = utils.read_excel_as_df(smxpath, "ARB Remediation CORE SMX")
df_jobs = utils.read_excel_as_df(jobspath, "jobs")


for x, y in zip(df_jobs['Table Name'], df_jobs['RecordID']):
    print(x, y)
    df = df_main[(df_main["Table Name"] == x) & (df_main["Record Id"] == y)]
    if df.empty:
        print("Please provide correct Target Table and Record Id")
        df_write = df_write.append(pd.DataFrame({"Table Name": [x],
         'RecordID': [y],
         'Dev Status': ["Not Done"],
         'Comments': ["Wrong Table and Record Id"]}))
        #exit()
    else:
        #translate_excel_to_sql(df, x, y)
        try:
            translate_excel_to_sql(df, x, y)
            df_write = df_write.append(pd.DataFrame({"Table Name": [x],
             "RecordID": [y],
             "Dev Status": ["Done"],
             "Comments": [""]}))
        except:
            df_write = df_write.append(pd.DataFrame({"Table Name": [x],
             "RecordID": [y],
             "Dev Status": ["Not Done"],
             "Comments": [sys.exc_info()]}))
            print(sys.exc_info())
print(df_write)         
utils.write_excel_as_df(jobsstatuspath, "jobs_status", df_write)     

