"""
alert.utils
~~~~~~~~~~~~~~
This module provides utility functions that are used within SMX to SQL
and are also useful for external consumption.
"""
import os

# Import pandas library
import pandas as pd
import numpy as np

# Import email libraries
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from smtplib import SMTP
import smtplib

from datetime import datetime, timedelta

from openpyxl import load_workbook


def read_excel_as_df(filepath, sheetname):
    df = pd.read_excel(filepath, sheet_name=sheetname)
    #print(df["Recycle Indicator"])
    df.dropna(subset = ["Table Name"], inplace = True)
    #df = df.drop(['1'])
    return df;
    
def write_excel_as_df(filepath, sheetname, df):
    book = load_workbook(filepath)
    writer = pd.ExcelWriter(filepath, engine="openpyxl")
    writer.book = book
    writer.sheets = {ws.title: ws for ws in book.worksheets}

    for sheetname in writer.sheets:
        df.to_excel(writer,sheet_name=sheetname)

    writer.save()
    print("DataFrame is written successfully to Excel File.")
    
def read_file_content(filepath):
    with open(filepath, "r") as file :
        filedata = file.read()
        return filedata


def write_file_content(filepath, content, extension):
    filepath = filepath + datetime.now().strftime('%Y%m%d') + extension
    with open(filepath, 'w') as file:
        file.write(content)
    
def get_sp_template():
    pass
    


def Logger(log_path, log_filename, log_retention, input_string):
  
    filename = "\\" + log_filename + datetime.now().strftime('%Y%m%d') + ".log"
  
    output_string = "\n" + datetime.now().strftime("%d-%b-%Y %H:%M:%S") + ", " + input_string + '\n'
  
    f = open(log_path + filename, "a")
    f.write(output_string)
    f.close()
  
    d = datetime.today() - timedelta(days = log_retention)
    old_filename_path = log_path + "\\" + log_filename + d.strftime('%Y%m%d') + ".log"
  
    if os.path.exists(old_filename_path):
          os.remove(old_filename_path)
    else:
         pass


def send_email(email_server, email_server_port, sender_email, sender_username, sender_password,
                       email_subject, reciepient_list, p_dataframe, p_category, message):
    mail_subject = email_subject + " (" + p_category + ")"         # from mysql
    #reciepient_list = "zeeshan.cornelius@gmail.com.com" #reciepient_list
    
    if p_category == "Low":
        color = "#ffc000"
    elif p_category == "Medium":
        color = "#c07a00"
    elif p_category == "High":
        color = "#c00000"
      
    date_var = datetime.now().strftime("%d-%b-%Y %I:%M %p")
    
    username = sender_username
    password = sender_password
    mail_from = sender_email
    mail_to = reciepient_list
    mail_subject = mail_subject
    
    mimemsg = MIMEMultipart('alternative')
    mimemsg['From']=mail_from
    mimemsg['To']=mail_to
    mimemsg['Subject']=mail_subject
    
    styler = p_dataframe.style.set_table_styles([dict(selector='th', props=[('text-align', 'center'),
                                                               ('border-style', 'solid'),
                                                               ('border-width', 'thin'),
                                                               ('border-color', 'black'),
                                                               ('border-collapse', 'collapse'),
                                                               ('font-size', '11pt'),
                                                               ('color', '#182354'),
                                                               ('font-family', '#verdana')
                                                              ])])
    
    styler.set_properties(**{'text-align': 'center', 'border-style' :'solid', 'border-width':'thin', 'border-color' :'black',
                         'border-collapse':'collapse',  'margin': '12px', 'font-size': '9pt',
                         'color': '#182354', 'font-family':'verdana'}).hide_index()
    html = """\
    <html> 
        <head>
          <title>Cyber Security Data Analytics Alert</title>
        </head>
        <body>               
             ${html_template}
        </body>
    </html>
    """.format(styler.render())
    part1 = MIMEText(html, 'html')
    mimemsg.attach(part1)
    connection = smtplib.SMTP(host = email_server, port = email_server_port)
    connection.starttls()
    connection.login(username,password)
    connection.send_message(mimemsg)
    connection.quit()
    
def get_unique_val_df(df, col_name, remove_char):
    unique_val = set(df[col_name].astype("string"))
    if remove_char in unique_val:
        #print(unique_val)
        unique_val.remove(remove_char)
    return "".join(unique_val)
