

# Databricks notebook source
# Read the table and make the dataframe pretty. In particualr fix the names and adjust the forecast to 7 BUs and round up the numbers to next 100,000
fy = spark.sql("select distinct fiscalYear,fiscalqtr from finance.dbu_dollars where date = (select max(date) from finance.dbu_dollars)").first()[0]
quarter = spark.sql("select distinct fiscalYear,fiscalqtr from finance.dbu_dollars where date = (select max(date) from finance.dbu_dollars)").first()[1]
forecast_df = spark.sql("select BU, floor((forecast_lower + 99999) / 100000) * 100000 as forecast_lower,floor((forecast + 99999) / 100000) * 100000 as forecast, floor((forecast_upper + 99999) / 100000) * 100000 as forecast_upper from (select case when a.BU = 'MSFTMSFT' then 'Microsoft House Account' when a.BU = 'AMERPubSec' then 'PubSec' when a.BU = 'EMEAALL' then 'EMEA' when a.BU = 'AMEREnterprise' then 'AMER Enterprise' when a.BU = 'AMERDigital Native' then 'Digital Native' when a.BU = 'APJALL' then 'APJ' when a.BU = 'AMERMM/Comm' then 'AMER MM/Comm' end as BU, case when a.BU = 'AMEREnterprise' then a.forecast-b.forecast  else a.forecast end as forecast, case when a.BU = 'AMEREnterprise' then a.forecast_lower-b.forecast_lower else a.forecast_lower end as forecast_lower, case when a.BU = 'AMEREnterprise' then a.forecasT_upper-b.forecast_upper else a.forecast_upper end as forecast_upper from usage_forecasting_prod.ai_tableau_snapshot a cross join usage_forecasting_prod.ai_tableau_snapshot b where b.BU= 'MSFTMSFT' and a.forecast_as_of in (select max(forecast_as_of) from usage_forecasting_prod.ai_tableau_snapshot) and b.forecast_as_of in (select max(forecast_as_of) from usage_forecasting_prod.ai_tableau_snapshot))")

# COMMAND ----------

import os 
from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils
import yaml

# start the spark session. Need this for DBconnet. Can be removed if you are using DB notebook.
spark = SparkSession.builder.appName('temps-demo').config("spark.driver.bindAddress", "127.0.0.1").getOrCreate()
dbutils = DBUtils(spark)
dbutils.secrets.get(scope = "data-science-dashboards", key = "AWS_ACCESS_KEY_ID")
os.environ["AWS_ACCESS_KEY_ID"] = dbutils.secrets.get(scope = "data-science-dashboards", key = "AWS_ACCESS_KEY_ID")
os.environ["AWS_SECRET_ACCESS_KEY"] = dbutils.secrets.get(scope = "data-science-dashboards", key = "AWS_SECRET_ACCESS_KEY")

# COMMAND ----------

import boto3
import pandas as pd
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication

def create_multipart_message(
        sender: str, recipients: list, title: str, text: str=None, html: str=None, attachments: list=None)\
        -> MIMEMultipart:
    """
    Creates a MIME multipart message object.
    Uses only the Python `email` standard library.
    Emails, both sender and recipients, can be just the email string or have the format 'The Name <the_email@host.com>'.

    :param sender: The sender.
    :param recipients: List of recipients. Needs to be a list, even if only one recipient.
    :param title: The title of the email.
    :param text: The text version of the email body (optional).
    :param html: The html version of the email body (optional).
    :param attachments: List of files to attach in the email.
    :return: A `MIMEMultipart` to be used to send the email.
    """
    multipart_content_subtype = 'alternative' if text and html else 'mixed'
    msg = MIMEMultipart(multipart_content_subtype)
    msg['Subject'] = title
    msg['From'] = sender
    msg['Bcc'] = ', '.join(recipients)

    # Record the MIME types of both parts - text/plain and text/html.
    # According to RFC 2046, the last part of a multipart message, in this case the HTML message, is best and preferred.
    if text:
        part = MIMEText(text, 'plain')
        msg.attach(part)
    if html:
        part = MIMEText(html, 'html')
        msg.attach(part)

    # Add attachments
    for attachment in attachments or []:
        with open(attachment, 'rb') as f:
            part = MIMEApplication(f.read())
            part.add_header('Content-Disposition', 'attachment', filename=os.path.basename(attachment))
            msg.attach(part)

    return msg

  
def send_mail(
        sender: str, recipients: list, title: str, text: str=None, html: str=None, attachments: list=None) -> dict:
    """
    Send email to recipients. Sends one mail to all recipients.
    The sender needs to be a verified email in SES.
    """
    msg = create_multipart_message(sender, recipients, title, text, html, attachments)
    ses_client = boto3.client('ses', region_name='us-west-2')  # Use your settings here
    return ses_client.send_raw_email(
        Source=sender,
        Destinations=recipients,
        RawMessage={'Data': msg.as_string()}
    )

def create_table_html(df):
  html = f"""<html>
<style>
table, th, td {{border:1px solid black;}}
</style>
<body>
Hi,<br><br>This is an automated message. The following table reflects the latest Data Science BU-level $DBU forecast for {quarter} {fy}. You are receiving this email, because you have subscribed to the Data Science $DBU weekly forecast report. If you do not wish to receive this email or have any questions, please contact the Data team.<br><br>Thank you!<br><br>
<table border=1 frame=hsides rules=rows" style="border:1px solid black;margin-left:auto;margin-right:auto;">
  <tr>
    <th>BU</th>
    <th>Forecast</th>
  </tr>"""

  for i in range(df.shape[0]):
    html += f"""<tr>
      <td style="text-align:center"><b>{df.iloc[i][0]}</b></td>
      <td style="text-align:center">{'$'+"{0:,.0f}".format(df.iloc[i][2])}</td>
    </tr>
    """
    
  html += f"""<tr>
      <td style="text-align:center"><b>Total</b></td>
      <td style="text-align:center">{'$'+"{0:,.0f}".format(df.sum()[1])}</td>
    </tr>
    """
  
  html += """</table> 
  </body>
  </html>"""  
  return html


if __name__ == '__main__':
    forecast_as_of = str(spark.sql("select max(forecast_as_of) from usage_forecasting_prod.forecast_results").first()[0])
    sender_ = 'amir.issaei@databricks.com'
    recipients_ = ['amir.issaei@databricks.com']
#     ,'michael.schaaf@databricks.com','zack.froeberg@databricks.com','sheng.su@databricks.com','jp.pasvankias@databricks.com', 'jc.collins@databricks.com','jonp@databricks.com','arsalan@databricks.com','cory@databricks.com', 'mbaker@databricks.com', 'sam.shah@databricks.com','feng.pan@databricks.com', andy.kofoid@databricks.com,jonathan.hunt@databricks.com,lewis.hinch@databricks.com,katrina.ablett@databricks.com,zia.buck@databricks.com,marijan.luksa@databricks.com,sanika.goleria@databricks.com]
    title_ = f"Data Science {quarter} {fy} $DBU forecast as of {forecast_as_of}"
    df = forecast_df.toPandas()
    df['BU'] = pd.Categorical(df['BU'], ["AMER Enterprise", "PubSec", "AMER MM/Comm","EMEA","APJ","Digital Native","Microsoft House Account"])
    body_ = create_table_html(df)
    response_ = send_mail(sender_, recipients_, title_, html = body_)
    if response_['ResponseMetadata']['HTTPStatusCode'] != 200:
      raise ValueError('Email was not sent.')
    else:
      print('Email was sent.')
