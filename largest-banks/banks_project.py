# Libraries
import requests
from bs4 import BeautifulSoup
import pandas as pd
import sqlite3
import numpy as np
from datetime import datetime

# Known variables
url = "https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks"
table_attribs = ["Name", "MC_USD_Billion"]
db_name = "Banks.db"
table_name = "Largest_banks"
exchange_rate_csv_path = "./exchange_rate.csv"
output_csv_path = "./Largest_banks_data.csv"

# Log the progress of the code
def log_progress(message):
  timestamp_format = "%Y-%h-%d-%H:%M:%S"
  now = datetime.now()
  timestamp = now.strftime(timestamp_format)
  with open("./code_log.txt", "a") as f:
  f.write(timestamp + " : " + message + "\n")

# Extract the tabular information from the given URL and save it to a dataframe
def extract(url, table_attribs):
  html_page = requests.get(url).text
  data = BeautifulSoup(html_page, "html.parser")
  df = pd.DataFrame(columns=table_attribs)
  tables = data.find_all("tbody")
  rows = tables[0].find_all("tr")
  for row in rows:
    col = row.find_all("td")
    if len(col)!=0:
      link_tags = col[1].find_all("a")
      data_dict = {
        "Name":link_tags[1].contents[0],
        "MC_USD_Billion":col[2].contents[0]
      }
      df1 = pd.DataFrame(data_dict, index=[0])
      df = pd.concat([df, df1], ignore_index=True)
  df["MC_USD_Billion"] = df["MC_USD_Billion"].str[:-1]
  df["MC_USD_Billion"] = pd.to_numeric(df["MC_USD_Billion"], errors="coerce")
  return df

# Transform the dataframe by adding columns for market capitalization in GBP, EUR, and INR, rounded to 2 decimals, based on the exchange rate information shared as a CSV file
def transform(df, csv_path):
  dataframe = pd.read_csv(csv_path)
  exchange_rate = dataframe.set_index("Currency").to_dict()["Rate"]
  df["MC_GBP_Billion"] = [np.round(x*exchange_rate["GBP"],2) for x in df["MC_USD_Billion"]]
  df["MC_EUR_Billion"] = [np.round(x*exchange_rate["EUR"],2) for x in df["MC_USD_Billion"]]
  df["MC_INR_Billion"] = [np.round(x*exchange_rate["INR"],2) for x in df["MC_USD_Billion"]]
  return df

# Load the transformed dataframe to an output CSV file
def load_to_csv(df, output_path):
  df.to_csv(output_path)

# Load the transformed dataframe to an SQL database server as a table
def load_to_db(df, sql_connection, table_name):
  df.to_sql(table_name, sql_connection, if_exists="replace", index=False)

# Run queries on the database
def run_query(query_statement, sql_connection):
  print(query_statement)
  query_output = pd.read_sql(query_statement, sql_connection)
  print(query_output)

# Log entries
log_progress("Preliminaries complete. Initializing ETL process.")
df = extract(url, table_attribs)
log_progress("Data extraction complete. Initializing transformation process.")
df = transform(df, exchange_rate_csv_path)
log_progress("Data transformation complete. Initializing loading process.")
load_to_csv(df, output_csv_path)
log_progress("Data saved to CSV file.")
sql_connection = sqlite3.connect(db_name)
log_progress("SQL connection initiated.")
load_to_db(df, sql_connection, table_name)
log_progress("Data loaded to database as a table. Executing queries.")
# Print the contents of the entire table
query_statement = f"SELECT * FROM Largest_banks"
pd.set_option("display.max_columns", None)
run_query(query_statement, sql_connection)
# Print the average market capitalization of all the banks in Billion GBP
query_statement = f"SELECT AVG(MC_GBP_Billion) FROM Largest_banks"
run_query(query_statement, sql_connection)
# Print only the names of the top 5 banks
query_statement = f"SELECT Name from Largest_banks LIMIT 5"
run_query(query_statement, sql_connection)
log_progress("Process complete.")
sql_connection.close()
log_progress("Server connection closed.")
