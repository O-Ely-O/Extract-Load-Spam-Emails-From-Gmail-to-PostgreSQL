# Perform ETL(Extract Transform Load) on Emails(Inbox, All Mail, Spam, Junk, etc.,) From Gmail --> PostgreSQL (For Analysis)
This simple project gives you the idea on how to perform ETL operation in your Mail Server and may be use the gathered data for further use e.x., Machine Learning like text classification, Word2Vec Conversion and so on. Here I extracted emails coming from my Spam Folder (msg_ID, sender_email, subject, datetime) you can also extract any folder like Inbox, Junk, etc., 

## System Requirements:

* PostgreSQL --> Database use for loading the data
* Your Mail Server (Outlook, Gmail, Yahoo, etc.,) Gmail in my case.
* Python Environment
* Additional Apache-Airflow for orchestrating ETL Process

## Installation Guide

### Step 1: Setting Up Postgre Database
1. [Guide on how to install Postgre in Windows.](https://www.postgresqltutorial.com/postgresql-getting-started/install-postgresql/)
2. Create Table:
```SQL
  create table spam_emails (
	id serial primary key,
	msg_id int,
	sender varchar not null,
	subject varchar not null,
	date_receive varchar,
	created_on timestamp default CURRENT_TIMESTAMP not null,
	updated_on timestamp default CURRENT_TIMESTAMP not null
)
```

### Step 2: Python Environment
Here I use Jupyter Notebook to run python and this are the library I used
* IMAPClient --> Python Library to fetch and extract emails from mail server
* Pandas --> to convert those extracted emails into DataFrame and further use to load it to database
* psycopg2 --> library to connect Databases
* Regular Expression --> to perform transformation to extract sender`s email

### Step 3: Implement Code
1. Install Python Libraries: to install simply run this command
```python
pip install imapclient, pandas
```
2. Import Libraries & Set Up connection to Database:
```python
import os, email, re
import imaplib
import datetime
import psycopg2, yaml
import pandas as pd
import numpy as np
from imapclient import IMAPClient
from timeit import default_timer as timer

# Instantiate connection with MailServer & Postgres
with open ("secret.yml", 'r') as f:
    data = yaml.full_load(f)

param_dic = {
    "host"      : data.get('host'),
    "database"  : data.get('database'),
    "user"      : data.get('user'),
    "password"  : data.get('password')
}

EMAIL_UN = os.environ['UNAME']
EMAIL_PW = os.environ['UPASS']
server = IMAPClient('imap.gmail.com', use_uid=True, ssl=True)

# Establish Connection
server.login(EMAIL_UN, EMAIL_PW)
```
3. Perform ETL
```python
# Create DataFrame to load fetched emails
email_df = pd.DataFrame(columns=['MSG_ID','From','Subject','DATE_RECIEVED'])

# Select Folder
select_info = server.select_folder('[Gmail]/Spam')
#print('%d messages in INBOX' % select_info[b'EXISTS'])

# Filter Search
messages = server.search(['ALL'])
#print("%d messages" % len(messages))

# Fetched All Data based on the searched criteria
for msgid, data in server.fetch(messages, ['ENVELOPE','RFC822']).items():
    try:
        envelope = data[b'ENVELOPE']
        email_message = email.message_from_bytes(data[b'RFC822'])
        new_row = pd.Series({"MSG_ID":msgid, "From":email_message.get("From") ,"Subject":envelope.subject.decode(), "DATE_RECIEVED":envelope.date})
        email_df = email_df._append(new_row, ignore_index=True)
        print('Email Fetched: ID-NO:%d: "%s" Received@: %s' % (msgid, envelope.subject.decode(), envelope.date))
    except:
        new_row = pd.Series({"MSG_ID":msgid, "From":email_message.get("From"), "Subject":'Not Able to Decode', "DATE_RECIEVED":envelope.date})
        email_df = email_df._append(new_row, ignore_index=True)
        print('Email Failed: Can`t Decode')

# Perform Reworks on DataFrame to prepare for loading in PostgreSQL
def find_email(text):
    email = re.findall(r'[\w\.-]+@[\w\.-]+',str(text))
    return ",".join(email)
email_df['From']=email_df['From'].apply(lambda x: find_email(x))
#print("\Extracting email from dataframe columns:")
#print(email_df['sender_email'])

df = email_df
df = df.rename(columns={
    "MSG_ID": "msg_id",
    "From": "sender",
    "Subject": "subject",
    "DATE_RECIEVED": "date_receive"
})
df = df.replace(np.nan, 'N/A', regex=True)

# Connect and Load the DATA in PostgreSQL

# This function will allow us to connect to the database
def connect(params_dic):
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params_dic)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        sys.exit(1) 
    print("Connection successful")
    return conn
    
conn = connect(param_dic)
# Function to execute any query in the database
def execute_query(conn, query):
    """ Execute a single query """
    
    ret = 0 # Return value
    cursor = conn.cursor()
    try:
        cursor.execute(query)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1

    # If this was a select query, return the result
    if 'select' in query.lower():
        ret = cursor.fetchall()
    cursor.close()
    return ret
# Here we use the execute_many operation
def execute_many(conn, df, table):
    """
    Using cursor.executemany() to insert the dataframe
    """
    # Create a list of tupples from the dataframe values
    tuples = [tuple(x) for x in df.to_numpy()]
    # Comma-separated dataframe columns
    cols = ','.join(list(df.columns))
    # SQL quert to execute
    query  = "INSERT INTO %s(%s) VALUES(%%s,%%s,%%s,%%s)" % (table, cols)
    cursor = conn.cursor()
    try:
        cursor.executemany(query, tuples)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    print("execute_many() done")
    cursor.close()

# Run the execute_many strategy
execute_many(conn, df, 'spam_emails')
```
