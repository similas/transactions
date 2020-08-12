import time
import psycopg2
import numpy as np
import seaborn as sns
from faker import Faker
from scipy.stats import beta
from termcolor import colored
from random import choice, randint


fake = Faker()
records = 10000

'''
Data Vars:
    Card Number (6 Digits)
    Transaction
            Type
            Result
            Time
            Amount
'''

def create_table():

    # Creating Table
    try:
        cur.execute('''
          CREATE TABLE transactions
          (ID                   SERIAL    PRIMARY KEY     NOT NULL,
          CardNumber            INT                       NOT NULL,
          TransResult           CHAR(5),
          TransAmount           INT,
          TransTime             DATE,
          TransType             CHAR(5))''')

        cur.execute(''' ALTER SEQUENCE transactions_id_seq RESTART WITH 1 INCREMENT BY 1;''')
        print(colored("Table created successfully", "green"))
    except:
        print(colored("Table Already Exists", "yellow"))

def generate_data():
       return [transaction_generator(amount) for amount in amount_generator()]

def insert_table(data):

    insert_query = """ INSERT INTO transactions
    (CardNumber, TransResult, TransAmount, TransTime, TransType)
    VALUES (%s,%s,%s,%s,%s)"""

    for record_to_insert in data:
       cur.execute(insert_query, record_to_insert)

    conn.commit()
    print(colored('Records inserted successfully into transactions table', 'green'))

def date_generator():
    # fake.date_between(start_date='today', end_date='+30y')
    generated_date = fake.date_time_between(start_date='-30d', end_date='now')
    generated_date = f'''
    {generated_date.year}-{generated_date.month}-{generated_date.day}
    '''
    return generated_date

def amount_generator():
    mu, sigma = 0, 0.1  # mean and standard deviation
    data_beta = beta.rvs(1, 100, size=records)
    generated_amounts = data_beta * 100
    for num in generated_amounts:
        yield(num)

def cardnumber_generator():
    card_prefixes_list = ['123', '321']
    new_cardnum = choice(card_prefixes_list) + str(randint(100,999))
    return new_cardnum

def transaction_generator(amount):
    transaction_type = "W" if amount < 0 else "D" # D(Deposit) , W(Withdrawl)
    transaction_result = choice(['S', 'U']) # S(Successful) , U(Unsuccessful)
    transaction_time = date_generator()
    transaction_amount = abs(int(amount * 10**7))
    card_number = cardnumber_generator()
    return card_number, transaction_result, transaction_amount, transaction_time, transaction_type


if __name__ == "__main__":
    conn = psycopg2.connect(dbname="adb" ,user="alisalimisadr")
    print("Opened database successfully")
    cur = conn.cursor()
    data = generate_data()
    insert_table(data)
    cur.close()
    conn.close()
