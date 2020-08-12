import time
import psycopg2
from termcolor import colored



# Input Values
TABLES_COUNT = 5
CARDNUM = "123789"
FROM_DATE = '2020-07-15'
TO_DATE = '2020-07-25'



def create_Ntables(TABLES_COUNT):

    # Creating Table
    for i in range(TABLES_COUNT):
        table_name = f"table_{str(i)}"
        try:
            cur.execute(f'''
                CREATE TABLE {table_name}(
                LIKE transactions
                INCLUDING all);
                ''')

            print(colored("Table created successfully", "green"))
        except:
            print(colored("Table Already Exists", "yellow"))

        conn.commit()

def fetch_data():
    ''' Fetches data from 'transactions' table '''

    the_query ='''SELECT * FROM transactions;'''
    cur.execute(the_query)
    conn.commit()
    records = cur.fetchall()

    print(colored(f" {len(records)} RECORDS FETCHED FROM TRANSACTIONS : ", 'green'))
    print()
    return records

def insert_to_tables(data):

    for rec in data:
        table_number = hash(str(rec[1])) % TABLES_COUNT # using hash to avoid collisions (reduce them)
        table_name = f"table_{str(table_number)}"
        insert_query = f"""
                    INSERT INTO {table_name}
                    (CardNumber, TransResult, TransAmount, TransTime, TransType)
                    VALUES (%s,%s,%s,%s,%s)"""
        # Inserting data , related to each table
        try:
            cur.execute(insert_query, rec[1:])
        except:
            print("Problem inserting data")

    print(colored("Data inserted.", "green"))
    conn.commit()

def query_generator(CARDNUM, FROM_DATE, TO_DATE):
    # which table to insert
    '''
    How do we split data ? hash(record) MOD of N is 0 -> N-1 possible values , that helps
    us split data into N categories whose cardnumbers mod differ and avoid collisions may occur.
    '''
    table_number = hash(CARDNUM) % TABLES_COUNT
    table_name = f"table_{table_number}"
    # select from the table that is most probable to be have the cardNum data
    QUERY = f'''
    SELECT * FROM {table_name}
    WHERE
    cardnumber = {CARDNUM}
    AND
    (transtime BETWEEN \'{FROM_DATE}\' and \'{TO_DATE}\');'''

    print("QUERY : ")
    print()
    print(colored(QUERY, 'blue'))
    print()
    print()
    return QUERY

def query_executor():
    the_query = query_generator(CARDNUM, FROM_DATE, TO_DATE)

    # Selecting data , related to each table
    cur.execute(the_query)
    conn.commit()
    records = cur.fetchall()

    print(colored("RECORDS : ", 'green'))
    print()
    for row in records:
        print(f"ID : {row[0]} | CARDNUMBER : {row[1]} | RESULT : {row[2]} | AMOUNT : {row[3]} | DATE : {row[4]} TYPE : {row[5]} ")
    print()
    print("----")

if __name__ == "__main__":
    conn = psycopg2.connect(dbname="adb" ,user="alisalimisadr")
    cur = conn.cursor()
    print("Opened database successfully")
    create_Ntables(TABLES_COUNT)
    data = fetch_data()
    insert_to_tables(data)
    query_executor()
    cur.close()
    conn.close()
