import gdown, csv
from gsheets import *
import sqlite3


def upload_to_sqlite():

    con = sqlite3.connect("sessions.db") 
    cur = con.cursor()
    cur.execute("CREATE TABLE data (id,ip_address,date,continent,country);")

    with open('sessions.csv','r') as f:
        dr = csv.DictReader(f)
        print(dr)
        to_db = [(i['id'], i['ip_address'], i['date']) for i in dr]

    cur.executemany("INSERT INTO data (id,ip_address,date) VALUES (?, ?, ?);", to_db)
    
    con.commit()
    con.close()


def main():
    upload_to_sqlite()


if __name__ == '__main__':
    main()