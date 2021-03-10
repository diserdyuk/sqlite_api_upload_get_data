import csv
import sqlite3
import requests


def upload_to_sqlite():

    con = sqlite3.connect("sessions.db") 
    cur = con.cursor()
    cur.execute("CREATE TABLE data (id,ip_address,date,continent,country);")

    with open('sessions.csv','r') as f:
        dr = csv.DictReader(f)
        to_db = [(i['id'], i['ip_address'], i['date']) for i in dr]

    cur.executemany("INSERT INTO data (id,ip_address,date) VALUES (?, ?, ?);", to_db)
    
    con.commit()
    con.close()


def get_geolocation():
    
    with open('sessions.csv') as f:
        reader = csv.DictReader(f)
        for i in reader:    
            ip = i['ip_address'].rpartition(':')[0]    # get IP
            
            URL = f'https://api.ipgeolocationapi.com/geolocate/{ip}'
            r = requests.get(url = URL) 
            data = r.json() 

            continent = data['continent']
            country = data['name']
            print(continent, country)

            # write to db  


def main():
    # upload_to_sqlite()
    get_geolocation()


if __name__ == '__main__':
    main()