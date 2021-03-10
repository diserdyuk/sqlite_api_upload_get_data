import csv
import sqlite3
import requests


def geo_data_for_ip(ip):
    URL = f'https://api.ipgeolocationapi.com/geolocate/{ip}'
    r = requests.get(url = URL) 
    data = r.json()
    return data 


def upload_data_to_sqlite():
    connection = sqlite3.connect("sessions_2.db") 
    cursor = connection.cursor()
    cursor.execute("CREATE TABLE data (id,ip_address,date,continent,country);")

    with open('sessions.csv') as file:
        reader = csv.DictReader(file)
        for row in reader:    
            ip = row['ip_address'].rpartition(':')[0]    # get IP

            try:
                continent = geo_data_for_ip(ip)['continent']
                country = geo_data_for_ip(ip)['name']
            except:
                continent = 'None'
                country = 'None'
            
            to_db = [(row['id'], row['ip_address'], row['date'], continent, country)]
            cursor.executemany("INSERT INTO data (id,ip_address,date,continent,country) VALUES (?, ?, ?, ?, ?);", to_db)
        
        connection.commit()
        connection.close()


def main():
    upload_data_to_sqlite()


if __name__ == '__main__':
    main()