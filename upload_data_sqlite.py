import csv
import sqlite3
import requests


def geo_data_for_ip(ip):
    URL = f'https://api.ipgeolocationapi.com/geolocate/{ip}'
    r = requests.get(url = URL) 
    data = r.json()
    return data 


def get_ip(row):
    return row['ip_address'].rpartition(':')[0]


def main():
    connection = sqlite3.connect("sessions_2.db") 
    cursor = connection.cursor()
    cursor.execute("CREATE TABLE data (id,ip_address,date,continent,country);")

    with open('sessions.csv') as file:
        reader = csv.DictReader(file)
        for row in reader:    
            ip = get_ip(row)

            try:
                geo_data = geo_data_for_ip(ip)
                continent = geo_data['continent']
                country = geo_data['name']
            except:
                continent = 'None'
                country = 'None'

            cursor.execute("INSERT INTO data (id,ip_address,date,continent,country) \
                VALUES (?, ?, ?, ?, ?);", (row['id'], row['ip_address'], row['date'], continent, country))

        print('Общее кол-во посещений') 
        print(cursor.execute('SELECT COUNT(id) FROM data;').fetchall())
        
        print('Континент - количество IP')
        print(cursor.execute('SELECT continent,COUNT(ip_address) \
            FROM data GROUP BY continent;').fetchall())

        print('Кол-во уникальных IP за последние 2 недели')
        print(cursor.execute("SELECT COUNT(DISTINCT ip_address) FROM data WHERE date \
            BETWEEN '2020-02-01' AND '2020-02-15';").fetchall())
        
        print('Страна - количество IP в промежутке между 2020-02-01 и 2020-02-15')
        print(cursor.execute("SELECT country,COUNT(ip_address) FROM data WHERE date \
            BETWEEN '2020-02-01' AND '2020-02-15' GROUP BY country;").fetchall())

        connection.commit()
        connection.close()


if __name__ == '__main__':
    main()
