import csv
import requests
import sys

# replace the "demo" apikey below with your own key from https://www.alphavantage.co/support/#api-key

API_KEY = "DH8U3449SHQ3O38F"


f = open("uso.csv", "a")


with requests.Session() as s:
    for i in range(23, 24):
        print(i)
        year = i // 12 + 1
        month = i % 12 + 1
        querydate = "year{}month{}".format(year, month)
        print(querydate)
        CSV_URL = 'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY_EXTENDED&symbol=USO&interval=60min&slice={}&apikey=DH8U3449SHQ3O38F'.format(querydate)
        download = s.get(CSV_URL)
        decoded_content = download.content.decode('utf-8')
        cr = csv.reader(decoded_content.splitlines(), delimiter=',')
        my_list = list(cr)
        for row in my_list[1:]:
            sys.stdout = f
            print("{},{},{},{},{},{}".format(row[0], row[1], row[2], row[3], row[4], row[5]))
            sys.stdout = sys.__stdout__