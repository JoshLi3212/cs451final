import csv
import requests
import sys

# replace the "demo" apikey below with your own key from https://www.alphavantage.co/support/#api-key
CSV_URL = 'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY_EXTENDED&symbol=SPY&interval=60min&slice=year2month12&apikey=DH8U3449SHQ3O38F'

API_KEY = "DH8U3449SHQ3O38F"


f = open("spy.csv", "w")

params = {
    "function":"TIME_SERIES_INTRADAY_EXTENDED",
    "symbol":"IBM",
    "interval":"60min",
    "slice":"year2month12",
    "apikey":API_KEY
}

sys.stdout = f
with requests.Session() as s:
    download = s.get(CSV_URL)
    decoded_content = download.content.decode('utf-8')
    cr = csv.reader(decoded_content.splitlines(), delimiter=',')
    my_list = list(cr)
    for row in my_list:
        print("{}, {}, {}, {}, {}, {}".format(row[0], row[1], row[2], row[3], row[4], row[5]))