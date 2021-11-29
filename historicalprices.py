#!/bin/python3
import datetime
import requests
import sys

start_time_str = sys.argv[1] + " 00:00:00"
end_time_str = sys.argv[2] + " 00:00:00"

start_time = datetime.datetime.fromisoformat(start_time_str)
end_time = datetime.datetime.fromisoformat(end_time_str)

API_KEY = "35bcff7eb2aedfce2d0125c3827774d30f531e8eafc041048e26bebe8f29b69b"
URL = "https://min-api.cryptocompare.com/data/v2/histohour"


start_hour = start_time.timestamp()
end_hour = end_time.timestamp()

f = open("historical.csv", "w")

while start_hour < end_hour:
    print(min(2000, (end_hour - start_hour) // 3600))
    params = {
        "api_key":API_KEY,
        "fsym":"BTC",
        "tsym":"USD",
        "limit": min(2000, (end_hour - start_hour) // 3600),
        "toTs": min(start_hour + 3600 * 2000, end_hour)
    }
    start_hour += 3600 * 2000
    r = requests.get(URL, params=params)
    for i in r.json()["Data"]["Data"]:
        sys.stdout = f
        time = i["time"]
        high = i["high"]
        low = i["low"]
        op = i["open"]
        print("{}, {}, {}, {}".format(datetime.datetime.fromtimestamp(time).strftime("%Y-%m-%d %H:%M:%S"), high, low, op))


