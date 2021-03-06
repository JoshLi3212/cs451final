#!/bin/python3

import sys
from datetime import datetime, timedelta

path = f"clean_data/{sys.argv[1]}"

with open(path, "r") as f:
    prev_date = None
    for line in f:
        cur_date, price, volume = line.split(",")
        cur_date = datetime.strptime(cur_date, "%Y-%m-%d %H")
        #if prev_date is not None and prev_date + timedelta(hours=1) != cur_date:
        #    print(prev_date + timedelta(hours=1))
        if (float(price) == 0 or int(volume) == 0):
            print(line)
        prev_date = cur_date
