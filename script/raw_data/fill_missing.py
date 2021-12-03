#!/bin/python3

import sys
from datetime import datetime, timedelta

file_path = sys.argv[1]

with open(file_path, "r") as file_from:
    lines = file_from.readlines()
    with open("tmp.csv", "w") as file_to:
        prev_date = None
        file_to.write(lines[0])
        for cur_line in lines[1:]:
            cur_date, rest = cur_line.split(",", 1)
            cur_date = datetime.strptime(cur_date, "%Y-%m-%d %H:%M:%S")
            if prev_date and prev_date >= cur_date:
                cur_date = prev_date + timedelta(hours=1)
            prev_date = cur_date
            cur_date = cur_date.strftime("%Y-%m-%d %H:%M:%S")
            file_to.write(f"{cur_date},{rest}")

with open("tmp.csv", "r") as f:
    prev_date = None
    for line in f.readlines()[1:]:
        cur_date, rest = line.split(",", 1)
        cur_date = datetime.strptime(cur_date, "%Y-%m-%d %H:%M:%S")
        assert prev_date is None or prev_date + timedelta(hours=1) == cur_date
        prev_date = cur_date
