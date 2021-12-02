#!/bin/python3

import sys
from datetime import datetime, time, timedelta

file_path = sys.argv[1]

with open(file_path, "r") as file_from:
    lines = file_from.readlines()
    with open("tmp.csv", "w") as file_to:
        prev_date = curLine = None
        file_to.write(lines[0])
        for cur_line in reversed(lines[1:]):
            cur_date, _ = cur_line.split(",", 1)
            cur_date = datetime.strptime(cur_date, "%Y-%m-%d %H:%M:%S")
            while prev_date and prev_date + timedelta(hours=1) != cur_date:
                file_to.write(prev_line)
                prev_date += timedelta(hours=1)
            file_to.write(cur_line)
            prev_date = cur_date
            prev_line = cur_line





