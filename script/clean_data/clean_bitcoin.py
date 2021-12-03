#!/bin/python3

from datetime import datetime, timedelta

input_path = f"raw_data/bitcoin.csv"
output_path = f"clean_data/bitcoin.csv"

with open(input_path, "r") as input_file:
    with open(output_path, "w") as output_file:
        for line in input_file:
            if line[0].isdigit():
                time, _, _, price, _, _ = line.split(",")
                time = datetime.strptime(time, "%Y-%m-%d %H:%M:%S")
                time = time.replace(minute=0, second=0, microsecond=0)
                time = time.strftime("%Y-%m-%d %H")
                output_file.write(f"{time},{price}\n")
