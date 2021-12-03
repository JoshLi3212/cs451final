#!/bin/python3

import requests
import os
import argparse
from pyspark import SparkContext, SparkConf

parser = argparse.ArgumentParser()
parser.add_argument("--output", default="/u2/jeli/blockchain_data")
parser.add_argument("--start", type=int)
parser.add_argument("--end", type=int)

args = parser.parse_args()
output_path = args.output
start_height = args.start
end_height = args.end

def download(height):
    file_path = f"{output_path}/blk{height}.dat"
    temp_path = f"{output_path}/tmp{height}.dat"
    if not os.path.exists(file_path):
        try:
            resp = requests.get(f"https://blockchain.info/block-height/{height}?format=json").json()
            if (len(resp["blocks"]) == 1):
                block_hash = resp["blocks"][0]["hash"]
                resp = requests.get(f"https://blockchain.info/rawblock/{block_hash}?format=hex")
                if resp.status_code != 200:
                    return
                binary = bytes.fromhex(resp.text)

                with open(temp_path, "wb") as f:
                    f.write(binary)
                    f.flush()
                    os.fsync(f.fileno())
                os.rename(temp_path, file_path)
        except:
            if os.path.exists(temp_path):
                os.remove(temp_path)

conf = SparkConf().setAppName("Bitcoin Download")
sc = SparkContext(conf=conf)
heights = sc.parallelize(list(range(start_height, end_height+1)))
heights.foreach(download)
