#!/bin/python3

import argparse
import os
import shutil
import block
from datetime import timedelta
from pyspark import SparkContext, SparkConf

parser = argparse.ArgumentParser()
parser.add_argument("--input", default="/u2/jeli/blockchain_data")
parser.add_argument("--output", type=str, required=True)

args = parser.parse_args()
input_path = args.input
output_path = args.output

conf = SparkConf().setAppName("Bitcoin Parse")
sc = SparkContext(conf=conf)

blocks = sc.parallelize([f"{input_path}/{file}" for file in os.listdir(input_path)])

def block_to_transactions(block_path):
    blk = block.Block(block_path)

    # Round time up to nearest hour
    time = blk.time.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
    time = time.strftime("%Y-%m-%d %H")
    amount = sum(tx.amount for tx in blk.transactions)
    num_transactions = len(blk.transactions)
    return (time, (amount, num_transactions))

transactions = blocks \
    .map(block_to_transactions) \
    .filter(lambda kv: "2019-12-12 05" <= kv[0]) \
    .reduceByKey(lambda tup1, tup2: (tup1[0]+tup2[0], tup1[1]+tup2[1])) \
    .sortByKey() \
    .map(lambda kv: f"{kv[0]},{kv[1][0]},{kv[1][1]}") \
    .coalesce(1)

if os.path.exists(output_path):
    shutil.rmtree(output_path)
transactions.saveAsTextFile(output_path)
