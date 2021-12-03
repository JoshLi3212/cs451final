#!/bin/python3

import argparse
import os
import shutil
from datetime import datetime, timedelta
from pyspark import SparkContext, SparkConf

parser = argparse.ArgumentParser()
parser.add_argument("--input", default="/u2/jeli/cs451/final/clean_data")

args = parser.parse_args()
input_path = args.input

conf = SparkConf().setAppName("Create Training Data")
sc = SparkContext(conf=conf)

def parse(line):
    time, price, volume = line.split(",")
    price = float(price)
    volume = int(volume)
    return (time, (price, volume))

def btc_parse(line):
    time, price = line.split(",")
    return (time, float(price))

def add_hour(time):
    time = datetime.strptime(time, "%Y-%m-%d %H") + timedelta(hours=1)
    return time.strftime("%Y-%m-%d %H")

def sub_hour(time):
    time = datetime.strptime(time, "%Y-%m-%d %H") - timedelta(hours=1)
    return time.strftime("%Y-%m-%d %H")

def safe_return(cur, prev):
    if prev == 0:
        return 0
    return (cur-prev)/prev

def single_return(kv):
    time, returns = kv
    cur, prev = returns
    return (time, safe_return(cur, prev))

def calculate_return(file):
    rdd = sc.textFile(f"{input_path}/{file}") \
        .map(lambda line: parse(line)) \
        .cache()
    rdd_offset = rdd.map(lambda kv: (add_hour(kv[0]), kv[1]))

    def pair_return(kv):
        time, returns = kv
        cur, prev = returns
        price_ret = safe_return(cur[0], prev[0])
        vol_ret = safe_return(cur[1], prev[1])
        return (time, (price_ret, vol_ret))

    rdd = rdd \
        .join(rdd_offset) \
        .map(pair_return)
    return rdd

transactions = calculate_return("transactions.csv")
gld = calculate_return("gld.csv")
spy = calculate_return("spy.csv")
uso = calculate_return("uso.csv")

btc = sc.textFile(f"{input_path}/bitcoin.csv") \
    .map(btc_parse) \
    .cache()
btc_offset = btc.map(lambda kv: (add_hour(kv[0]), kv[1]))
btc_return = btc.join(btc_offset) \
    .map(single_return)
btc_offset = btc.map(lambda kv: (sub_hour(kv[0]), kv[1]))
btc_buy = btc.join(btc_offset) \
    .map(lambda kv: (kv[0], kv[1][0] <= kv[1][1]))
btc = btc_return.join(btc_buy)

def join_tuples(kv):
    key, tuples = kv
    return (key, tuples[0] + tuples[1])

data = transactions.join(gld).map(join_tuples)
data = data.join(spy).map(join_tuples)
data = data.join(uso).map(join_tuples)
data = data.join(btc).map(join_tuples)
data.cache()

train_data = data.filter(lambda kv: "2019-12-12 05" <= kv[0] < "2021-11-01 00") \
    .sortByKey() \
    .map(lambda kv: kv[0] + "," + ",".join(str(val) for val in kv[1])) \
    .coalesce(1)
test_data = data.filter(lambda kv: "2021-11-01 00" <= kv[0] < "2021-12-01 00") \
    .sortByKey() \
    .map(lambda kv: kv[0] + "," + ",".join(str(val) for val in kv[1])) \
    .coalesce(1)

if os.path.exists("train"):
    shutil.rmtree("train")
if os.path.exists("test"):
    shutil.rmtree("test")

train_data.saveAsTextFile("train")
test_data.saveAsTextFile("test")
