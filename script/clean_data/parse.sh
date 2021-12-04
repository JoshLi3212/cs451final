#!/bin/bash

spark-submit --py-files script/clean_data/block.py script/clean_data/parse.py --output transactions
hadoop fs -get transactions/part-00000 clean_data/transactions.csv
