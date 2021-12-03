#!/bin/bash

spark-submit --py-files script/block.py script/parse.py --output transactions
hadoop fs -get transactions/part-00000 clean_data/transactions.csv
