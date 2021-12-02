#!/bin/bash

spark-submit --py-files script/block.py script/parse.py --output transactions
cp transactions/part-00000 ${1:-data/transactions.csv}
