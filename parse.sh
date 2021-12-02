#!/bin/bash

spark-submit parse.py --output transactions
cp transactions/part-00000 ${1:-transactions.csv}
