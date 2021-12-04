#!/bin/bash

spark-submit script/train/create_train_test.py
cp train/part-00000 train.csv
rm -r train
cp test/part-00000 test.csv
rm -r test
