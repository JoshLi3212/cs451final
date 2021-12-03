#!/bin/bash

spark-submit --py-files script/block.py script/download_spark.py --start 600000 --end 699999
