#!/bin/python3

import sys
from block import Block

path = "/u2/jeli/blockchain_data"
height = sys.argv[1]

block_path = f"{path}/blk{height}.dat"

block = Block(block_path)

output = block.__dict__
del(output["transactions"])
print(output)
