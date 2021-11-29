#!/bin/python3

import requests
import sys
import os

path = "/u2/jeli/blockchain_data"
num_blocks = int(sys.argv[1])

resp = requests.get("https://blockchain.info/latestblock").json()
block_hash = resp["hash"]

for _ in range(num_blocks):
    resp = requests.get(f"https://blockchain.info/rawblock/{block_hash}?format=json").json()
    height = resp["height"]
    block_hash = resp["prev_block"]
    if not os.path.isfile(f"{path}/blk{height}.dat"):
        resp = requests.get(f"https://blockchain.info/rawblock/{block_hash}?format=hex")
        binary = bytes.fromhex(resp.text)
        with open(f"{path}/blk{height}.dat", "wb") as f:
            f.write(binary)
