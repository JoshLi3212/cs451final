#!/bin/python3

import requests
import sys
import os
import time

path = "/u2/jeli/blockchain_data"
min_height = int(sys.argv[1])
temp_path = f"{path}/tmp.dat"

def get_height_hash(height):
    resp = requests.get(f"https://blockchain.info/block-height/{height}?format=json").json()
    back_height = height
    while len(resp["blocks"]) > 1:
        back_height += 1
        resp = requests.get(f"https://blockchain.info/block-height/{back_height}?format=json").json()

    block_hash = resp["blocks"][0]["hash"]

    while back_height > height:
        resp = requests.get(f"https://blockchain.info/rawblock/{block_hash}?format=json").json()
        block_hash = resp["prev_block"]
        back_height -= 1

    return block_hash

def get_prev_hash(raw_bytes):
    return raw_bytes[4:36][::-1].hex()

if (len(sys.argv) > 2):
    height = int(sys.argv[2])
    block_hash = get_height_hash(height)
else:
    resp = requests.get("https://blockchain.info/latestblock").json()
    height = int(resp["height"])
    block_hash = resp["hash"]

resp = requests.get(f"https://blockchain.info/rawblock/{block_hash}?format=json").json()
assert height == int(resp["height"])

while height >= min_height:
    while True:
        file_path = f"{path}/blk{height}.dat"
        if not os.path.exists(file_path):
            break
        print(f"Block at height {height} exists at {file_path}")
        height -= 1

    if height < min_height:
        break

    block_hash = get_height_hash(height)
    while not os.path.exists(file_path):
        while True:
            try:
                resp = requests.get(f"https://blockchain.info/rawblock/{block_hash}?format=hex")
                break
            except:
                time.sleep(0.1)
        if resp.status_code != 200:
            raise RuntimeError(resp.json())
        binary = bytes.fromhex(resp.text)

        with open(temp_path, "wb") as f:
            f.write(binary)
            f.flush()
            os.fsync(f.fileno())
        os.rename(temp_path, file_path)

        print(f"Block at height {height} downloaded to {file_path}")
        block_hash = get_prev_hash(binary)
        height -= 1
        file_path = f"{path}/blk{height}.dat"
        if height < min_height:
            break
