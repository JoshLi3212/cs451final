from hashlib import sha256
from datetime import datetime

def get_var_int(f):
    head = f.read(1)
    if head < bytes.fromhex("fd"):
        return int.from_bytes(head, "little")
    elif head == bytes.fromhex("fd"):
        size = 2
    elif head == bytes.fromhex("fe"):
        size = 4
    elif head == bytes.fromhex("ff"):
        size = 8
    else:
        raise RuntimeError("Variable int invalid size, this should never happen")
    return int.from_bytes(f.read(size), "little")

def blockchain_hash(b):
    return sha256(sha256(b).digest()).digest()[::-1].hex()

class Input:
    def __init__(self, f):
        start_pos = f.tell()

        self.prev_tx_hash = f.read(32)[::-1].hex()
        self.prev_tx_out_index = f.read(4)[::-1].hex()
        self.script_len = get_var_int(f)
        self.script = f.read(self.script_len).hex()
        self.sequence = f.read(4)[::-1].hex()

        self.size = f.tell() - start_pos

class Output:
    def __init__(self, f):
        start_pos = f.tell()

        self.value = int.from_bytes(f.read(8), "little")
        self.script_len = get_var_int(f)
        self.script = f.read(self.script_len).hex()

        self.size = f.tell() - start_pos

class Transaction:
    def __init__(self, f):
        start_pos = f.tell()

        self.version = f.read(4)[::-1].hex()

        self.num_inputs = get_var_int(f)
        witness_flag = False
        if self.num_inputs == 0:
            assert f.read(1) == b"\x01"
            self.num_inputs = get_var_int(f)
            witness_flag = True

        self.inputs = []
        for _ in range(self.num_inputs):
            self.inputs.append(Input(f))

        self.num_outputs = get_var_int(f)
        self.outputs = []
        for _ in range(self.num_outputs):
            self.outputs.append(Output(f))

        cur_pos = f.tell()
        f.seek(start_pos)
        transaction_bytes = f.read(cur_pos - start_pos)

        self.witnesses = []
        if witness_flag:
            transaction_bytes = transaction_bytes[:4] + transaction_bytes[6:]
            for _ in range(self.num_inputs):
                witness_component_count = get_var_int(f)
                for _ in range(witness_component_count):
                    witness_component_len = get_var_int(f)
                    if witness_component_len > 0:
                        witness = f.read(witness_component_len).hex()
                        self.witnesses.append(witness)

        self.lock_time = f.read(4)
        transaction_bytes += self.lock_time
        self.lock_time = self.lock_time[::-1].hex()
        
        self.size = f.tell() - start_pos
        self.hash = blockchain_hash(transaction_bytes)

class Block:
    def __init__(self, path):
        with open(path, "rb") as f:
            self.hash = blockchain_hash(f.read(80))
            f.seek(0)

            self.version = f.read(4)[::-1].hex()
            self.prev_block_hash = f.read(32)[::-1].hex()
            self.merkle_root = f.read(32)[::-1].hex()
            self.time = datetime.fromtimestamp(int.from_bytes(f.read(4), "little")).strftime("%Y-%m-%d %H-%M-%S")
            self.n_bits = f.read(4)[::-1].hex()
            self.nonce = int.from_bytes(f.read(4), "little")

            self.num_tx = get_var_int(f)
            self.transactions = []
            for _ in range(self.num_tx):
                self.transactions.append(Transaction(f))

            self.size = f.tell()
