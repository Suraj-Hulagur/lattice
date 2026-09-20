"""4+2 Reed-Solomon erasure coding over GF(256).

Splits an object into 4 data shards and generates 2 parity shards using
Reed-Solomon coding.  Any 4 of the 6 shards are sufficient to reconstruct
the original object.

Uses numpy for the Galois Field arithmetic -- no heavy third-party EC
library is needed for a 4+2 scheme.
"""

import numpy as np

# ---------- Galois Field GF(2^8) tables ----------

_EXP = np.zeros(512, dtype=np.int32)
_LOG = np.zeros(256, dtype=np.int32)


def _init_tables():
    """Build exp/log tables for GF(2^8) with primitive polynomial 0x11d."""
    x = 1
    for i in range(255):
        _EXP[i] = x
        _LOG[x] = i
        x <<= 1
        if x & 0x100:
            x ^= 0x11d
    # Extend the exp table so mul/div never needs a modulo
    for i in range(255, 512):
        _EXP[i] = _EXP[i - 255]


_init_tables()


def gf_mul(a: int, b: int) -> int:
    if a == 0 or b == 0:
        return 0
    return int(_EXP[_LOG[a] + _LOG[b]])


def gf_div(a: int, b: int) -> int:
    if b == 0:
        raise ZeroDivisionError("division by zero in GF(256)")
    if a == 0:
        return 0
    return int(_EXP[(_LOG[a] - _LOG[b]) % 255])


def gf_inv(a: int) -> int:
    if a == 0:
        raise ZeroDivisionError("zero has no inverse in GF(256)")
    return int(_EXP[255 - _LOG[a]])


# ---------- Matrix helpers over GF(256) ----------

def _gf_mat_mul(A: np.ndarray, B: np.ndarray) -> np.ndarray:
    """Matrix multiply over GF(256)."""
    rows_a, cols_a = A.shape
    _, cols_b = B.shape
    C = np.zeros((rows_a, cols_b), dtype=np.uint8)
    for i in range(rows_a):
        for j in range(cols_b):
            val = 0
            for k in range(cols_a):
                val ^= gf_mul(int(A[i, k]), int(B[k, j]))
            C[i, j] = val
    return C


def _gf_invert(matrix: np.ndarray) -> np.ndarray:
    """Invert a square matrix over GF(256) using Gauss-Jordan elimination."""
    n = matrix.shape[0]
    # Augment with identity
    work = np.zeros((n, 2 * n), dtype=np.uint8)
    work[:, :n] = matrix
    for i in range(n):
        work[i, n + i] = 1

    for col in range(n):
        # Find pivot
        pivot = -1
        for row in range(col, n):
            if work[row, col] != 0:
                pivot = row
                break
        if pivot == -1:
            raise ValueError("Matrix is singular in GF(256)")

        # Swap rows
        if pivot != col:
            work[[col, pivot]] = work[[pivot, col]]

        # Scale pivot row
        inv = gf_inv(int(work[col, col]))
        for j in range(2 * n):
            work[col, j] = gf_mul(int(work[col, j]), inv)

        # Eliminate
        for row in range(n):
            if row == col or work[row, col] == 0:
                continue
            factor = int(work[row, col])
            for j in range(2 * n):
                work[row, j] ^= gf_mul(factor, int(work[col, j]))

    return work[:, n:]


# ---------- Encoding matrix ----------

DATA_SHARDS = 4
PARITY_SHARDS = 2
TOTAL_SHARDS = DATA_SHARDS + PARITY_SHARDS


def _vandermonde_matrix() -> np.ndarray:
    """Build a 6x4 encoding matrix (Vandermonde-style).

    Top 4 rows are the identity (so data shards = original data).
    Bottom 2 rows are the parity rows.
    """
    matrix = np.zeros((TOTAL_SHARDS, DATA_SHARDS), dtype=np.uint8)
    # Identity for data rows
    for i in range(DATA_SHARDS):
        matrix[i, i] = 1
    # Parity rows: row i, col j = (i+1)^j in GF(256)
    for i in range(PARITY_SHARDS):
        for j in range(DATA_SHARDS):
            # Use small generator values to keep things simple
            val = 1
            base = i + 1  # 1, 2
            for _ in range(j):
                val = gf_mul(val, base)
            matrix[DATA_SHARDS + i, j] = val
    return matrix


ENCODE_MATRIX = _vandermonde_matrix()


# ---------- Public API ----------

def encode(data: bytes) -> list[bytes]:
    """Split `data` into 4 data shards + 2 parity shards.

    Each shard is a bytes object.  The original data can be recovered from
    any 4 of the 6 shards.
    """
    # Pad data to a multiple of DATA_SHARDS
    padded_len = len(data)
    remainder = padded_len % DATA_SHARDS
    if remainder:
        data = data + b'\x00' * (DATA_SHARDS - remainder)
        padded_len = len(data)

    shard_size = padded_len // DATA_SHARDS

    # Split into data shards
    data_shards = []
    for i in range(DATA_SHARDS):
        data_shards.append(data[i * shard_size : (i + 1) * shard_size])

    # Compute parity shards
    parity_shards = []
    for p in range(PARITY_SHARDS):
        parity = bytearray(shard_size)
        for byte_idx in range(shard_size):
            val = 0
            for d in range(DATA_SHARDS):
                val ^= gf_mul(int(ENCODE_MATRIX[DATA_SHARDS + p, d]), data_shards[d][byte_idx])
            parity[byte_idx] = val
        parity_shards.append(bytes(parity))

    return data_shards + parity_shards


def decode(shards: list[bytes | None], original_size: int) -> bytes:
    """Reconstruct the original data from any 4 of 6 shards.

    `shards` is a list of length 6. Missing shards must be None.
    `original_size` is the byte length of the original (unpadded) data.
    """
    present = [(i, s) for i, s in enumerate(shards) if s is not None]
    if len(present) < DATA_SHARDS:
        raise ValueError(
            f"Need at least {DATA_SHARDS} shards to reconstruct, "
            f"only have {len(present)}"
        )

    # Take the first DATA_SHARDS available shards
    used = present[:DATA_SHARDS]
    indices = [i for i, _ in used]
    shard_data = [s for _, s in used]
    shard_size = len(shard_data[0])

    # Build the sub-matrix from the encoding matrix using the available rows
    sub_matrix = np.zeros((DATA_SHARDS, DATA_SHARDS), dtype=np.uint8)
    for row, idx in enumerate(indices):
        for col in range(DATA_SHARDS):
            sub_matrix[row, col] = ENCODE_MATRIX[idx, col]

    # Invert it
    inv_matrix = _gf_invert(sub_matrix)

    # Multiply the inverse by the available shard data to recover original data
    recovered_shards = []
    for d in range(DATA_SHARDS):
        recovered = bytearray(shard_size)
        for byte_idx in range(shard_size):
            val = 0
            for k in range(DATA_SHARDS):
                val ^= gf_mul(int(inv_matrix[d, k]), shard_data[k][byte_idx])
            recovered[byte_idx] = val
        recovered_shards.append(bytes(recovered))

    # Concatenate and trim padding
    full = b''.join(recovered_shards)
    return full[:original_size]


def reconstruct_shard(shards: list[bytes | None], target_index: int) -> bytes:
    """Reconstruct a single missing shard (for repair).

    Uses the same decode logic but then re-encodes the target shard.
    """
    present = [(i, s) for i, s in enumerate(shards) if s is not None]
    if len(present) < DATA_SHARDS:
        raise ValueError(
            f"Need at least {DATA_SHARDS} shards to reconstruct, "
            f"only have {len(present)}"
        )

    used = present[:DATA_SHARDS]
    indices = [i for i, _ in used]
    shard_data = [s for _, s in used]
    shard_size = len(shard_data[0])

    # Build sub-matrix and invert
    sub_matrix = np.zeros((DATA_SHARDS, DATA_SHARDS), dtype=np.uint8)
    for row, idx in enumerate(indices):
        for col in range(DATA_SHARDS):
            sub_matrix[row, col] = ENCODE_MATRIX[idx, col]
    inv_matrix = _gf_invert(sub_matrix)

    # Recover original data shards first
    original_data = []
    for d in range(DATA_SHARDS):
        recovered = bytearray(shard_size)
        for byte_idx in range(shard_size):
            val = 0
            for k in range(DATA_SHARDS):
                val ^= gf_mul(int(inv_matrix[d, k]), shard_data[k][byte_idx])
            recovered[byte_idx] = val
        original_data.append(bytes(recovered))

    # Now re-encode the target shard
    if target_index < DATA_SHARDS:
        # It's a data shard, already recovered
        return original_data[target_index]
    else:
        # It's a parity shard, re-encode it
        parity = bytearray(shard_size)
        for byte_idx in range(shard_size):
            val = 0
            for d in range(DATA_SHARDS):
                val ^= gf_mul(int(ENCODE_MATRIX[target_index, d]), original_data[d][byte_idx])
            parity[byte_idx] = val
        return bytes(parity)
