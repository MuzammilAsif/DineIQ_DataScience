from dataclasses import dataclass, field

import numpy as np

SEED = 42
START_DATE = np.datetime64("2025-01-01")
END_DATE = np.datetime64("2025-12-31")
N_DAYS = int((END_DATE - START_DATE).astype(int)) + 1
PRICE_HISTORY_START = np.datetime64("2024-07-01")

BASE_VOLUMES = {
    "customers": 55_000,
    "orders": 185_000,
    "order_lines": 1_200_000,
    "ratings": 105_000,
}


@dataclass
class Context:
    """Hidden generation state shared between table builders; never written to CSV."""
    rng: np.random.Generator
    scale: float
    restaurants: dict = field(default_factory=dict)
    menu: dict = field(default_factory=dict)
    pricing: dict = field(default_factory=dict)
    customers: dict = field(default_factory=dict)
    promotions: dict = field(default_factory=dict)
    orders: dict = field(default_factory=dict)
    order_items: dict = field(default_factory=dict)
    inventory: dict = field(default_factory=dict)


def make_ids(prefix, count, width, start=1):
    numbers = np.arange(start, start + count).astype(str)
    return np.char.add(prefix, np.char.zfill(numbers, width))


def to_day_index(date_str):
    return int((np.datetime64(date_str) - START_DATE).astype(int))


def day_to_date_str(day_index):
    return np.datetime_as_string(START_DATE + np.asarray(day_index).astype("timedelta64[D]"), unit="D")


def dates_to_str(dates):
    return np.datetime_as_string(np.asarray(dates).astype("datetime64[D]"), unit="D")


def sample_from_cdf_rows(cdf, row_index, rng, low=None, high=None):
    """Draw one column index per entry of row_index from row-wise CDFs.

    cdf has shape (rows, cols + 1), starts at 0 and ends at 1 on every row. low/high optionally
    restrict each draw to columns [low, high] inclusive.
    """
    n_rows, width = cdf.shape
    cols = width - 1
    row_index = np.asarray(row_index)
    if low is None:
        low = np.zeros(len(row_index), dtype=np.int64)
    if high is None:
        high = np.full(len(row_index), cols - 1, dtype=np.int64)
    flat = (cdf + np.arange(n_rows)[:, None] * 2.0).ravel()
    lo_val = cdf[row_index, low]
    hi_val = cdf[row_index, high + 1]
    u = lo_val + rng.random(len(row_index)) * (hi_val - lo_val)
    pos = np.searchsorted(flat, row_index * 2.0 + u, side="left")
    col = pos - row_index * width - 1
    return np.clip(col, low, high)


def build_cdf(weights):
    weights = np.asarray(weights, dtype=np.float64)
    totals = weights.sum(axis=1, keepdims=True)
    totals[totals == 0] = 1.0
    cdf = np.zeros((weights.shape[0], weights.shape[1] + 1))
    cdf[:, 1:] = np.cumsum(weights / totals, axis=1)
    cdf[:, -1] = 1.0
    return cdf


def weighted_sample_without_replacement(weights, k, rng):
    keys = rng.random(len(weights)) ** (1.0 / np.maximum(weights, 1e-12))
    return np.argpartition(-keys, k - 1)[:k]
