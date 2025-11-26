import os
import json
import pickle
import pandas as pd
from typing import Any, Union


# -------------------------------------------------------------
# Directory helper
# -------------------------------------------------------------
def ensure_dir(path: str):
    """
    Ensure directory exists for given file path.
    Example: ensure_dir("cache/data.csv") creates folder "cache".
    """
    directory = os.path.dirname(path)
    if directory and not os.path.exists(directory):
        os.makedirs(directory, exist_ok=True)


# -------------------------------------------------------------
# CSV helpers
# -------------------------------------------------------------
def save_csv(df: pd.DataFrame, path: str, index: bool = False):
    """Save DataFrame to CSV."""
    ensure_dir(path)
    df.to_csv(path, index=index)

def load_csv(path: str) -> pd.DataFrame:
    """Load DataFrame from CSV."""
    return pd.read_csv(path)


# -------------------------------------------------------------
# Pickle helpers
# -------------------------------------------------------------
def save_pickle(obj: Any, path: str):
    """Save any Python object to a pickle file."""
    ensure_dir(path)
    with open(path, "wb") as f:
        pickle.dump(obj, f)

def load_pickle(path: str) -> Any:
    """Load a Python object from a pickle file."""
    with open(path, "rb") as f:
        return pickle.load(f)


# -------------------------------------------------------------
# JSON helpers
# -------------------------------------------------------------
def save_json(data: Union[dict, list], path: str, indent: int = 2):
    """Save dict/list to JSON file."""
    ensure_dir(path)
    with open(path, "w") as f:
        json.dump(data, f, indent=indent)

def load_json(path: str) -> Union[dict, list]:
    """Load JSON file."""
    with open(path, "r") as f:
        return json.load(f)


# -------------------------------------------------------------
# Optional: timestamp helper
# -------------------------------------------------------------
import time
def timestamp() -> str:
    """Return a YYYYMMDD_HHMMSS timestamp string."""
    return time.strftime("%Y%m%d_%H%M%S")