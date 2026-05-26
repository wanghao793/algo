from __future__ import annotations

import builtins
import re
import traceback

import numpy as np
import pandas as pd

_SAFE_BUILTINS: dict = {
    k: getattr(builtins, k)
    for k in [
        "len", "range", "str", "int", "float", "bool", "list", "dict",
        "set", "tuple", "isinstance", "round", "abs", "enumerate", "zip",
        "map", "filter", "sorted", "min", "max", "sum", "any", "all", "print",
    ]
    if hasattr(builtins, k)
}
_SAFE_BUILTINS.update({"None": None, "True": True, "False": False})


def run_transform(code: str, raw_data: list[dict]) -> dict:
    """
    Execute agent-generated Python code in a restricted sandbox.
    Code accesses raw data as `df` and must assign output to `result`.
    """
    df = pd.DataFrame(raw_data)
    env: dict = {
        "__builtins__": _SAFE_BUILTINS,
        "pd": pd,
        "np": np,
        "re": re,
        "df": df,
    }
    try:
        exec(compile(code, "<sdtm_transform>", "exec"), env)
    except Exception as exc:
        return {
            "success": False,
            "error": f"{type(exc).__name__}: {exc}",
            "traceback": traceback.format_exc(),
        }

    result = env.get("result")
    if result is None:
        return {"success": False, "error": "Code must assign output to 'result'"}

    series = pd.Series(result, index=df.index)
    return {
        "success": True,
        "values": series.tolist(),
        "n_rows": len(series),
        "n_nulls": int(series.isna().sum()),
        "sample": series.dropna().head(10).tolist(),
        "dtype": str(series.dtype),
    }
