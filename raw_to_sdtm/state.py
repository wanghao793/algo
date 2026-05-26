from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal

import pandas as pd

MappingType = Literal["direct", "logical", "complex", "unknown"]


@dataclass
class MappingContext:
    """All state for a single raw→SDTM variable mapping session."""

    raw_column: str
    raw_data: list[dict]
    sdtm_variable: str
    sdtm_domain: str
    # spec keys: variable, type (Char|Num), label, codelist, required, length, description
    sdtm_spec: dict

    human_rules: list[str] = field(default_factory=list)
    verify_failures: int = 0
    attempt_history: list[dict] = field(default_factory=list)
    mapping_type: MappingType = "unknown"

    @property
    def raw_col_info(self) -> dict:
        s = pd.DataFrame(self.raw_data)[self.raw_column]
        vc: dict = {}
        if s.nunique() <= 30:
            vc = {str(k): int(v) for k, v in s.value_counts().items()}
        return {
            "name": self.raw_column,
            "dtype": str(s.dtype),
            "n_rows": len(s),
            "n_nulls": int(s.isna().sum()),
            "n_unique": int(s.nunique()),
            "sample": s.dropna().head(10).tolist(),
            "value_counts": vc,
        }
