from __future__ import annotations
import numpy as np
import pandas as pd
from ..config import RAW_COLS

def to_bool01(x):
    if pd.isna(x): return np.nan
    s = str(x).strip().lower()
    if s in {"1","true","t","yes","y"}: return 1
    if s in {"0","false","f","no","n"}: return 0
    return np.nan

class IFeatureBuilder:
    """Интерфейс: обе реализации должны иметь одинаковые методы."""
    def fit_transform(self, df_raw: pd.DataFrame) -> pd.DataFrame: ...
    def transform_with_state(self, df_raw: pd.DataFrame, state) -> pd.DataFrame: ...
