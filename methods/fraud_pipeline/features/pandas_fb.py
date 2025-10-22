from __future__ import annotations

import numpy as np
import pandas as pd
from typing import List
from joblib import Parallel, delayed

from .base import IFeatureBuilder, to_bool01
from ..config import (
    RAW_COLS,
    DEFAULT_WINDOWS,
    DEFAULT_LAST_N,
    DEFAULT_BURST_MINUTES,
    DEFAULT_BURST_TXN,
    DEFAULT_BURST_UNIQ_SENDERS,
)

# ---------- быстрые sliding-хелперы (numpy-сканеры) ----------
def _parse_win_to_sec(w: str) -> int:
    w = w.strip().lower()
    if w.endswith("h"):
        return int(w[:-1]) * 3600
    if w.endswith("min"):
        return int(w[:-3]) * 60
    if w.endswith("m"):
        return int(w[:-1]) * 60
    if w.endswith("d"):
        return int(w[:-1]) * 86400
    raise ValueError(f"Unsupported window: {w}")

def _ts_seconds(series: pd.Series) -> np.ndarray:
    # без .view — совместимо с pandas 2.x
    return series.astype("int64", copy=False).to_numpy() // 10**9

def _sliding_count(g: pd.DataFrame, win_seconds: int) -> np.ndarray:
    ts = _ts_seconds(g["timestamp"])
    n = len(g)
    out = np.zeros(n, dtype=np.int32)
    from collections import deque
    dq = deque()
    j = 0
    for i in range(n):
        cur = ts[i]
        dq.append(i)
        while j <= i and cur - ts[j] > win_seconds:
            j += 1
            while dq and dq[0] < j:
                dq.popleft()
        out[i] = len(dq)
    return out

def _sliding_sum_amount(g: pd.DataFrame, win_seconds: int) -> np.ndarray:
    ts = _ts_seconds(g["timestamp"])
    vals = g["amount"].to_numpy(dtype=float)
    n = len(g)
    out = np.zeros(n, dtype=float)
    from collections import deque
    dq = deque()
    j = 0
    acc = 0.0
    for i in range(n):
        cur = ts[i]
        dq.append(i)
        acc += vals[i]
        while j <= i and cur - ts[j] > win_seconds:
            acc -= vals[j]
            j += 1
            while dq and dq[0] < j:
                dq.popleft()
        out[i] = acc
    return out

def _sliding_unique(g: pd.DataFrame, key_col: str, win_seconds: int) -> np.ndarray:
    ts = _ts_seconds(g["timestamp"])
    keys = g[key_col].to_numpy()
    n = len(g)
    out = np.zeros(n, dtype=np.int32)
    from collections import defaultdict, deque
    dq = deque()
    freq = defaultdict(int)
    j = 0
    for i in range(n):
        cur = ts[i]
        dq.append(i)
        freq[keys[i]] += 1
        while j <= i and cur - ts[j] > win_seconds:
            freq[keys[j]] -= 1
            if freq[keys[j]] == 0:
                del freq[keys[j]]
            j += 1
            while dq and dq[0] < j:
                dq.popleft()
        out[i] = len(freq)
    return out


class PandasFeatureBuilder(IFeatureBuilder):
    """
    Та же логика, что была, но отправка тяжёлых окон по группам в joblib.Parallel (процессы).
    """

    def __init__(
        self,
        time_windows=DEFAULT_WINDOWS,
        rolling_last_n=DEFAULT_LAST_N,
        burst_T_minutes=DEFAULT_BURST_MINUTES,
        burst_min_txn=DEFAULT_BURST_TXN,
        burst_min_unique_senders=DEFAULT_BURST_UNIQ_SENDERS,
        n_jobs: int = -1,
    ):
        self.time_windows = time_windows
        self.rolling_last_n = rolling_last_n
        self.burst_T_minutes = burst_T_minutes
        self.burst_min_txn = burst_min_txn
        self.burst_min_unique_senders = burst_min_unique_senders
        self.n_jobs = n_jobs

    # -------- базовая очистка --------
    def _base_clean(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        for c in RAW_COLS:
            if c not in df.columns:
                df[c] = np.nan

        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
        before = len(df)
        df = df[~df["timestamp"].isna()].copy()
        if len(df) < before:
            print(f"[clean] dropped {before - len(df)} rows with NaT timestamp")

        df = df.sort_values(["sender_account", "timestamp"]).reset_index(drop=True)

        for c in (
            "amount",
            "time_since_last_transaction",
            "spending_deviation_score",
            "velocity_score",
            "geo_anomaly_score",
        ):
            df[c] = pd.to_numeric(df[c], errors="coerce")

        if "is_fraud" in df.columns:
            df["is_fraud"] = df["is_fraud"].apply(to_bool01).astype("Int64")

        for c in (
            "sender_account",
            "receiver_account",
            "device_hash",
            "ip_address",
            "transaction_type",
            "merchant_category",
            "location",
            "device_used",
            "payment_channel",
            "transaction_id",
            "fraud_type",
        ):
            df[c] = df[c].fillna("").astype(str)

        if df["transaction_id"].eq("").any():
            df["transaction_id"] = (
                df["sender_account"].astype(str)
                + "_"
                + df["timestamp"].astype("int64", copy=False).astype(str)
            )

        # частоты id (компактнее OHE для high-card)
        for idc in ("sender_account", "receiver_account"):
            vc = df[idc].astype(str).value_counts(dropna=False)
            freq_map = (vc / len(df)).astype(float)
            df[f"{idc}_freq"] = df[idc].astype(str).map(freq_map).astype(float)

        return df

    # -------- темпоральные --------
    def _temporal(self, df: pd.DataFrame) -> pd.DataFrame:
        df["hour"] = df["timestamp"].dt.hour
        df["day_of_week"] = df["timestamp"].dt.dayofweek
        df["is_night"] = ((df["hour"] < 6) | (df["hour"] >= 23)).astype(int)
        df["is_weekend"] = (df["day_of_week"] >= 5).astype(int)
        df = df.sort_values(["sender_account", "timestamp"]).reset_index(drop=True)
        df["time_diff_prev_sec"] = (
            df.groupby("sender_account")["timestamp"]
            .diff()
            .dt.total_seconds()
            .fillna(999999)
        )
        return df

    # -------- sender (параллельно по sender_account) --------
    def _sender_feats(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.sort_values(["sender_account", "timestamp"]).reset_index(drop=True)

        parts: List[pd.DataFrame] = [g for _, g in df.groupby("sender_account", sort=False)]

        for w in self.time_windows:
            sec = _parse_win_to_sec(w)

            def _one_sender(g: pd.DataFrame) -> pd.DataFrame:
                s_cnt = pd.Series(_sliding_count(g, sec), index=g.index, name=f"sender_txn_count_{w}")
                s_sum = pd.Series(_sliding_sum_amount(g, sec), index=g.index, name=f"sender_amount_sum_{w}")
                return pd.concat([s_cnt, s_sum], axis=1)

            res = Parallel(n_jobs=self.n_jobs, backend="loky")(delayed(_one_sender)(g) for g in parts)
            block = pd.concat(res).sort_index()
            df[f"sender_txn_count_{w}"] = block[f"sender_txn_count_{w}"].to_numpy()
            df[f"sender_amount_sum_{w}"] = block[f"sender_amount_sum_{w}"].to_numpy()

        # rolling по последним N — оставим последовательным (он быстрый)
        df["sender_avg_amount_lastN"] = (
            df.groupby("sender_account")["amount"]
            .transform(lambda s: s.rolling(self.rolling_last_n, min_periods=1).mean())
        )
        df["sender_std_amount_lastN"] = (
            df.groupby("sender_account")["amount"]
            .transform(lambda s: s.rolling(self.rolling_last_n, min_periods=1).std().fillna(0))
        )
        df["amount_dev_from_sender_mean"] = df["amount"] / (
            df["sender_avg_amount_lastN"] + 1e-6
        )

        df["is_new_receiver_in_batch"] = (
            df.groupby(["sender_account", "receiver_account"]).cumcount() == 0
        ).astype(int)
        return df

    # -------- receiver (параллельно по receiver_account) --------
    def _receiver_feats(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.sort_values(["receiver_account", "timestamp"]).reset_index(drop=True)
        base_sec = _parse_win_to_sec(self.time_windows[0])
        burst_sec = self.burst_T_minutes * 60

        parts: List[pd.DataFrame] = [g for _, g in df.groupby("receiver_account", sort=False)]

        def _one_receiver(g: pd.DataFrame) -> pd.DataFrame:
            r_cnt_base = pd.Series(
                _sliding_count(g, base_sec), index=g.index, name="receiver_txn_count_base"
            )
            r_uni_base = pd.Series(
                _sliding_unique(g, "sender_account", base_sec),
                index=g.index,
                name="receiver_unique_senders_base",
            )
            r_cnt_burst = _sliding_count(g, burst_sec)
            r_uni_burst = _sliding_unique(g, "sender_account", burst_sec)
            r_burst = pd.Series(
                ((r_cnt_burst >= self.burst_min_txn) & (r_uni_burst >= self.burst_min_unique_senders)).astype(int),
                index=g.index,
                name="receiver_burst_flag",
            )
            return pd.concat([r_cnt_base, r_uni_base, r_burst], axis=1)

        res = Parallel(n_jobs=self.n_jobs, backend="loky")(delayed(_one_receiver)(g) for g in parts)
        block = pd.concat(res).sort_index()

        df[f"receiver_txn_count_{self.time_windows[0]}"] = block["receiver_txn_count_base"].to_numpy()
        df[f"receiver_unique_senders_{self.time_windows[0]}"] = block["receiver_unique_senders_base"].to_numpy()
        df["receiver_burst_flag"] = block["receiver_burst_flag"].to_numpy()
        return df

    # -------- device/ip (параллельно по sender_account) --------
    def _device_ip(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.sort_values(["sender_account", "timestamp"]).reset_index(drop=True)
        sec = 24 * 3600
        parts: List[pd.DataFrame] = [g for _, g in df.groupby("sender_account", sort=False)]

        def _one_sender_uniq(g: pd.DataFrame) -> pd.DataFrame:
            dev = pd.Series(
                _sliding_unique(g, "device_hash", sec),
                index=g.index,
                name="sender_unique_devices_24h",
            )
            ip = pd.Series(
                _sliding_unique(g, "ip_address", sec),
                index=g.index,
                name="sender_unique_ips_24h",
            )
            return pd.concat([dev, ip], axis=1)

        res = Parallel(n_jobs=self.n_jobs, backend="loky")(delayed(_one_sender_uniq)(g) for g in parts)
        block = pd.concat(res).sort_index()
        df["sender_unique_devices_24h"] = block["sender_unique_devices_24h"].to_numpy()
        df["sender_unique_ips_24h"] = block["sender_unique_ips_24h"].to_numpy()
        return df

    # -------- публичное API --------
    def fit_transform(self, df_raw: pd.DataFrame) -> pd.DataFrame:
        df = self._base_clean(df_raw)
        df = self._temporal(df)
        df = self._sender_feats(df)
        df = self._receiver_feats(df)
        df = self._device_ip(df)
        return df

    def transform_with_state(self, df_raw: pd.DataFrame, state) -> pd.DataFrame:
        df = self._base_clean(df_raw).sort_values("timestamp").reset_index(drop=True)
        news = df.apply(
            lambda r: state.check_news(
                r["sender_account"], r["receiver_account"], r["device_hash"], r["ip_address"]
            ),
            axis=1,
            result_type="expand",
        )
        df[["is_new_receiver_state", "is_new_device_state", "is_new_ip_state"]] = news
        df = self._temporal(df)
        df = self._sender_feats(df)
        df = self._receiver_feats(df)
        df = self._device_ip(df)
        for _, r in df.iterrows():
            state.update_seen(
                str(r["sender_account"]),
                str(r["receiver_account"]),
                str(r.get("device_hash", "")),
                str(r.get("ip_address", "")),
            )
        return df
