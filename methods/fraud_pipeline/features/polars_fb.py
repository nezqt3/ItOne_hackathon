# methods/fraud_pipeline/features/polars_fb.py
from __future__ import annotations
import numpy as np
import pandas as pd
from typing import Optional
import polars as pl

from .base import IFeatureBuilder
from ..config import (
    RAW_COLS,
    DEFAULT_WINDOWS,
    DEFAULT_LAST_N,
    DEFAULT_BURST_MINUTES,
    DEFAULT_BURST_TXN,
    DEFAULT_BURST_UNIQ_SENDERS,
)

# ========= вспомогалки, как в pandas_fb =========

def _parse_win_to_sec(w: str) -> int:
    w = w.strip().lower()
    if w.endswith("h"):   return int(w[:-1]) * 3600
    if w.endswith("min"): return int(w[:-3]) * 60
    if w.endswith("m"):   return int(w[:-1]) * 60
    if w.endswith("d"):   return int(w[:-1]) * 86400
    raise ValueError(f"Unsupported window: {w}")

def _ts_seconds(series: pd.Series) -> np.ndarray:
    return series.astype("int64", copy=False).to_numpy() // 10**9

def _sliding_count(group_df: pd.DataFrame, win_seconds: int) -> np.ndarray:
    ts = _ts_seconds(group_df["timestamp"])
    n = len(group_df)
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

def _sliding_sum_amount(group_df: pd.DataFrame, win_seconds: int) -> np.ndarray:
    ts = _ts_seconds(group_df["timestamp"])
    vals = group_df["amount"].to_numpy(dtype=float)
    n = len(group_df)
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

def _sliding_unique(group_df: pd.DataFrame, key_col: str, win_seconds: int) -> np.ndarray:
    ts = _ts_seconds(group_df["timestamp"])
    keys = group_df[key_col].to_numpy()
    n = len(group_df)
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


# ========= Polars FeatureBuilder =========

class PolarsFeatureBuilder(IFeatureBuilder):
    """
    Логика признаков идентична pandas_fb:
      - темпоральные признаки
      - sender: count/sum в окнах, rolling avg/std, dev от среднего, new-receiver-in-batch
      - receiver: count и unique senders в окне + burst-флаг за T минут
      - device/ip: unique за 24h
      - freq-encoding для sender/receiver
      - «новизна» снаружи (в transform_with_state)
    Polars используется для быстрой очистки/типизации и freq-encoding (векторно).
    Сами скользящие окна считаются быстрыми numpy-сканерами, чтобы строго сохранить
    семантику pandas-версии и избежать нестабильностей rolling в Polars.
    """

    def __init__(
        self,
        time_windows=DEFAULT_WINDOWS,
        rolling_last_n=DEFAULT_LAST_N,
        burst_T_minutes=DEFAULT_BURST_MINUTES,
        burst_min_txn=DEFAULT_BURST_TXN,
        burst_min_unique_senders=DEFAULT_BURST_UNIQ_SENDERS,
    ):
        self.time_windows = time_windows
        self.rolling_last_n = rolling_last_n
        self.burst_T_minutes = burst_T_minutes
        self.burst_min_txn = burst_min_txn
        self.burst_min_unique_senders = burst_min_unique_senders

    # ---------- базовая очистка в Polars → pandas ----------
    def _base_clean_to_pandas(self, df_pd: pd.DataFrame) -> pd.DataFrame:
        # добьём недостающие столбцы
        for c in RAW_COLS:
            if c not in df_pd.columns:
                df_pd[c] = np.nan

        df = pl.from_pandas(df_pd, include_index=False)

        # timestamp
        df = (
            df.with_columns(pl.col("timestamp").str.strptime(pl.Datetime, strict=False))
              .drop_nulls("timestamp")
        )

        # числовые
        num_cols = [
            "amount",
            "time_since_last_transaction",
            "spending_deviation_score",
            "velocity_score",
            "geo_anomaly_score",
        ]
        df = df.with_columns([pl.col(c).cast(pl.Float64, strict=False) for c in num_cols])

        # строки
        str_cols = [
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
        ]
        for c in str_cols:
            df = df.with_columns(pl.col(c).cast(pl.Utf8, strict=False).fill_null(""))

        # сортировка (sender, timestamp)
        df = df.sort(by=["sender_account", "timestamp"])

        # surrogate id, если пусто
        df = df.with_columns(
            pl.when((pl.col("transaction_id") == "") | (pl.col("transaction_id").is_null()))
            .then(
                pl.concat_str(
                    [
                        pl.col("sender_account"),
                        pl.lit("_"),
                        pl.col("timestamp").cast(pl.Int64).cast(pl.Utf8),
                    ]
                )
            )
            .otherwise(pl.col("transaction_id"))
            .alias("transaction_id")
        )

        # is_fraud -> Int64 (0/1), если присутствует
        if "is_fraud" in df.columns:
            df = df.with_columns(
                pl.when(
                    pl.col("is_fraud")
                    .cast(pl.Utf8)
                    .str.to_lowercase()
                    .is_in(["1", "true", "t", "yes", "y"])
                )
                .then(1)
                .when(
                    pl.col("is_fraud")
                    .cast(pl.Utf8)
                    .str.to_lowercase()
                    .is_in(["0", "false", "f", "no", "n"])
                )
                .then(0)
                .otherwise(None)
                .cast(pl.Int64)
                .alias("is_fraud")
            )

        # freq-encoding на стороне Polars (быстро и без Python-циклов)
        for idc in ("sender_account", "receiver_account"):
            vc = (
                df.group_by(idc)
                .len()
                .with_columns((pl.col("len") / df.height).alias("freq"))
                .select(idc, "freq")
            )
            df = df.join(vc, on=idc, how="left").rename({"freq": f"{idc}_freq"})

        # назад в pandas для дальнейших numpy-сканеров (семантика как в pandas_fb)
        pdf = df.to_pandas()

        # приведение типов для совместимости с downstream
        pdf["timestamp"] = pd.to_datetime(pdf["timestamp"])
        if "is_fraud" in pdf.columns:
            pdf["is_fraud"] = pdf["is_fraud"].astype("Int64")

        return pdf

    # ---------- temportal ----------
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

    # ---------- sender ----------
    def _sender_feats(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.sort_values(["sender_account", "timestamp"]).reset_index(drop=True)
        for w in self.time_windows:
            sec = _parse_win_to_sec(w)
            df[f"sender_txn_count_{w}"] = (
                df.groupby("sender_account", group_keys=False)
                .apply(lambda g: pd.Series(_sliding_count(g, sec), index=g.index))
                .reset_index(level=0, drop=True)
                .to_numpy()
            )
            df[f"sender_amount_sum_{w}"] = (
                df.groupby("sender_account", group_keys=False)
                .apply(lambda g: pd.Series(_sliding_sum_amount(g, sec), index=g.index))
                .reset_index(level=0, drop=True)
                .to_numpy()
            )

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

    # ---------- receiver ----------
    def _receiver_feats(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.sort_values(["receiver_account", "timestamp"]).reset_index(drop=True)
        base_sec = _parse_win_to_sec(self.time_windows[0])
        burst_sec = self.burst_T_minutes * 60

        def _grp(g: pd.DataFrame) -> pd.DataFrame:
            cnt_base = _sliding_count(g, base_sec)
            uniq_base = _sliding_unique(g, "sender_account", base_sec)
            cnt_burst = _sliding_count(g, burst_sec)
            uniq_burst = _sliding_unique(g, "sender_account", burst_sec)
            return pd.DataFrame(
                {
                    "receiver_txn_count_base": cnt_base,
                    "receiver_unique_senders_base": uniq_base,
                    "receiver_burst_flag": (
                        (cnt_burst >= self.burst_min_txn)
                        & (uniq_burst >= self.burst_min_unique_senders)
                    ).astype(int),
                },
                index=g.index,
            )

        tmp = df.groupby("receiver_account", group_keys=False).apply(_grp)
        df[f"receiver_txn_count_{self.time_windows[0]}"] = (
            tmp["receiver_txn_count_base"].to_numpy()
        )
        df[f"receiver_unique_senders_{self.time_windows[0]}"] = (
            tmp["receiver_unique_senders_base"].to_numpy()
        )
        df["receiver_burst_flag"] = tmp["receiver_burst_flag"].to_numpy()
        return df

    # ---------- device/ip ----------
    def _device_ip_feats(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.sort_values(["sender_account", "timestamp"]).reset_index(drop=True)
        sec = 24 * 3600

        def _uniq(g: pd.DataFrame, col: str) -> np.ndarray:
            return _sliding_unique(g, col, sec)

        dev = (
            df.groupby("sender_account", group_keys=False)
            .apply(lambda g: pd.Series(_uniq(g, "device_hash"), index=g.index))
            .reset_index(level=0, drop=True)
            .to_numpy()
        )
        ip = (
            df.groupby("sender_account", group_keys=False)
            .apply(lambda g: pd.Series(_uniq(g, "ip_address"), index=g.index))
            .reset_index(level=0, drop=True)
            .to_numpy()
        )
        df["sender_unique_devices_24h"] = dev
        df["sender_unique_ips_24h"] = ip
        return df

    # ---------- public ----------
    def fit_transform(self, df_raw: pd.DataFrame) -> pd.DataFrame:
        df = self._base_clean_to_pandas(df_raw)
        df = self._temporal(df)
        df = self._sender_feats(df)
        df = self._receiver_feats(df)
        df = self._device_ip_feats(df)
        return df

    def transform_with_state(self, df_raw: pd.DataFrame, state) -> pd.DataFrame:
        df = self._base_clean_to_pandas(df_raw).sort_values("timestamp").reset_index(drop=True)

        # новизна до обновления state
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
        df = self._device_ip_feats(df)

        # обновляем state после расчёта признаков
        for _, r in df.iterrows():
            state.update_seen(
                str(r["sender_account"]),
                str(r["receiver_account"]),
                str(r.get("device_hash", "")),
                str(r.get("ip_address", "")),
            )
        return df
