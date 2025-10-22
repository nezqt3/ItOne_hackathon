# methods/fraud_pipeline/model/trainer.py
from __future__ import annotations

import os
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
import lightgbm as lgb

from sklearn.model_selection import train_test_split
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder
from sklearn.impute import SimpleImputer
from sklearn.metrics import (
    roc_auc_score,
    precision_recall_curve,
    classification_report,
    confusion_matrix,
)

from ..state import FeatureState
from ..thresholds import choose_threshold_by_budget, choose_threshold_constrained
from ..config import (
    DEFAULT_WINDOWS,
    DEFAULT_LAST_N,
    DEFAULT_BURST_MINUTES,
    DEFAULT_BURST_TXN,
    DEFAULT_BURST_UNIQ_SENDERS,
)


def make_pre(cat_cols, num_cols):
    """Колонк-процессор: числовые -> median impute, категориальные -> OHE (sparse)."""
    # sklearn>=1.2: sparse_output; старее — sparse
    try:
        ohe = OneHotEncoder(handle_unknown="ignore", sparse_output=True)
    except TypeError:
        ohe = OneHotEncoder(handle_unknown="ignore", sparse=True)

    return ColumnTransformer(
        transformers=[
            ("num", Pipeline([("imp", SimpleImputer(strategy="median"))]), num_cols),
            ("cat", Pipeline([("imp", SimpleImputer(strategy="most_frequent")),
                              ("oh", ohe)]), cat_cols),
        ],
        sparse_threshold=0.3,
        n_jobs=None,
    )


def _fit_lgbm(clf: lgb.LGBMClassifier, Xtr, ytr, Xva, yva):
    """Совместимый fit для разных версий lightgbm."""
    # Настроим «тише» вывод
    try:
        clf.set_params(verbosity=-1)
    except Exception:
        pass

    try:
        # Новые версии поддерживают callbacks
        clf.fit(
            Xtr, ytr,
            eval_set=[(Xva, yva)],
            callbacks=[lgb.early_stopping(50), lgb.log_evaluation(50)],
        )
    except TypeError:
        # Старые версии: без callbacks, используем early_stopping_rounds
        clf.fit(
            Xtr, ytr,
            eval_set=[(Xva, yva)],
            early_stopping_rounds=50,
        )
    return clf


def train(
    csv_path: Path,
    model_path: Path,
    state_path: Path,
    engine: str = "pandas",      # 'pandas' | 'polars'
    fb_jobs: int = -1,           # воркеры для pandas-фичей (joblib)
    ratio: int = 2,              # undersampling: neg = ratio * pos
    spw_cap: float = 6.0,        # cap печати для scale_pos_weight
    strategy: str = "budget",    # 'budget' | 'constrained' | 'f1'
    budget_rate: float = 0.02,   # доля алертов
    min_precision: float = 0.80,
    min_recall: float = 0.10,
    precision_floor: float = 0.70,
    relax_step: float = 0.02,
):
    print(f"CPU count: {os.cpu_count()}\nCSV: {csv_path}\nengine: {engine}\nfb_jobs: {fb_jobs}")

    # 1) читаем сырой CSV
    df_raw = pd.read_csv(csv_path)

    # 2) выбираем FeatureBuilder
    if engine == "polars":
        try:
            from ..features.polars_fb import PolarsFeatureBuilder as FB
            fb = FB(
                DEFAULT_WINDOWS,
                DEFAULT_LAST_N,
                DEFAULT_BURST_MINUTES,
                DEFAULT_BURST_TXN,
                DEFAULT_BURST_UNIQ_SENDERS,
            )
        except Exception as e:
            print(f"[warn] polars недоступен ({e}) — переключаюсь на pandas")
            from ..features.pandas_fb import PandasFeatureBuilder as FB
            fb = FB(
                DEFAULT_WINDOWS,
                DEFAULT_LAST_N,
                DEFAULT_BURST_MINUTES,
                DEFAULT_BURST_TXN,
                DEFAULT_BURST_UNIQ_SENDERS,
                n_jobs=fb_jobs,
            )
    else:
        from ..features.pandas_fb import PandasFeatureBuilder as FB
        fb = FB(
            DEFAULT_WINDOWS,
            DEFAULT_LAST_N,
            DEFAULT_BURST_MINUTES,
            DEFAULT_BURST_TXN,
            DEFAULT_BURST_UNIQ_SENDERS,
            n_jobs=fb_jobs,
        )

    # 3) фичи
    df = fb.fit_transform(df_raw)

    # 4) таргет
    if "is_fraud" not in df.columns:
        raise ValueError("CSV must contain 'is_fraud' for training.")
    y = df["is_fraud"].astype(int)

    # 5) разметка признаков
    cat_cols = [c for c in [
        "transaction_type", "merchant_category", "location",
        "device_used", "payment_channel"
    ] if c in df.columns]

    meta_cols = ["transaction_id", "timestamp", "fraud_type", "is_fraud"]
    id_text_cols = ["sender_account", "receiver_account", "device_hash", "ip_address"]
    drop_cols = set(cat_cols + meta_cols + id_text_cols)

    num_cols = [c for c in df.columns if c not in drop_cols]  # всё остальное — числа
    X = df[cat_cols + num_cols].copy()

    # 6) holdout
    X_train, X_valid, y_train, y_valid = train_test_split(
        X, y, test_size=0.2, stratify=y, random_state=42
    )

    # 7) undersampling
    tr = X_train.copy()
    tr["y"] = y_train.values
    pos_df, neg_df = tr[tr["y"] == 1], tr[tr["y"] == 0]
    neg_keep = min(len(neg_df), ratio * len(pos_df))
    if neg_keep < len(neg_df):
        neg_sampled = neg_df.sample(n=neg_keep, random_state=42)
        tr_bal = pd.concat([pos_df, neg_sampled]).sample(frac=1.0, random_state=42)
        X_train, y_train = tr_bal.drop(columns=["y"]), tr_bal["y"].astype(int)
        print(f"[undersampling] pos={len(pos_df)} neg={len(neg_df)} -> neg_sampled={neg_keep}")
    else:
        print("[undersampling] skipped")

    # 8) препроцессинг и обучение
    pre = make_pre(cat_cols, num_cols)
    X_train_t = pre.fit_transform(X_train)
    X_valid_t = pre.transform(X_valid)

    pos, neg = int(y_train.sum()), int((y_train == 0).sum())
    spw = float(min(spw_cap, neg / max(pos, 1)))
    print(f"[imbalance] pos={pos}, neg={neg}, scale_pos_weight={spw:.2f}")

    clf = lgb.LGBMClassifier(
        n_estimators=1200,
        learning_rate=0.05,
        num_leaves=64,
        subsample=0.9,
        colsample_bytree=0.9,
        n_jobs=-1,
        objective="binary",
    )
    _fit_lgbm(clf, X_train_t, y_train, X_valid_t, y_valid)

    # 9) метрики + выбор порога
    proba = clf.predict_proba(X_valid_t)[:, 1]
    roc = roc_auc_score(y_valid, proba)
    print(f"ROC AUC: {roc:.4f}")

    if strategy == "budget":
        thr = choose_threshold_by_budget(proba, budget_rate=budget_rate)
        policy = {"type": "budget", "budget_rate": float(budget_rate)}
        print(f"Chosen threshold by budget: thr={thr:.4f} (top {budget_rate*100:.3f}% alerts)")
    elif strategy == "constrained":
        thr, meta = choose_threshold_constrained(
            y_valid, proba, min_precision, min_recall, relax_step, precision_floor
        )
        policy = {
            "type": "constrained",
            "min_precision": float(min_precision),
            "min_recall": float(min_recall),
            "precision_floor": float(precision_floor),
            "relax_step": float(relax_step),
            "note": meta.get("note", ""),
        }
        print(
            f"Chosen threshold (constrained): thr={thr:.4f} | precision={meta['precision']:.3f} recall={meta['recall']:.3f} ({meta['note']})"
        )
    else:
        P, R, T = precision_recall_curve(y_valid, proba)
        P, R, T = P[:-1], R[:-1], T
        f1 = (2 * P * R) / (P + R + 1e-12)
        i = int(np.argmax(f1)) if len(f1) else -1
        thr = float(T[i]) if len(T) else 0.5
        policy = {"type": "f1"}
        print(f"Chosen threshold by best F1: thr={thr:.4f}")

    y_pred = (proba >= thr).astype(int)
    print(classification_report(y_valid, y_pred))
    print("Confusion:\n", confusion_matrix(y_valid, y_pred))

    # 10) упаковка артефакта
    pipe = Pipeline([("pre", pre), ("clf", clf)])
    artifact = {
        "pipeline": pipe,
        "version": f"lgbm_{engine}_v2_parallel",
        "decision_threshold": float(thr),
        "threshold_policy": policy,
        "cat_cols": cat_cols,
        "num_cols": num_cols,
        "feature_builder": {
            "engine": engine,
            "fb_jobs": fb_jobs,
            "time_windows": list(DEFAULT_WINDOWS),
            "rolling_last_n": DEFAULT_LAST_N,
            "burst_T_minutes": DEFAULT_BURST_MINUTES,
            "burst_min_txn": DEFAULT_BURST_TXN,
            "burst_min_unique_senders": DEFAULT_BURST_UNIQ_SENDERS,
        },
    }
    joblib.dump(artifact, model_path)
    print(f"Saved model -> {model_path}")

    # 11) прогрев состояния фичей (для онлайн-«новизны»)
    st = FeatureState()
    for _, r in df.sort_values("timestamp").iterrows():
        st.update_seen(
            str(r["sender_account"]),
            str(r["receiver_account"]),
            str(r.get("device_hash", "")),
            str(r.get("ip_address", "")),
        )
    st.save(state_path)
    print(f"Saved feature-state -> {state_path}")
