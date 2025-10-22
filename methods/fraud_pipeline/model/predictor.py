from __future__ import annotations
from pathlib import Path
import joblib
import pandas as pd
from ..state import FeatureState
from ..config import DEFAULT_WINDOWS, DEFAULT_LAST_N, DEFAULT_BURST_MINUTES, DEFAULT_BURST_TXN, DEFAULT_BURST_UNIQ_SENDERS

def predict(csv_path: Path, model_path: Path, state_path: Path, out_path: Path):
    print(f"Loading model: {model_path}")
    bundle = joblib.load(model_path)
    pipe = bundle["pipeline"]; thr = bundle.get("decision_threshold", 0.5)

    fb_conf = bundle.get("feature_builder", {})
    engine = fb_conf.get("engine", "pandas")

    if engine == "polars":
        try:
            from ..features.polars_fb import PolarsFeatureBuilder as FB
        except Exception:
            print("[warn] polars недоступен — переключаюсь на pandas")
            from ..features.pandas_fb import PandasFeatureBuilder as FB
    else:
        from ..features.pandas_fb import PandasFeatureBuilder as FB

    fb = FB(fb_conf.get("time_windows", DEFAULT_WINDOWS),
            fb_conf.get("rolling_last_n", DEFAULT_LAST_N),
            fb_conf.get("burst_T_minutes", DEFAULT_BURST_MINUTES),
            fb_conf.get("burst_min_txn", DEFAULT_BURST_TXN),
            fb_conf.get("burst_min_unique_senders", DEFAULT_BURST_UNIQ_SENDERS))

    df_raw = pd.read_csv(csv_path)
    state = FeatureState.load(state_path)
    df_feat = fb.transform_with_state(df_raw, state=state)

    cat_cols = bundle["cat_cols"]; num_cols = bundle["num_cols"]
    X = df_feat[cat_cols + num_cols].copy()
    proba = pipe.predict_proba(X)[:,1]; pred = (proba >= thr).astype(int)

    out = df_raw.copy()
    out["fraud_proba"] = proba; out["fraud_pred"] = pred; out["decision_threshold"] = thr
    out.to_csv(out_path, index=False)
    print(f"Saved predictions -> {out_path}")

    state.save(state_path); print(f"Updated feature-state -> {state_path}")
