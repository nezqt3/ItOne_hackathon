import numpy as np
from sklearn.metrics import precision_recall_curve

def choose_threshold_by_budget(proba, budget_rate=0.002) -> float:
    budget_rate = max(1e-6, min(0.5, float(budget_rate)))
    q = 1.0 - budget_rate
    return float(np.quantile(proba, q))

def choose_threshold_constrained(y_true, proba,
                                 min_precision=0.90, min_recall=0.05,
                                 relax_step=0.05, precision_floor=0.70):
    prec, rec, thr = precision_recall_curve(y_true, proba)
    P, R, T = prec[:-1], rec[:-1], thr
    p = float(min_precision)
    while p >= float(precision_floor):
        mask = (P >= p) & (R >= min_recall)
        if mask.any():
            i = int(np.argmax(R[mask])); return float(T[mask][i]), {"precision": float(P[mask][i]), "recall": float(R[mask][i]), "note": f"precision>={p:.2f}, recall>={min_recall:.2f}"}
        p -= relax_step
    f1 = (2*P*R)/(P+R+1e-12)
    if len(f1): i = int(np.argmax(f1)); return float(T[i]), {"precision": float(P[i]), "recall": float(R[i]), "note": "fallback: best F1"}
    return 0.99, {"precision":0.0,"recall":0.0,"note":"fallback"}
