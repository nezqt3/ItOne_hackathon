import argparse
from pathlib import Path
from .model.trainer import train
from .model.predictor import predict

def main():
    ap = argparse.ArgumentParser(description="Fraud pipeline (LightGBM) with pluggable feature engine")
    sub = ap.add_subparsers(dest="cmd", required=True)

    tr = sub.add_parser("train")
    tr.add_argument("--csv", required=True)
    tr.add_argument("--model", required=True)
    tr.add_argument("--state", required=True)
    tr.add_argument("--engine", choices=["pandas", "polars"], default="pandas")
    tr.add_argument("--fb-jobs", type=int, default=-1, help="workers for feature engineering (pandas engine only)")
    tr.add_argument("--ratio", type=int, default=2)
    tr.add_argument("--spw-cap", type=float, default=6.0)
    tr.add_argument("--strategy", choices=["budget", "constrained", "f1"], default="budget")
    tr.add_argument("--budget-rate", type=float, default=0.02)
    tr.add_argument("--min-precision", type=float, default=0.80)
    tr.add_argument("--min-recall", type=float, default=0.10)
    tr.add_argument("--precision-floor", type=float, default=0.70)
    tr.add_argument("--relax-step", type=float, default=0.02)

    pr = sub.add_parser("predict")
    pr.add_argument("--csv", required=True)
    pr.add_argument("--model", required=True)
    pr.add_argument("--state", required=True)
    pr.add_argument("--out", required=True)

    args = ap.parse_args()
    if args.cmd == "train":
        train(
            Path(args.csv),
            Path(args.model),
            Path(args.state),
            engine=args.engine,
            fb_jobs=args.fb_jobs,
            ratio=args.ratio,
            spw_cap=args.spw_cap,
            strategy=args.strategy,
            budget_rate=args.budget_rate,
            min_precision=args.min_precision,
            min_recall=args.min_recall,
            precision_floor=args.precision_floor,
            relax_step=args.relax_step,
        )
    else:
        predict(Path(args.csv), Path(args.model), Path(args.state), Path(args.out))

if __name__ == "__main__":
    main()
