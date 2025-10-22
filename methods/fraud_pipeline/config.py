from pathlib import Path
import os

RAW_COLS = [
    "transaction_id","timestamp","sender_account","receiver_account","amount",
    "transaction_type","merchant_category","location","device_used","is_fraud","fraud_type",
    "time_since_last_transaction","spending_deviation_score","velocity_score","geo_anomaly_score",
    "payment_channel","ip_address","device_hash"
]

# стараемся использовать все ядра
os.environ.setdefault("OMP_NUM_THREADS", str(os.cpu_count() or 8))

# дефолты фичей
DEFAULT_WINDOWS = ("1h", "24h")
DEFAULT_LAST_N = 5
DEFAULT_BURST_MINUTES = 30
DEFAULT_BURST_TXN = 5
DEFAULT_BURST_UNIQ_SENDERS = 5
