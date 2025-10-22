from __future__ import annotations
from typing import Dict, Set, Tuple, Optional
from pathlib import Path
import joblib

class FeatureState:
    """Тёплое состояние для «новизны» (для онлайна/батчей)."""
    def __init__(self):
        self.sender_receivers: Dict[str, Set[str]] = {}
        self.sender_devices: Dict[str, Set[str]] = {}
        self.sender_ips: Dict[str, Set[str]] = {}

    @staticmethod
    def load(path: Optional[Path]) -> "FeatureState":
        st = FeatureState()
        if path and Path(path).exists():
            blob = joblib.load(path)
            st.sender_receivers = {k: set(v) for k, v in blob.get("sender_receivers", {}).items()}
            st.sender_devices   = {k: set(v) for k, v in blob.get("sender_devices", {}).items()}
            st.sender_ips       = {k: set(v) for k, v in blob.get("sender_ips", {}).items()}
        return st

    def save(self, path: Path):
        blob = {
            "sender_receivers": {k: list(v) for k, v in self.sender_receivers.items()},
            "sender_devices":   {k: list(v) for k, v in self.sender_devices.items()},
            "sender_ips":       {k: list(v) for k, v in self.sender_ips.items()},
        }
        joblib.dump(blob, path)

    def update_seen(self, sender: str, receiver: str, device: str, ip: str):
        if not sender: return
        self.sender_receivers.setdefault(sender, set()).add(receiver)
        if device: self.sender_devices.setdefault(sender, set()).add(device)
        if ip:     self.sender_ips.setdefault(sender, set()).add(ip)

    def check_news(self, sender: str, receiver: str, device: str, ip: str) -> Tuple[int,int,int]:
        is_new_receiver = 0 if (sender in self.sender_receivers and receiver in self.sender_receivers[sender]) else 1
        is_new_device   = 0 if (sender in self.sender_devices   and device   in self.sender_devices[sender])   else 1
        is_new_ip       = 0 if (sender in self.sender_ips       and ip       in self.sender_ips[sender])       else 1
        return is_new_receiver, is_new_device, is_new_ip
