from __future__ import annotations

import json
import os
import urllib.request
from pathlib import Path

from dotenv import load_dotenv


def _load_env() -> None:
    root = Path(__file__).resolve().parents[1]
    for env_file in (root / ".env", Path.cwd() / ".env"):
        if env_file.exists():
            load_dotenv(env_file, override=False)


def send_push(title: str, content: str, template: str = "txt", timeout: int = 10) -> bool:
    _load_env()
    token = os.getenv("PUSHPLUS_TOKEN", "").strip()
    if not token:
        return False

    payload = {
        "token": token,
        "title": title,
        "content": content,
        "template": template,
    }
    data = json.dumps(payload).encode("utf-8")

    req = urllib.request.Request(
        url=os.getenv("PUSHPLUS_URL", "https://www.pushplus.plus/send"),
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8", errors="ignore")
        body = json.loads(raw)
        return int(body.get("code", 0)) == 200
    except Exception:
        return False
