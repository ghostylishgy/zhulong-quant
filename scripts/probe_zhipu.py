#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import time
from pathlib import Path

import requests
from dotenv import load_dotenv


API_URL = 'https://open.bigmodel.cn/api/paas/v4/chat/completions'
MODEL = 'glm-4.7'
TOTAL_CALLS = 5
TIMEOUT_SECONDS = 60


def _load_env() -> None:
    project_root = Path(__file__).resolve().parents[1]
    env_path = project_root / '.env'
    if env_path.exists():
        load_dotenv(env_path, override=False)


def _build_long_prompt() -> str:
    fake_news = (
        '【RAG情报注入】行业景气、订单增速、估值切换、资金博弈、政策催化、'
        '股东行为、舆情扰动、上下游传导、产能利用率、库存周期。'
    )
    return ('你是审计助手，请仅回复JSON。\n' + fake_news * 220)[:7000]


def main() -> None:
    _load_env()
    key = os.getenv('ZHIPU_API_KEY', '').strip()
    if not key:
        print('ZHIPU_API_KEY missing')
        return

    headers = {
        'Authorization': f'Bearer {key}',
        'Content-Type': 'application/json',
    }
    payload = {
        'model': MODEL,
        'messages': [{'role': 'user', 'content': _build_long_prompt()}],
        'max_tokens': 128,
        'temperature': 0.1,
    }

    print(f'probe start: total_calls={TOTAL_CALLS} model={MODEL}')
    for i in range(1, TOTAL_CALLS + 1):
        print(f'\n--- call {i}/{TOTAL_CALLS} ---')
        try:
            resp = requests.post(API_URL, headers=headers, json=payload, timeout=TIMEOUT_SECONDS)
            print(f'status={resp.status_code}')
            try:
                body_obj = resp.json()
                print('response.json:')
                print(json.dumps(body_obj, ensure_ascii=False, indent=2))
            except Exception:
                print('response.text:')
                print(resp.text)
            if resp.status_code == 429:
                print('hit_429=true')
        except Exception as exc:
            print(f'request_error={type(exc).__name__}: {exc}')
        time.sleep(0.2)


if __name__ == '__main__':
    main()
