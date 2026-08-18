#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Sandbox test: verify analyze_hold_logic PARSE_FAIL logs head/tail snapshots.
No real API calls.
"""

import sys
import traceback
from pathlib import Path


def main() -> int:
    root = Path(__file__).resolve().parents[1]
    sys.path.append(str(root / '02_brain'))
    sys.path.append(str(root))

    import decision_engine as de

    captured = []

    def capture_log(msg: str, layer: str = 'SYS', level: str = 'INFO'):
        captured.append((msg, layer, level))
        print(f'[{layer}|{level}] {msg}')

    de.log = capture_log

    head_marker = 'HEAD_MARKER_' + ('A' * 120)
    tail_marker = ('Z' * 120) + '_TAIL_MARKER'
    broken = head_marker + '"broken_json": [1, 2,}' + tail_marker

    class FakeResp:
        def __init__(self, txt: str):
            self.status_code = 200
            self.text = txt
            self.content = b'nonempty'

        def json(self):
            raise ValueError('intentional broken json')

    def fake_api_call_with_retry(*args, **kwargs):
        return FakeResp(broken)

    de.api_call_with_retry = fake_api_call_with_retry

    court = de.L4SupremeCourt()
    court.zhipu_key = 'sandbox_key'

    result = court.analyze_hold_logic('300502.SZ', {'reasoning': 'bull'}, {'reasoning': 'bear'})
    print('RESULT=', result)

    assert isinstance(result, dict)
    assert result.get('error_type') == 'PARSE_FAIL'
    assert 'raw_head' in result and 'raw_tail' in result
    assert len(result.get('raw_head', '')) <= 100
    assert len(result.get('raw_tail', '')) <= 100

    log_text = '\n'.join(msg for msg, _, _ in captured)
    assert 'head100=' in log_text and 'tail100=' in log_text

    print('TEST_PASS: parse_fail snapshot logging works and no crash')
    return 0


if __name__ == '__main__':
    try:
        raise SystemExit(main())
    except Exception:
        traceback.print_exc()
        raise SystemExit(2)
