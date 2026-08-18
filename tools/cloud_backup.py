#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Cloud disaster backup for Zhulong.

Design goals:
- Run after RAG refresh and before market open.
- Keep database facts and RAG vectors recoverable together.
- Use restic for encrypted incremental local repository.
- Optionally upload the restic repository to Baidu Netdisk via BaiduPCS-Go.
"""

from __future__ import annotations

import json
import os
import secrets
import shutil
import socket
import sqlite3
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple

import requests

PROJECT_ROOT = Path('/root/quant_project')
LOG_PATH = PROJECT_ROOT / 'logs' / 'cloud_backup.log'
ENV_PATH = PROJECT_ROOT / '.env'
DB_PATH = PROJECT_ROOT / 'storage' / 'database' / 'zhulong.duckdb'
DB_SNAPSHOT_PATH = PROJECT_ROOT / 'storage' / 'database' / 'zhulong_cloud_backup.duckdb'
CHROMA_DIR = PROJECT_ROOT / 'storage' / 'chromadb'
PROJECT_REPORTS_DIR = PROJECT_ROOT / 'storage' / 'reports'
US_RADAR_DB_PATH = PROJECT_ROOT / '10_us_radar' / 'data' / 'us_radar.sqlite3'
US_RADAR_DB_SNAPSHOT_PATH = PROJECT_ROOT / 'storage' / 'database' / 'us_radar_cloud_backup.sqlite3'
US_RADAR_ENV_PATH = PROJECT_ROOT / '10_us_radar' / '.env.local'
US_RADAR_REPORTS_DIR = PROJECT_ROOT / '10_us_radar' / 'reports'
GIT_BUNDLE_PATH = PROJECT_ROOT / 'storage' / 'backup_staging' / 'zhulong_source.bundle'
GIT_BUNDLE_REF_DEFAULT = 'HEAD'
RESTIC_REPO_DEFAULT = PROJECT_ROOT / 'storage' / 'cloud_restic_repo'
RESTIC_PASSWORD_DEFAULT = PROJECT_ROOT / '.secrets' / 'restic_password'

ENGINE_LIB = PROJECT_ROOT / '01_engine' / 'lib'
if str(ENGINE_LIB) not in sys.path:
    sys.path.insert(0, str(ENGINE_LIB))
from db_gateway import DBGateway


def _log(msg: str) -> None:
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    line = f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | {msg}"
    print(line)
    with LOG_PATH.open('a', encoding='utf-8') as f:
        f.write(line + '\n')


def _load_env() -> Dict[str, str]:
    env = dict(os.environ)
    if ENV_PATH.exists():
        for raw in ENV_PATH.read_text(encoding='utf-8', errors='ignore').splitlines():
            line = raw.strip()
            if not line or line.startswith('#') or '=' not in line:
                continue
            key, value = line.split('=', 1)
            value = value.strip().strip('"').strip("'")
            env.setdefault(key.strip(), value)
    return env


def _run(cmd: List[str], *, env: Dict[str, str] | None = None, timeout: int = 7200) -> Tuple[int, str]:
    _log('RUN ' + ' '.join(cmd))
    proc = subprocess.run(
        cmd,
        cwd=str(PROJECT_ROOT),
        env=env,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        timeout=timeout,
        check=False,
    )
    output = (proc.stdout or '').strip()
    if output:
        _log(output[-4000:])
    return proc.returncode, output


def _baidu_output_has_error(output: str, codes: Tuple[str, ...] = ('31045', '31066', '31023')) -> bool:
    text = str(output or '')
    lower = text.lower()
    hard_markers = (
        'user not exists',
        'login required',
        'not logged in',
        'access token',
        'cookie',
    )
    if any(marker in lower for marker in hard_markers):
        return True
    context_markers = (
        'errno',
        'error_code',
        'error code',
        'error:',
        'errmsg',
        '\u9519\u8bef\u7801',
        '\u5931\u8d25',
    )
    return any(code in lower for code in codes) and any(marker in lower for marker in context_markers)


def _pushplus(env: Dict[str, str], title: str, content: str) -> None:
    token = str(env.get('PUSHPLUS_TOKEN') or '').strip()
    if not token:
        _log('PushPlus skipped: PUSHPLUS_TOKEN missing')
        return
    url = str(env.get('PUSHPLUS_URL') or 'https://www.pushplus.plus/send')
    timeout = int(env.get('PUSHPLUS_TIMEOUT') or 10)
    try:
        resp = requests.post(
            url,
            json={'token': token, 'title': title, 'content': content[:3000], 'template': 'txt'},
            timeout=timeout,
        )
        ok = resp.status_code == 200 and resp.json().get('code') == 200
        _log(f'PushPlus sent={ok} status={resp.status_code}')
    except Exception as exc:
        _log(f'PushPlus exception: {exc}')


def _ensure_password(path: Path) -> bool:
    if path.exists() and path.read_text(encoding='utf-8', errors='ignore').strip():
        return False
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(secrets.token_urlsafe(36) + '\n', encoding='utf-8')
    os.chmod(path, 0o600)
    return True


def _refresh_db_snapshot() -> None:
    _log('Refreshing DuckDB cloud snapshot')
    tmp_path = DB_SNAPSHOT_PATH.with_suffix('.tmp')
    try:
        with DBGateway(
            DB_PATH,
            read_only=False,
            expected_hold_seconds=120,
        ) as conn:
            conn.execute('CHECKPOINT')
            # The open RW connection holds DuckDB's cross-process file lock,
            # so another writer cannot mutate the source during this copy.
            shutil.copy2(DB_PATH, tmp_path)
        tmp_path.replace(DB_SNAPSHOT_PATH)
    except Exception:
        try:
            tmp_path.unlink(missing_ok=True)
        except Exception as cleanup_exc:
            _log(f'DuckDB snapshot temp cleanup failed: {cleanup_exc}')
        raise
    _log(f'DuckDB snapshot ready: {DB_SNAPSHOT_PATH} size={DB_SNAPSHOT_PATH.stat().st_size}')


def _cleanup_db_snapshot() -> str:
    if not DB_SNAPSHOT_PATH.exists():
        _log(f'DuckDB cloud snapshot already absent: {DB_SNAPSHOT_PATH}')
        return 'ALREADY_ABSENT'
    size = DB_SNAPSHOT_PATH.stat().st_size
    DB_SNAPSHOT_PATH.unlink()
    _log(
        f'DuckDB cloud snapshot removed after verified backup: '
        f'{DB_SNAPSHOT_PATH} size={size}'
    )
    return 'REMOVED'


def _refresh_us_radar_snapshot() -> str:
    if not US_RADAR_DB_PATH.exists():
        _log(f'US Radar SQLite source absent; snapshot skipped: {US_RADAR_DB_PATH}')
        return 'SOURCE_ABSENT'
    _log('Refreshing US Radar SQLite cloud snapshot')
    tmp_path = US_RADAR_DB_SNAPSHOT_PATH.with_suffix('.tmp')
    tmp_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        source_uri = f'file:{US_RADAR_DB_PATH}?mode=ro'
        with sqlite3.connect(source_uri, uri=True, timeout=30) as source:
            with sqlite3.connect(str(tmp_path), timeout=30) as destination:
                source.backup(destination)
                integrity = destination.execute('PRAGMA integrity_check').fetchone()
                if not integrity or str(integrity[0]).lower() != 'ok':
                    raise RuntimeError(f'US Radar SQLite integrity check failed: {integrity}')
        tmp_path.replace(US_RADAR_DB_SNAPSHOT_PATH)
    except Exception:
        tmp_path.unlink(missing_ok=True)
        raise
    size = US_RADAR_DB_SNAPSHOT_PATH.stat().st_size
    _log(f'US Radar SQLite snapshot ready: {US_RADAR_DB_SNAPSHOT_PATH} size={size}')
    return 'READY'


def _cleanup_us_radar_snapshot() -> str:
    if not US_RADAR_DB_SNAPSHOT_PATH.exists():
        _log(f'US Radar cloud snapshot already absent: {US_RADAR_DB_SNAPSHOT_PATH}')
        return 'ALREADY_ABSENT'
    size = US_RADAR_DB_SNAPSHOT_PATH.stat().st_size
    US_RADAR_DB_SNAPSHOT_PATH.unlink()
    _log(
        f'US Radar cloud snapshot removed after verified backup: '
        f'{US_RADAR_DB_SNAPSHOT_PATH} size={size}'
    )
    return 'REMOVED'


def _refresh_git_bundle(env: Dict[str, str]) -> str:
    """Stage the checked-out committed history for encrypted restic backup."""
    _log('Refreshing Git source bundle')
    GIT_BUNDLE_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = GIT_BUNDLE_PATH.with_suffix('.tmp')
    bundle_ref = str(env.get('CLOUD_BACKUP_GIT_BUNDLE_REF') or GIT_BUNDLE_REF_DEFAULT).strip()
    if not bundle_ref:
        bundle_ref = GIT_BUNDLE_REF_DEFAULT
    try:
        tmp_path.unlink(missing_ok=True)
        rc, out = _run(
            ['git', 'bundle', 'create', str(tmp_path), bundle_ref],
            timeout=int(env.get('CLOUD_BACKUP_GIT_BUNDLE_TIMEOUT', '600')),
        )
        if rc != 0:
            raise RuntimeError(f'git bundle create failed: {out[-800:]}')
        rc, verify_out = _run(
            ['git', 'bundle', 'verify', str(tmp_path)],
            timeout=int(env.get('CLOUD_BACKUP_GIT_BUNDLE_TIMEOUT', '600')),
        )
        if rc != 0:
            raise RuntimeError(f'git bundle verify failed: {verify_out[-800:]}')
        tmp_path.replace(GIT_BUNDLE_PATH)
    except Exception:
        tmp_path.unlink(missing_ok=True)
        raise
    size = GIT_BUNDLE_PATH.stat().st_size
    _log(f'Git source bundle ready: {GIT_BUNDLE_PATH} ref={bundle_ref} size={size}')
    return 'READY'


def _cleanup_git_bundle() -> str:
    if not GIT_BUNDLE_PATH.exists():
        _log(f'Git source bundle already absent: {GIT_BUNDLE_PATH}')
        return 'ALREADY_ABSENT'
    size = GIT_BUNDLE_PATH.stat().st_size
    GIT_BUNDLE_PATH.unlink()
    _log(f'Git source bundle removed after verified backup: {GIT_BUNDLE_PATH} size={size}')
    return 'REMOVED'


def _restic_env(env: Dict[str, str], repo: Path, password_file: Path) -> Dict[str, str]:
    out = dict(os.environ)
    out.update(env)
    out['RESTIC_REPOSITORY'] = str(repo)
    out['RESTIC_PASSWORD_FILE'] = str(password_file)
    return out


def _restic_init_if_needed(restic_env: Dict[str, str]) -> None:
    rc, _ = _run(['restic', 'snapshots', '--json'], env=restic_env, timeout=120)
    if rc == 0:
        return
    rc, out = _run(['restic', 'init'], env=restic_env, timeout=300)
    if rc != 0:
        raise RuntimeError(f'restic init failed: {out[-500:]}')


def _backup_sources(env: Dict[str, str]) -> List[str]:
    configured = str(env.get('CLOUD_BACKUP_EXTRA_PATHS') or '').strip()
    sources = [
        str(DB_SNAPSHOT_PATH),
        str(CHROMA_DIR),
        str(PROJECT_REPORTS_DIR),
        str(GIT_BUNDLE_PATH),
        str(US_RADAR_DB_SNAPSHOT_PATH),
        str(US_RADAR_ENV_PATH),
        str(US_RADAR_REPORTS_DIR),
        str(PROJECT_ROOT / '.env'),
        str(PROJECT_ROOT / '05_shadow' / 'config' / 'rules.yaml'),
        str(PROJECT_ROOT / 'config' / 'config.yaml'),
        str(PROJECT_ROOT / 'config' / 'settings.py'),
        str(PROJECT_ROOT / 'README.md'),
        str(PROJECT_ROOT / 'README.zh-CN.md'),
        str(PROJECT_ROOT / 'devlog.md'),
    ]
    if configured:
        sources.extend(p for p in configured.split(':') if p)
    return [p for p in sources if Path(p).exists()]


def _run_restic_backup(env: Dict[str, str], repo: Path, password_file: Path) -> Dict[str, object]:
    restic_env = _restic_env(env, repo, password_file)
    _restic_init_if_needed(restic_env)
    sources = _backup_sources(env)
    if not sources:
        raise RuntimeError('no backup sources found')
    cmd = ['restic', 'backup', '--json', '--tag', 'zhulong', '--tag', socket.gethostname()] + sources
    rc, out = _run(cmd, env=restic_env, timeout=int(env.get('CLOUD_BACKUP_RESTIC_TIMEOUT', '7200')))
    if rc != 0:
        raise RuntimeError(f'restic backup failed: {out[-800:]}')
    rc, forget_out = _run(
        [
            'restic', 'forget', '--prune',
            '--keep-daily', str(env.get('CLOUD_BACKUP_KEEP_DAILY') or '30'),
            '--keep-weekly', str(env.get('CLOUD_BACKUP_KEEP_WEEKLY') or '12'),
            '--keep-monthly', str(env.get('CLOUD_BACKUP_KEEP_MONTHLY') or '6'),
        ],
        env=restic_env,
        timeout=int(env.get('CLOUD_BACKUP_PRUNE_TIMEOUT', '7200')),
    )
    if rc != 0:
        raise RuntimeError(f'restic forget/prune failed: {forget_out[-800:]}')
    rc, snap_out = _run(['restic', 'snapshots', '--json'], env=restic_env, timeout=300)
    snapshots = []
    if rc == 0 and snap_out:
        try:
            snapshots = json.loads(snap_out)
        except Exception:
            snapshots = []
    latest_snapshot_id = ''
    if snapshots:
        try:
            latest_snapshot_id = sorted(snapshots, key=lambda x: str(x.get('time') or ''))[-1].get('id') or ''
        except Exception:
            latest_snapshot_id = ''
    return {
        'sources': sources,
        'snapshots': len(snapshots),
        'latest_snapshot_id': latest_snapshot_id,
        'repo': str(repo),
    }


def _baidu_assert_login(bin_path: str, env: Dict[str, str]) -> None:
    rc, who_out = _run([bin_path, 'who'], timeout=int(env.get('CLOUD_BACKUP_BAIDU_CHECK_TIMEOUT', '120')))
    if rc != 0 or 'uid: 0' in who_out or _baidu_output_has_error(who_out, codes=('31045', '31023')):
        raise RuntimeError(f'BAIDU_LOGIN_INVALID: Baidu login invalid before upload: {who_out[-500:]}')
    rc, quota_out = _run([bin_path, 'quota'], timeout=int(env.get('CLOUD_BACKUP_BAIDU_CHECK_TIMEOUT', '120')))
    if rc != 0 or _baidu_output_has_error(quota_out, codes=('31045', '31023')):
        raise RuntimeError(f'BAIDU_LOGIN_INVALID: Baidu quota check failed before upload: {quota_out[-500:]}')


def _baidu_assert_remote_exists(bin_path: str, env: Dict[str, str], remote_path: str) -> None:
    rc, out = _run([bin_path, 'meta', remote_path], timeout=int(env.get('CLOUD_BACKUP_BAIDU_CHECK_TIMEOUT', '120')))
    if rc != 0 or _baidu_output_has_error(out, codes=('31045', '31066', '31023')):
        raise RuntimeError(f'Baidu remote verification failed for {remote_path}: {out[-500:]}')


def _repo_relative_paths_from_upload_output(output: str, repo: Path) -> List[str]:
    prefix = str(repo).rstrip('/') + '/'
    found = []
    seen = set()
    for raw in str(output or '').splitlines():
        idx = raw.find(prefix)
        if idx < 0:
            continue
        rel = raw[idx + len(prefix):].strip().split()[0]
        rel = rel.strip().strip(',;')
        if not rel or rel.startswith('locks') or rel in seen:
            continue
        seen.add(rel)
        found.append(rel)
    return found


def _baidu_verify_reported_repo_paths(bin_path: str, env: Dict[str, str], repo: Path, remote_dir: str, output: str) -> List[str]:
    rel_paths = _repo_relative_paths_from_upload_output(output, repo)
    if not rel_paths:
        reason = 'UPLOAD_OUTPUT_UNVERIFIABLE: no restic repository paths parsed from successful upload output'
        _log(f'Baidu reported-path verification failed: {reason}')
        return [reason]
    max_verify = max(1, int(env.get('CLOUD_BACKUP_VERIFY_REPORTED_UPLOAD_PATHS_MAX') or '500'))
    if len(rel_paths) > max_verify:
        _log(f'Baidu reported {len(rel_paths)} repo paths; verifying first {max_verify}')
        rel_paths = rel_paths[:max_verify]
    missing = []
    for rel in rel_paths:
        remote_path = f'{remote_dir}/{rel}'
        try:
            _baidu_assert_remote_exists(bin_path, env, remote_path)
        except Exception as exc:
            missing.append(f'{rel}: {exc}')
    if missing:
        _log(f'Baidu reported-path verification missing={len(missing)}')
    else:
        _log(f'Baidu reported-path verification ok count={len(rel_paths)}')
    return missing


def _upload_to_baidu(env: Dict[str, str], repo: Path, expected_snapshot_id: str = '') -> str:
    enabled = str(env.get('CLOUD_BACKUP_UPLOAD_ENABLED') or '0').strip() == '1'
    if not enabled:
        return 'SKIPPED_UPLOAD_DISABLED'
    bin_path = str(env.get('BAIDUPCS_BIN') or shutil.which('BaiduPCS-Go') or '/usr/local/bin/BaiduPCS-Go')
    if not Path(bin_path).exists():
        raise RuntimeError(f'BaiduPCS-Go not found: {bin_path}')
    _baidu_assert_login(bin_path, env)
    remote_dir = str(env.get('BAIDUPCS_REMOTE_DIR') or '/zhulong-quant/restic_repo').rstrip('/')
    upload_args = [arg for arg in str(env.get('BAIDUPCS_UPLOAD_ARGS') or '--policy rsync').split() if arg]
    repo_children = [str(p) for p in sorted(repo.iterdir()) if p.name != 'locks']
    if not repo_children:
        raise RuntimeError(f'restic repo is empty: {repo}')

    retries = max(1, int(env.get('CLOUD_BACKUP_UPLOAD_RETRIES') or '3'))
    retry_delay = max(1, int(env.get('CLOUD_BACKUP_UPLOAD_RETRY_DELAY_SEC') or '45'))
    timeout = int(env.get('CLOUD_BACKUP_UPLOAD_TIMEOUT', '14400'))
    last_out = ''
    cmd = [bin_path, 'upload'] + upload_args + repo_children + [remote_dir]

    for attempt in range(1, retries + 1):
        if attempt > 1:
            _log(f'Baidu upload retry {attempt}/{retries} after {retry_delay}s')
            time.sleep(retry_delay)
            _baidu_assert_login(bin_path, env)
        rc, out = _run(cmd, timeout=timeout)
        last_out = out
        if rc == 0:
            missing = _baidu_verify_reported_repo_paths(bin_path, env, repo, remote_dir, out)
            if not missing:
                break
            last_out = '\n'.join(missing)[-1200:]
        _log(f'Baidu upload attempt {attempt}/{retries} failed rc={rc}')
    else:
        raise RuntimeError(f'BAIDU_UPLOAD_FAILED: Baidu upload failed after {retries} attempts: {last_out[-1200:]}')

    try:
        for child in ('config', 'data', 'index', 'keys', 'snapshots'):
            _baidu_assert_remote_exists(bin_path, env, f'{remote_dir}/{child}')
        if expected_snapshot_id:
            _baidu_assert_remote_exists(bin_path, env, f'{remote_dir}/snapshots/{expected_snapshot_id}')
    except Exception as exc:
        raise RuntimeError(f'BAIDU_UPLOAD_VERIFY_FAILED: {exc}') from exc
    return 'UPLOADED_VERIFIED'


def main() -> int:
    started = time.time()
    env = _load_env()
    repo = Path(env.get('RESTIC_REPOSITORY') or env.get('CLOUD_BACKUP_RESTIC_REPO') or RESTIC_REPO_DEFAULT)
    password_file = Path(env.get('RESTIC_PASSWORD_FILE') or env.get('CLOUD_BACKUP_RESTIC_PASSWORD_FILE') or RESTIC_PASSWORD_DEFAULT)
    created_password = False
    status = 'FAILED'
    upload_status = 'NOT_RUN'
    snapshot_cleanup_status = 'PRESERVED_NOT_SUCCESSFUL'
    us_radar_snapshot_status = 'NOT_RUN'
    us_radar_cleanup_status = 'PRESERVED_NOT_SUCCESSFUL'
    git_bundle_status = 'NOT_RUN'
    git_bundle_cleanup_status = 'PRESERVED_NOT_SUCCESSFUL'
    detail: Dict[str, object] = {}
    try:
        _log('=' * 72)
        _log('Zhulong cloud backup start')
        created_password = _ensure_password(password_file)
        if created_password:
            _log(f'Generated restic password file: {password_file}; copy it to an offline safe place.')
        repo.mkdir(parents=True, exist_ok=True)
        _refresh_db_snapshot()
        us_radar_snapshot_status = _refresh_us_radar_snapshot()
        git_bundle_status = _refresh_git_bundle(env)
        detail = _run_restic_backup(env, repo, password_file)
        detail['us_radar_snapshot_status'] = us_radar_snapshot_status
        detail['git_bundle_status'] = git_bundle_status
        upload_status = _upload_to_baidu(env, repo, str(detail.get('latest_snapshot_id') or ''))
        status = 'OK' if upload_status in {'UPLOADED_VERIFIED', 'SKIPPED_UPLOAD_DISABLED'} else 'WARN'
        return 0
    except Exception as exc:
        _log(f'ERROR {exc}')
        detail['error'] = str(exc)
        error_msg = str(exc)
        if 'BAIDU_LOGIN_INVALID' in error_msg:
            upload_status = 'BAIDU_LOGIN_INVALID'
            status = 'FAILED_BAIDU_AUTH'
        elif 'BAIDU_UPLOAD_VERIFY_FAILED' in error_msg:
            upload_status = 'BAIDU_UPLOAD_VERIFY_FAILED'
            status = 'FAILED_BAIDU_UPLOAD'
        elif 'BAIDU_UPLOAD_FAILED' in error_msg or 'Baidu upload failed' in error_msg:
            upload_status = 'BAIDU_UPLOAD_FAILED'
            status = 'FAILED_BAIDU_UPLOAD'
        return 2
    finally:
        if status == 'OK':
            try:
                snapshot_cleanup_status = _cleanup_db_snapshot()
            except Exception as exc:
                snapshot_cleanup_status = f'FAILED: {exc}'
                _log(f'DuckDB cloud snapshot cleanup warning: {exc}')
            try:
                us_radar_cleanup_status = _cleanup_us_radar_snapshot()
            except Exception as exc:
                us_radar_cleanup_status = f'FAILED: {exc}'
                _log(f'US Radar cloud snapshot cleanup warning: {exc}')
            try:
                git_bundle_cleanup_status = _cleanup_git_bundle()
            except Exception as exc:
                git_bundle_cleanup_status = f'FAILED: {exc}'
                _log(f'Git source bundle cleanup warning: {exc}')
        else:
            _log(
                f'DuckDB cloud snapshot preserved because backup status is {status}: '
                f'{DB_SNAPSHOT_PATH}'
            )
            if US_RADAR_DB_SNAPSHOT_PATH.exists():
                _log(
                    f'US Radar cloud snapshot preserved because backup status is {status}: '
                    f'{US_RADAR_DB_SNAPSHOT_PATH}'
                )
            if GIT_BUNDLE_PATH.exists():
                _log(
                    f'Git source bundle preserved because backup status is {status}: '
                    f'{GIT_BUNDLE_PATH}'
                )
        elapsed = time.time() - started
        repo_size = 0
        try:
            repo_size = sum(p.stat().st_size for p in repo.rglob('*') if p.is_file())
        except Exception:
            repo_size = 0
        status_text = {
            'OK': '\u6b63\u5e38',
            'WARN': '\u9700\u5173\u6ce8',
            'FAILED_BAIDU_AUTH': '\u767e\u5ea6\u6388\u6743\u5931\u6548',
            'FAILED_BAIDU_UPLOAD': '\u4e91\u7aef\u540c\u6b65\u5931\u8d25',
        }.get(status, status)
        upload_text = {
            'UPLOADED_VERIFIED': '\u5df2\u4e0a\u4f20\u5e76\u6821\u9a8c',
            'SKIPPED_UPLOAD_DISABLED': '\u672c\u8f6e\u53ea\u505a\u672c\u5730\u52a0\u5bc6\u5907\u4efd\uff0c\u672a\u5f00\u542f\u7f51\u76d8\u4e0a\u4f20',
            'BAIDU_LOGIN_INVALID': '\u767e\u5ea6\u7f51\u76d8\u767b\u5f55\u6001\u5931\u6548',
            'BAIDU_UPLOAD_FAILED': '\u767e\u5ea6\u7f51\u76d8\u4e0a\u4f20\u5931\u8d25\uff08\u767b\u5f55\u6b63\u5e38\uff09',
            'BAIDU_UPLOAD_VERIFY_FAILED': '\u767e\u5ea6\u7f51\u76d8\u4e0a\u4f20\u540e\u6821\u9a8c\u5931\u8d25\uff08\u767b\u5f55\u6b63\u5e38\uff09',
        }.get(upload_status, upload_status)
        title = f'\u70db\u9f99\u4e91\u5907\u4efd | {status_text}'
        error_text = str(detail.get('error') or '').strip()
        auth_hint = ''
        if upload_status == 'BAIDU_LOGIN_INVALID':
            auth_hint = chr(10).join([
                '',
                '\u5904\u7406\u5efa\u8bae\uff1a\u767e\u5ea6\u7f51\u76d8 Cookie/\u767b\u5f55\u6001\u5df2\u5931\u6548\uff0c\u8bf7\u91cd\u65b0\u6267\u884c BaiduPCS-Go login -cookies \u6388\u6743\u3002',
                '\u6ce8\u610f\uff1a\u672c\u5730 DuckDB \u5feb\u7167\u4e0e restic \u52a0\u5bc6\u4ed3\u5e93\u53ef\u80fd\u5df2\u7ecf\u751f\u6210\uff0c\u4f46\u672c\u8f6e\u672a\u786e\u8ba4\u4e91\u7aef\u4e0a\u4f20\u6210\u529f\u3002',
            ])
        elif upload_status in {'BAIDU_UPLOAD_FAILED', 'BAIDU_UPLOAD_VERIFY_FAILED'}:
            auth_hint = chr(10).join([
                '',
                '\u5904\u7406\u5efa\u8bae\uff1a\u767e\u5ea6\u767b\u5f55\u6b63\u5e38\uff0c\u4f46\u4e91\u7aef\u4ed3\u5e93\u672a\u5b8c\u6210\u4e0a\u4f20\u6821\u9a8c\u3002\u8bf7\u4f18\u5148\u67e5\u770b\u4e0a\u4f20\u65e5\u5fd7\u3001\u7f51\u7edc\u548c BaiduPCS-Go \u8fd4\u56de\u4fe1\u606f\u3002',
                '\u6ce8\u610f\uff1a\u672c\u5730 DuckDB \u5feb\u7167\u4e0e restic \u52a0\u5bc6\u4ed3\u5e93\u5df2\u4f18\u5148\u4fdd\u7559\uff0c\u4f46\u707e\u96be\u6062\u590d\u8981\u6c42\u4e91\u7aef\u540c\u6b65\u6210\u529f\u540e\u624d\u7b97\u95ed\u73af\u3002',
            ])
        content = '\n'.join([
            '[烛龙云备份]',
            f'状态：{status_text}',
            f'云端上传：{upload_text}',
            f'耗时：{elapsed:.1f}s',
            f'本地加密仓库：{repo}',
            f'仓库大小：{repo_size / 1024 / 1024:.1f} MB',
            f'快照数量：{detail.get("snapshots", 0)}',
            f'备份源数量：{len(detail.get("sources", [])) if isinstance(detail.get("sources"), list) else 0}',
            f'US Radar快照：{detail.get("us_radar_snapshot_status", us_radar_snapshot_status)}',
            f'临时数据库快照清理：{snapshot_cleanup_status}',
            f'US Radar临时快照清理：{us_radar_cleanup_status}',
            f'Git源码bundle：{detail.get("git_bundle_status", git_bundle_status)}',
            f'Git源码bundle清理：{git_bundle_cleanup_status}',
            f'首次生成密码文件：{1 if created_password else 0}',
            f'错误：{error_text[-1200:] if error_text else "无"}',
            f'日志：{LOG_PATH}',
            '',
            '范围：已包含 DuckDB 主库快照、RAG 事实表、storage/chromadb 向量索引、已提交 Git 对象 bundle、核心配置与运行文档。',
            'Git bundle 只包含当前 HEAD 的已提交历史；restic 使用内容块去重，后续备份按增量上传。',
            '提醒：如果云端上传显示未开启，说明当前只完成了本地加密备份。',
            auth_hint,
        ])
        _pushplus(env, title, content)
        _log('Zhulong cloud backup done')


if __name__ == '__main__':
    raise SystemExit(main())
