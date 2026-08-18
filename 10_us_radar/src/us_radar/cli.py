"""Command-line entrypoint for the US radar sidecar."""

from __future__ import annotations

import argparse
from dataclasses import asdict, replace
from datetime import datetime, timedelta, timezone
import json
import os
import sys
import time

from .config import load_settings, load_watchlist, resolve_project_root
from .evidence import default_prompt_record, extract_dry_run_signal
from .form4 import fetch_form4_xml_from_index, parse_form4_xml
from .industry import expand_event_targets, load_industry_graph
from .market_data import MarketDataClient, ReturnResult, median_return
from .push import build_daily_push, resolve_pushplus_token, send_pushplus
from .quality import classify_event
from .reports import write_event_report
from .sec_edgar import SecConfigError, SecEdgarClient
from .sec_daily_index import (
    SecDailyIndexNotAvailable,
    fetch_daily_index,
    reconcile_watchlist,
)
from .storage import EventStore
from .validation import (
    CN_CHAIN_BENCHMARKS,
    CN_DEFAULT_BENCHMARK,
    US_BENCHMARKS,
    ValidationUpdate,
    build_validation_task,
    classify_direction,
    classify_quality_gate,
    classify_signal,
    classify_window,
    combine_quality,
    event_direction_from_quality,
    now_utc_text,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="US market radar sidecar")
    parser.add_argument("--root", default=None, help="Project root. Defaults to the current working tree root guess.")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("init-db", help="Initialize the sidecar event database")
    sub.add_parser("init-prompts", help="Initialize prompt version records")

    reset = sub.add_parser("reset-derived-data", help="Delete derived MVP data while keeping raw SEC events")
    reset.add_argument("--yes", action="store_true", help="Required confirmation flag")

    run = sub.add_parser("create-run", help="Create an MVP run marker")
    run.add_argument("--data-version", default="mvp_watchlist_v1")
    run.add_argument("--purpose", default="research_validation")

    fetch = sub.add_parser("fetch-sec", help="Fetch and store SEC Atom events")
    fetch.add_argument("--form-type", default="8-K", help="SEC form type, e.g. 8-K or 4")
    fetch.add_argument("--limit", type=int, default=None, help="Maximum SEC Atom entries to request")
    fetch.add_argument("--all-companies", action="store_true", help="Fetch all current filings instead of the configured watchlist CIKs")

    reconcile = sub.add_parser(
        "reconcile-sec-index",
        help="Compare one SEC daily master index with stored watchlist events",
    )
    reconcile.add_argument("--date", required=True, help="SEC filing date in YYYY-MM-DD format")
    reconcile.add_argument("--forms", default="8-K,4", help="Comma-separated exact SEC form families")
    reconcile.add_argument("--show-missing", type=int, default=20, help="Maximum missing rows to print")

    form4 = sub.add_parser("enrich-form4", help="Fetch and parse Form 4 XML transaction details")
    form4.add_argument("--hours", type=int, default=72)
    form4.add_argument("--limit", type=int, default=20)

    classify = sub.add_parser("classify-events", help="Classify event quality and transmission window")
    classify.add_argument("--hours", type=int, default=72)
    classify.add_argument("--limit", type=int, default=200)

    extract = sub.add_parser("extract-evidence", help="Create dry-run structured evidence for stored events")
    extract.add_argument("--hours", type=int, default=72)
    extract.add_argument("--limit", type=int, default=100)

    targets = sub.add_parser("expand-targets", help="Expand stored source events into multi-market signal targets")
    targets.add_argument("--hours", type=int, default=72)
    targets.add_argument("--limit", type=int, default=100)

    validate = sub.add_parser("prepare-validation", help="Create pending validation tasks for signal targets")
    validate.add_argument("--limit", type=int, default=500)
    validate.add_argument("--horizons", default="1,3,5,20", help="Comma-separated horizons in trading days")

    returns = sub.add_parser("validate-returns", help="Fill validation tasks with post-event returns")
    returns.add_argument("--limit", type=int, default=500)
    returns.add_argument("--include-complete", action="store_true", help="Recompute completed rows as well as pending forward windows")
    returns.add_argument("--market", choices=["US", "CN"], default=None, help="Optional market filter")
    returns.add_argument("--max-us-symbols", type=int, default=None, help="Maximum unique US target symbols to validate in this run")

    check_data = sub.add_parser("check-market-data", help="Check market data provider availability without writing the database")
    check_data.add_argument("--market", choices=["US", "CN"], default="US")
    check_data.add_argument("--symbols", default=None, help="Comma-separated symbols. Defaults to core MVP benchmarks.")
    check_data.add_argument("--benchmark", action="store_true", help="Use benchmark lookup path, useful for CN indices")
    check_data.add_argument("--horizon", type=int, default=1)
    check_data.add_argument("--event-days-ago", type=int, default=14)
    check_data.add_argument("--provider", choices=["auto", "fmp", "yfinance", "alpha", "twelve", "alpaca", "massive", "duckdb", "tushare"], default="auto", help="Probe one provider instead of the fallback chain")

    compare_data = sub.add_parser("compare-market-data", help="Compare read-only US provider returns without changing validation data")
    compare_data.add_argument("--symbols", default=None, help="Comma-separated symbols. Defaults to core MVP benchmarks.")
    compare_data.add_argument("--providers", default="yfinance,alpaca,massive", help="Comma-separated explicit providers")
    compare_data.add_argument("--horizon", type=int, default=1)
    compare_data.add_argument("--event-days-ago", type=int, default=14)

    report = sub.add_parser("report", help="Write a Markdown transmission report")
    report.add_argument("--hours", type=int, default=24)

    push_daily = sub.add_parser("push-daily", help="Build or send an independent daily research push")
    push_daily.add_argument("--hours", type=int, default=24)
    push_daily.add_argument("--limit", type=int, default=5)
    push_daily.add_argument("--send", action="store_true", help="Actually send via PushPlus. Default only prints.")

    return parser


def _store(root: str) -> EventStore:
    settings = load_settings(root)
    store = EventStore.from_settings(settings)
    store.initialize()
    return store


def cmd_init_db(root: str) -> int:
    store = _store(root)
    print(f"initialized sidecar database: {store.db_path}")
    return 0


def cmd_reset_derived(args: argparse.Namespace, root: str) -> int:
    if not args.yes:
        print("reset-derived-data requires --yes", file=sys.stderr)
        return 2
    deleted = _store(root).reset_derived_data()
    print(json.dumps(deleted, ensure_ascii=False, sort_keys=True))
    return 0


def cmd_create_run(args: argparse.Namespace, root: str) -> int:
    run_id = _store(root).create_mvp_run(args.data_version, args.purpose)
    print(json.dumps({"run_id": run_id, "data_version": args.data_version, "purpose": args.purpose}, sort_keys=True))
    return 0


def cmd_init_prompts(root: str) -> int:
    inserted = _store(root).upsert_prompt_version(default_prompt_record())
    print(f"prompt_versions_inserted={inserted}")
    return 0


def cmd_fetch_sec(args: argparse.Namespace, root: str) -> int:
    settings = load_settings(root)
    watchlist = load_watchlist(root)
    store = EventStore.from_settings(settings)
    store.initialize()
    try:
        client = SecEdgarClient.from_settings(settings)
    except SecConfigError as exc:
        print(f"SEC config error: {exc}", file=sys.stderr)
        return 2
    limit = args.limit or int(settings.sec.get("default_limit", 40))
    errors = 0
    attempts = 1 if args.all_companies else len(watchlist)
    if args.all_companies:
        try:
            filings = client.fetch_current_filings(args.form_type, limit=limit)
        except Exception as exc:
            print(f"SEC fetch warning form_type={args.form_type} scope=all error={exc}", file=sys.stderr)
            filings = []
            errors += 1
    else:
        filings = []
        for item in watchlist:
            try:
                filings.extend(client.fetch_company_filings(item.cik, args.form_type, limit=limit, ticker=item.ticker))
            except Exception as exc:
                errors += 1
                print(f"SEC fetch warning form_type={args.form_type} ticker={item.ticker} cik={item.cik} error={exc}", file=sys.stderr)
            time.sleep(float(os.environ.get("US_RADAR_SEC_SLEEP_SECONDS", "0.2")))
    inserted = store.upsert_events(filings)
    print(f"fetched={len(filings)} inserted={inserted} errors={errors} form_type={args.form_type}")
    if attempts > 0 and errors >= attempts:
        print(f"SEC fetch failed for every request form_type={args.form_type} attempts={attempts}", file=sys.stderr)
        return 1
    return 0


def cmd_reconcile_sec_index(args: argparse.Namespace, root: str) -> int:
    settings = load_settings(root)
    watchlist = load_watchlist(root)
    store = EventStore.from_settings(settings)
    store.initialize()
    try:
        filed_date = datetime.strptime(args.date, "%Y-%m-%d").date()
    except ValueError:
        print("reconcile-sec-index requires --date YYYY-MM-DD", file=sys.stderr)
        return 2
    try:
        client = SecEdgarClient.from_settings(settings)
        entries = fetch_daily_index(filed_date, user_agent=client.user_agent)
    except SecConfigError as exc:
        print(f"SEC config error: {exc}", file=sys.stderr)
        return 2
    except SecDailyIndexNotAvailable as exc:
        print(str(exc), file=sys.stderr)
        return 2
    except Exception as exc:
        print(f"SEC daily index fetch failed: {exc}", file=sys.stderr)
        return 1

    forms = {item.strip().upper() for item in args.forms.split(",") if item.strip()}
    result = reconcile_watchlist(
        entries=entries,
        watchlist_ciks={item.cik for item in watchlist},
        requested_forms=forms,
        known_accessions=store.known_event_accessions(),
    )
    missing_limit = max(0, int(args.show_missing))
    payload = {
        "status": "GAP_DETECTED" if result.missing_rows else "OK",
        "filed_date": filed_date.isoformat(),
        "forms": sorted(forms),
        "total_index_rows": result.total_index_rows,
        "watched_rows": result.watched_rows,
        "known_rows": result.known_rows,
        "missing_count": len(result.missing_rows),
        "missing_rows": [asdict(row) for row in result.missing_rows[:missing_limit]],
        "mode": "audit_only_no_ingest",
    }
    print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
    return 0


def cmd_enrich_form4(args: argparse.Namespace, root: str) -> int:
    settings = load_settings(root)
    store = EventStore.from_settings(settings)
    store.initialize()
    try:
        client = SecEdgarClient.from_settings(settings)
    except SecConfigError as exc:
        print(f"SEC config error: {exc}", file=sys.stderr)
        return 2
    since = datetime.now(timezone.utc) - timedelta(hours=args.hours)
    events = store.recent_events(since, event_type="4")[: args.limit]
    parsed = 0
    inserted = 0
    errors = 0
    for event in events:
        try:
            xml_text = fetch_form4_xml_from_index(event.url, client.user_agent)
            if not xml_text:
                continue
            txs = parse_form4_xml(event, xml_text)
            parsed += len(txs)
            inserted += store.insert_form4_transactions(txs)
        except Exception as exc:
            errors += 1
            print(f"Form4 enrich warning event_id={event.event_id} ticker={event.ticker} error={exc}", file=sys.stderr)
    print(json.dumps({"events_seen": len(events), "transactions_parsed": parsed, "transactions_inserted": inserted, "errors": errors}, sort_keys=True))
    return 0


def cmd_classify_events(args: argparse.Namespace, root: str) -> int:
    store = _store(root)
    since = datetime.now(timezone.utc) - timedelta(hours=args.hours)
    events = store.recent_events(since)[: args.limit]
    upserted = 0
    for event in events:
        upserted += store.upsert_event_quality(
            classify_event(
                event,
                store.form4_open_market_count(event.event_id),
                store.form4_transaction_count(event.event_id),
            )
        )
    print(json.dumps({"events_seen": len(events), "quality_upserted": upserted}, sort_keys=True))
    return 0


def cmd_extract_evidence(args: argparse.Namespace, root: str) -> int:
    store = _store(root)
    store.upsert_prompt_version(default_prompt_record())
    since = datetime.now(timezone.utc) - timedelta(hours=args.hours)
    events = store.all_events_without_signals(since)[: args.limit]
    inserted = sum(store.insert_signal(extract_dry_run_signal(event)) for event in events)
    print(json.dumps({"events_seen": len(events), "signals_inserted": inserted}, sort_keys=True))
    return 0


def cmd_expand_targets(args: argparse.Namespace, root: str) -> int:
    store = _store(root)
    graph = load_industry_graph(root)
    since = datetime.now(timezone.utc) - timedelta(hours=args.hours)
    events = store.recent_events(since)[: args.limit]
    inserted = 0
    expanded = 0
    for event in events:
        targets = expand_event_targets(event, graph)
        expanded += len(targets)
        inserted += store.upsert_signal_targets(targets)
    print(json.dumps({"events_seen": len(events), "targets_expanded": expanded, "targets_inserted": inserted}, sort_keys=True))
    return 0


def cmd_prepare_validation(args: argparse.Namespace, root: str) -> int:
    settings = load_settings(root)
    store = EventStore.from_settings(settings)
    store.initialize()
    horizons = list(dict.fromkeys(int(item.strip()) for item in args.horizons.split(",") if item.strip()))
    benchmarks = settings.sec  # harmless placeholder to keep settings loaded
    del benchmarks
    rows = store.pending_validation_targets(horizons, args.limit)
    inserted = 0
    default_benchmarks = {"US": "QQQ", "CN": "000300.SH"}
    for row in rows:
        for horizon in horizons:
            inserted += store.insert_validation_task(build_validation_task(row, horizon, default_benchmarks.get(row["target_market"])))
    print(json.dumps({"targets_seen": len(rows), "validation_tasks_inserted": inserted}, sort_keys=True))
    return 0


def cmd_validate_returns(args: argparse.Namespace, root: str) -> int:
    store = _store(root)
    client = MarketDataClient(root)
    rows = store.validation_work_items(args.limit, include_complete=args.include_complete, market=args.market)
    rows, skipped_us = _apply_us_api_budget(rows, args.max_us_symbols)
    updated = 0
    errors = 0
    qualities: dict[str, int] = {}
    for row in rows:
        try:
            update = _build_return_update(store, client, row)
        except Exception as exc:
            errors += 1
            print(
                f"validate-returns row warning validation_id={row.get('validation_id')} "
                f"target={row.get('target_market')}:{row.get('target_ticker')} error={exc}",
                file=sys.stderr,
            )
            update = _error_update(row, exc)
        updated += store.update_validation_result(update)
        qualities[update.data_quality] = qualities.get(update.data_quality, 0) + 1
    chain_summaries = store.refresh_event_chain_summaries()
    print(json.dumps({"validation_rows_seen": len(rows), "validation_rows_updated": updated, "event_chain_summaries": chain_summaries, "skipped_us_rows": skipped_us, "errors": errors, "qualities": qualities}, ensure_ascii=False, sort_keys=True))
    return 0


def cmd_check_market_data(args: argparse.Namespace, root: str) -> int:
    client = MarketDataClient(root)
    event_time = (datetime.now(timezone.utc) - timedelta(days=args.event_days_ago)).isoformat()
    symbols = _market_data_symbols(args.market, args.symbols)
    rows = []
    for symbol in symbols:
        if args.provider != "auto":
            result = client.provider_return(args.market, symbol, event_time, args.horizon, args.provider)
        elif args.benchmark:
            result = client.get_benchmark_return(args.market, symbol, event_time, args.horizon)
        else:
            result = client.get_return(args.market, symbol, event_time, args.horizon)
        rows.append(
            {
                "market": args.market,
                "symbol": symbol,
                "requested_provider": args.provider,
                "provider": result.provider,
                "data_quality": result.data_quality,
                "base_date": result.base_date,
                "horizon_date": result.horizon_date,
                "return_pct": result.return_pct,
            }
        )
    print(json.dumps({"event_time": event_time, "horizon": args.horizon, "results": rows}, ensure_ascii=False, sort_keys=True))
    return 0


def cmd_compare_market_data(args: argparse.Namespace, root: str) -> int:
    client = MarketDataClient(root)
    event_time = (datetime.now(timezone.utc) - timedelta(days=args.event_days_ago)).isoformat()
    symbols = _market_data_symbols("US", args.symbols)
    providers = [item.strip() for item in args.providers.split(",") if item.strip()]
    rows = []
    for symbol in symbols:
        results = [client.provider_return("US", symbol, event_time, args.horizon, provider) for provider in providers]
        baseline = next((result for result in results if result.data_quality == "DATA_OK" and result.return_pct is not None), None)
        rows.append({
            "symbol": symbol,
            "baseline_provider": None if baseline is None else baseline.provider,
            "providers": [
                {
                    "requested_provider": requested_provider,
                    "provider": result.provider,
                    "data_quality": result.data_quality,
                    "base_date": result.base_date,
                    "horizon_date": result.horizon_date,
                    "return_pct": result.return_pct,
                    "difference_vs_baseline_bps": None
                    if baseline is None or result.return_pct is None
                    else round((result.return_pct - baseline.return_pct) * 10000, 4),
                }
                for requested_provider, result in zip(providers, results)
            ],
        })
    print(json.dumps({
        "mode": "shadow_read_only_no_validation_write",
        "event_time": event_time,
        "horizon": args.horizon,
        "results": rows,
    }, ensure_ascii=False, sort_keys=True))
    return 0


def cmd_report(args: argparse.Namespace, root: str) -> int:
    settings = load_settings(root)
    store = EventStore.from_settings(settings)
    store.initialize()
    report_path = write_event_report(store, settings, hours=args.hours)
    print(f"wrote report: {report_path}")
    return 0


def cmd_push_daily(args: argparse.Namespace, root: str) -> int:
    settings = load_settings(root)
    store = EventStore.from_settings(settings)
    store.initialize()
    message = build_daily_push(store, settings.root, hours=args.hours, limit=args.limit)
    if not args.send:
        print(message.content)
        return 0
    token = resolve_pushplus_token()
    if not token:
        print("US_RADAR_PUSHPLUS_TOKEN or PUSHPLUS_TOKEN is required when --send is used", file=sys.stderr)
        return 2
    result = send_pushplus(message, token)
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    root = str(resolve_project_root(args.root))
    if args.command == "init-db":
        return cmd_init_db(root)
    if args.command == "reset-derived-data":
        return cmd_reset_derived(args, root)
    if args.command == "create-run":
        return cmd_create_run(args, root)
    if args.command == "init-prompts":
        return cmd_init_prompts(root)
    if args.command == "fetch-sec":
        return cmd_fetch_sec(args, root)
    if args.command == "reconcile-sec-index":
        return cmd_reconcile_sec_index(args, root)
    if args.command == "enrich-form4":
        return cmd_enrich_form4(args, root)
    if args.command == "classify-events":
        return cmd_classify_events(args, root)
    if args.command == "extract-evidence":
        return cmd_extract_evidence(args, root)
    if args.command == "expand-targets":
        return cmd_expand_targets(args, root)
    if args.command == "prepare-validation":
        return cmd_prepare_validation(args, root)
    if args.command == "validate-returns":
        return cmd_validate_returns(args, root)
    if args.command == "check-market-data":
        return cmd_check_market_data(args, root)
    if args.command == "compare-market-data":
        return cmd_compare_market_data(args, root)
    if args.command == "report":
        return cmd_report(args, root)
    if args.command == "push-daily":
        return cmd_push_daily(args, root)
    parser.error(f"unknown command: {args.command}")
    return 2


def _build_return_update(store: EventStore, client: MarketDataClient, row: dict) -> ValidationUpdate:
    event_time = row["event_time"]
    horizon = int(row["horizon_days"])
    market = row["target_market"]
    target = client.get_return(market, row["target_ticker"], event_time, horizon)
    if market == "CN":
        return _build_cn_update(store, client, row, target)
    if market == "US":
        return _build_us_update(client, row, target)
    return _base_update(row, target, benchmark=None, benchmark_return=None, data_quality="UNSUPPORTED_MARKET")


def _build_cn_update(store: EventStore, client: MarketDataClient, row: dict, target: ReturnResult) -> ValidationUpdate:
    event_time = row["event_time"]
    horizon = int(row["horizon_days"])
    csi300 = client.get_benchmark_return("CN", CN_DEFAULT_BENCHMARK, event_time, horizon)
    industry_symbol = CN_CHAIN_BENCHMARKS.get(row.get("theme"), CN_DEFAULT_BENCHMARK)
    industry = client.get_benchmark_return("CN", industry_symbol, event_time, horizon)
    peer_values = []
    for peer in store.cn_chain_targets(row["event_id"], row.get("theme"), exclude_ticker=row["target_ticker"]):
        peer_result = client.get_return("CN", peer["target_ticker"], event_time, horizon)
        if peer_result.return_pct is not None:
            peer_values.append(peer_result.return_pct)
    chain_median = median_return(peer_values)

    excess_vs_csi300 = _diff(target.return_pct, csi300.return_pct)
    excess_vs_industry = _diff(target.return_pct, industry.return_pct)
    excess_vs_chain_median = _diff(target.return_pct, chain_median)
    primary = _first_not_none(excess_vs_industry, excess_vs_csi300, excess_vs_chain_median)
    data_quality = combine_quality(target.data_quality, [csi300.data_quality, industry.data_quality])
    update = _base_update(
        row,
        target,
        benchmark=industry_symbol,
        benchmark_return=industry.return_pct,
        data_quality=data_quality,
        primary_excess=primary,
    )
    return replace(
        update,
        excess_vs_csi300=excess_vs_csi300,
        excess_vs_industry=excess_vs_industry,
        excess_vs_chain_median=excess_vs_chain_median,
        benchmark_data_quality=f"csi300={csi300.data_quality};industry={industry.data_quality}",
    )


def _build_us_update(client: MarketDataClient, row: dict, target: ReturnResult) -> ValidationUpdate:
    event_time = row["event_time"]
    horizon = int(row["horizon_days"])
    benchmarks = {symbol: client.get_benchmark_return("US", symbol, event_time, horizon) for symbol in US_BENCHMARKS}
    excess_vs_qqq = _diff(target.return_pct, benchmarks["QQQ"].return_pct)
    excess_vs_spy = _diff(target.return_pct, benchmarks["SPY"].return_pct)
    excess_vs_soxx = _diff(target.return_pct, benchmarks["SOXX"].return_pct)
    primary = _first_not_none(excess_vs_qqq, excess_vs_spy, excess_vs_soxx)
    data_quality = combine_quality(target.data_quality, [item.data_quality for item in benchmarks.values()])
    update = _base_update(
        row,
        target,
        benchmark="QQQ",
        benchmark_return=benchmarks["QQQ"].return_pct,
        data_quality=data_quality,
        primary_excess=primary,
    )
    return replace(
        update,
        excess_vs_qqq=excess_vs_qqq,
        excess_vs_spy=excess_vs_spy,
        excess_vs_soxx=excess_vs_soxx,
        benchmark_data_quality=";".join(f"{symbol}={result.data_quality}" for symbol, result in benchmarks.items()),
    )


def _base_update(
    row: dict,
    target: ReturnResult,
    benchmark: str | None,
    benchmark_return: float | None,
    data_quality: str,
    primary_excess: float | None = None,
) -> ValidationUpdate:
    signal_label = classify_signal(primary_excess, int(row["horizon_days"]))
    quality_gate, is_effective_sample = classify_quality_gate(data_quality, primary_excess)
    event_direction, direction_source, direction_confidence = event_direction_from_quality(row)
    direction_label = classify_direction(event_direction, primary_excess, int(row["horizon_days"]))
    return ValidationUpdate(
        validation_id=row["validation_id"],
        benchmark=benchmark,
        target_return=target.return_pct,
        benchmark_return=benchmark_return,
        excess_return=primary_excess,
        data_quality=data_quality,
        measured_at=now_utc_text(),
        base_date=target.base_date,
        horizon_date=target.horizon_date,
        base_close=target.base_close,
        horizon_close=target.horizon_close,
        price_provider=target.provider,
        primary_excess=primary_excess,
        signal_label=signal_label,
        direction_label=direction_label,
        event_direction=event_direction,
        direction_source=direction_source,
        direction_confidence=direction_confidence,
        window_label=classify_window(row.get("event_type"), int(row["horizon_days"])),
        transmission_type=str(row.get("target_transmission_type") or "unknown"),
        quality_gate=quality_gate,
        is_effective_sample=is_effective_sample,
    )


def _error_update(row: dict, exc: Exception) -> ValidationUpdate:
    target = ReturnResult(
        symbol=str(row.get("target_ticker") or ""),
        market=str(row.get("target_market") or ""),
        horizon_days=int(row["horizon_days"]),
        return_pct=None,
        base_date=None,
        horizon_date=None,
        base_close=None,
        horizon_close=None,
        provider=f"exception:{exc.__class__.__name__}",
        data_quality="PROVIDER_ERROR",
    )
    return _base_update(
        row,
        target,
        benchmark=row.get("benchmark"),
        benchmark_return=None,
        data_quality="PROVIDER_ERROR",
        primary_excess=None,
    )


def _diff(left: float | None, right: float | None) -> float | None:
    if left is None or right is None:
        return None
    return left - right


def _first_not_none(*values: float | None) -> float | None:
    for value in values:
        if value is not None:
            return value
    return None


def _apply_us_api_budget(rows: list[dict], max_symbols: int | None) -> tuple[list[dict], int]:
    budget = max_symbols
    if budget is None:
        budget = int(os.environ.get("US_RADAR_MAX_US_API_SYMBOLS", "4"))
    if budget < 0:
        return rows, 0
    seen: set[str] = set()
    kept: list[dict] = []
    skipped = 0
    for row in rows:
        if row.get("target_market") != "US":
            kept.append(row)
            continue
        ticker = str(row.get("target_ticker") or "")
        if ticker not in seen and len(seen) >= budget:
            skipped += 1
            continue
        seen.add(ticker)
        kept.append(row)
    return kept, skipped


def _market_data_symbols(market: str, raw_symbols: str | None) -> list[str]:
    if raw_symbols:
        return [item.strip() for item in raw_symbols.split(",") if item.strip()]
    if market == "CN":
        return list(dict.fromkeys([CN_DEFAULT_BENCHMARK, *CN_CHAIN_BENCHMARKS.values()]))
    return ["QQQ", "SPY", "SOXX", "NVDA", "AMD"]


if __name__ == "__main__":
    sys.exit(main())
