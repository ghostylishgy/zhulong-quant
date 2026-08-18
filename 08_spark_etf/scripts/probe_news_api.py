from __future__ import annotations

import json
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from dotenv import load_dotenv


def _project_paths() -> tuple[Path, Path, Path]:
    script_path = Path(__file__).resolve()
    spark_root = script_path.parents[1]
    quant_root = spark_root.parents[0]
    logs_path = spark_root / "logs" / "news_api_samples.json"
    return spark_root, quant_root, logs_path


def _load_env(quant_root: Path, spark_root: Path) -> None:
    env_candidates = [
        quant_root / ".env",
        spark_root / ".env",
        Path.cwd() / ".env",
    ]
    for env_file in env_candidates:
        if env_file.exists():
            load_dotenv(env_file, override=False)


def _df_sample(df: Any, limit: int = 3) -> list[dict[str, Any]]:
    if df is None:
        return []
    if hasattr(df, "head") and hasattr(df, "to_dict"):
        try:
            rows = df.head(limit).to_dict(orient="records")
            return [{str(k): v for k, v in row.items()} for row in rows]
        except Exception:
            return []
    return []


def _probe_tushare_news(pro: Any) -> dict[str, Any]:
    sources = ["sina", "wallstreetcn"]
    errors: dict[str, str] = {}

    for src in sources:
        try:
            df = pro.news(src=src, limit=30)
            sample = _df_sample(df, limit=3)
            if sample:
                return {"source": src, "sample": sample}
            errors[src] = "empty result"
        except Exception as exc:
            errors[src] = str(exc)

    return {"error": "all sources failed", "details": errors}


def _probe_tushare_cctv_news(pro: Any) -> dict[str, Any]:
    today = datetime.now().strftime("%Y%m%d")
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")

    tries = [
        {"date": today},
        {"date": yesterday},
        {"start_date": yesterday, "end_date": today},
    ]

    last_error = "unknown"
    for params in tries:
        try:
            df = pro.cctv_news(**params)
            sample = _df_sample(df, limit=3)
            if sample:
                return {"params": params, "sample": sample}
            last_error = f"empty result with params={params}"
        except Exception as exc:
            last_error = f"params={params}: {exc}"

    return {"error": last_error}


def _probe_tushare() -> dict[str, Any]:
    try:
        import tushare as ts
    except Exception as exc:
        return {"error": f"tushare import failed: {exc}"}

    token = os.getenv("TUSHARE_TOKEN")
    if not token:
        return {"error": "TUSHARE_TOKEN not found in environment/.env"}

    try:
        pro = ts.pro_api(token)
    except Exception as exc:
        return {"error": f"tushare.pro_api init failed: {exc}"}

    result: dict[str, Any] = {}
    try:
        result["news"] = _probe_tushare_news(pro)
    except Exception as exc:
        result["news"] = {"error": str(exc)}

    try:
        result["cctv_news"] = _probe_tushare_cctv_news(pro)
    except Exception as exc:
        result["cctv_news"] = {"error": str(exc)}

    return result


def _probe_akshare_global_news() -> dict[str, Any]:
    import akshare as ak

    df = ak.stock_info_global_em()
    sample = _df_sample(df, limit=3)
    if sample:
        return {"sample": sample}
    return {"error": "empty result"}


def _probe_akshare_stock_news() -> dict[str, Any]:
    import akshare as ak

    # 选择常见宽基成分股作为样例代码
    symbol = "600036"
    df = ak.stock_news_em(symbol=symbol)
    sample = _df_sample(df, limit=3)
    if sample:
        return {"symbol": symbol, "sample": sample}
    return {"symbol": symbol, "error": "empty result"}


def _probe_akshare() -> dict[str, Any]:
    result: dict[str, Any] = {}
    try:
        result["stock_info_global_em"] = _probe_akshare_global_news()
    except Exception as exc:
        result["stock_info_global_em"] = {"error": str(exc)}

    try:
        result["stock_news_em"] = _probe_akshare_stock_news()
    except Exception as exc:
        result["stock_news_em"] = {"error": str(exc)}

    return result


def main() -> int:
    spark_root, quant_root, output_file = _project_paths()
    _load_env(quant_root=quant_root, spark_root=spark_root)

    samples: dict[str, Any] = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "environment": {
            "spark_root": str(spark_root),
            "quant_root": str(quant_root),
            "token_loaded": bool(os.getenv("TUSHARE_TOKEN")),
        },
        "tushare": {},
        "akshare": {},
    }

    try:
        samples["tushare"] = _probe_tushare()
    except Exception as exc:
        samples["tushare"] = {"error": f"unexpected failure: {exc}"}

    try:
        samples["akshare"] = _probe_akshare()
    except Exception as exc:
        samples["akshare"] = {"error": f"unexpected failure: {exc}"}

    output_file.parent.mkdir(parents=True, exist_ok=True)
    output_file.write_text(json.dumps(samples, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"[probe] samples saved to: {output_file}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
