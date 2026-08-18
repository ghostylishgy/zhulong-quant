from __future__ import annotations

import argparse
import importlib.util
import json
import os
import re
import urllib.request
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import uuid4

import duckdb
from dotenv import load_dotenv

from spark_etf.config.loader import PortfolioItem, load_spark_config
from spark_etf.db.db_init import DEFAULT_DB_PATH, EVENT_INSERT_SQL, init_db

PROJECT_ROOT = Path(__file__).resolve().parents[4]


@dataclass(slots=True)
class NewsItem:
    source: str
    title: str
    content: str
    published_at: datetime
    url: str | None = None


class NewsService:
    def __init__(
        self,
        db_path: Path | str | None = None,
        config_path: Path | str | None = None,
        ollama_model: str = "fin-auditor:latest",
        ollama_url: str | None = None,
        ollama_timeout: int = 30,
        max_audit_items: int = 20,
    ) -> None:
        self._load_env_files()

        self.db_path = Path(db_path).expanduser().resolve() if db_path else DEFAULT_DB_PATH.resolve()
        self.config_path = Path(config_path).expanduser().resolve() if config_path else None

        self.ollama_model = ollama_model
        self.ollama_url = ollama_url or os.getenv("SPARK_OLLAMA_URL") or os.getenv("OLLAMA_URL") or "http://192.0.2.20:11434/api/generate"
        self.ollama_timeout = ollama_timeout
        self.max_audit_items = max_audit_items

        init_db(self.db_path)

        config = load_spark_config(self.config_path)
        self.portfolios = config.portfolios
        self.etf_keywords = self._build_etf_keywords(self.portfolios)
        self.etf_patterns = {
            etf_code: self._compile_keyword_pattern(keywords)
            for etf_code, keywords in self.etf_keywords.items()
        }

        all_keywords = sorted({kw for kws in self.etf_keywords.values() for kw in kws})
        self.global_pattern = self._compile_keyword_pattern(all_keywords)
        self.cloud_provider = self._load_cloud_provider_config()

    def _load_env_files(self) -> None:
        here = Path(__file__).resolve()
        env_candidates = [
            here.parents[2] / ".env",  # ../../.env
            here.parents[3] / ".env",  # 08_spark_etf/.env
            here.parents[4] / ".env",  # quant_project/.env
            Path.cwd() / ".env",
        ]
        for env_file in env_candidates:
            if env_file.exists():
                load_dotenv(dotenv_path=env_file, override=False)

    def _load_cloud_provider_config(self) -> dict[str, str] | None:
        settings_path = PROJECT_ROOT / "config" / "settings.py"
        if not settings_path.exists():
            return None

        try:
            spec = importlib.util.spec_from_file_location("spark_root_settings", str(settings_path))
            if spec is None or spec.loader is None:
                return None
            mod = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(mod)
            cfg = mod.Config

            deepseek_key = str(getattr(cfg, "DEEPSEEK_API_KEY", "") or "").strip()
            kimi_key = str(getattr(cfg, "KIMI_API_KEY", "") or "").strip()

            if deepseek_key:
                return {
                    "provider": "deepseek",
                    "api_key": deepseek_key,
                    "base_url": str(getattr(cfg, "DEEPSEEK_BASE_URL", "https://api.deepseek.com")),
                    "model": str(getattr(cfg, "DEEPSEEK_MODEL_V3", "deepseek-chat")),
                }

            if kimi_key:
                return {
                    "provider": "kimi",
                    "api_key": kimi_key,
                    "base_url": str(getattr(cfg, "KIMI_BASE_URL", "https://api.moonshot.cn")),
                    "model": str(getattr(cfg, "KIMI_MODEL", "moonshot-v1-128k")),
                }
        except Exception:
            return None

        return None

    def _build_etf_keywords(self, portfolios: tuple[PortfolioItem, ...]) -> dict[str, set[str]]:
        alias_map = {
            "纳斯达克": {"纳指", "NASDAQ", "美股科技"},
            "半导体": {"芯片", "晶圆", "国产替代"},
            "人工智能": {"AI", "大模型", "算力"},
            "机器人": {"自动化", "人形机器人"},
            "港股": {"恒生", "港股科技"},
            "创新药": {"医药", "生物医药"},
            "医药医疗": {"医疗", "医药"},
            "黄金": {"金价", "贵金属", "避险"},
            "中概互联": {"中概", "互联网平台"},
        }

        result: dict[str, set[str]] = {}
        for item in portfolios:
            words: set[str] = set()
            words.update(tag.strip() for tag in item.tags if tag and tag.strip())

            name_tokens = re.findall(r"[\u4e00-\u9fffA-Za-z]{2,}", item.name)
            words.update(name_tokens)

            aliases: set[str] = set()
            for word in list(words):
                for key, values in alias_map.items():
                    if key in word or word in key:
                        aliases.update(values)
            words.update(aliases)

            cleaned = {word for word in words if len(word) >= 2}
            result[item.etf_code] = cleaned

        return result
    @staticmethod
    def _compile_keyword_pattern(keywords: list[str] | set[str]) -> re.Pattern[str]:
        terms = [kw.strip() for kw in keywords if kw and kw.strip()]
        if not terms:
            return re.compile(r"$^")
        terms = sorted(set(terms), key=lambda x: len(x), reverse=True)
        return re.compile("|".join(re.escape(t) for t in terms), re.IGNORECASE)

    @staticmethod
    def _parse_datetime(value: Any) -> datetime | None:
        if value is None:
            return None
        if isinstance(value, datetime):
            return value

        text = str(value).strip()
        if not text:
            return None

        text = text.replace("T", " ").replace("Z", "")

        formats = (
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d %H:%M",
            "%Y/%m/%d %H:%M:%S",
            "%Y/%m/%d %H:%M",
            "%Y-%m-%d",
            "%Y%m%d",
            "%m-%d %H:%M",
        )

        for fmt in formats:
            try:
                dt = datetime.strptime(text, fmt)
                if fmt == "%m-%d %H:%M":
                    return dt.replace(year=datetime.now().year)
                return dt
            except ValueError:
                continue

        try:
            return datetime.fromisoformat(text)
        except ValueError:
            return None

    @staticmethod
    def _pick_first(row: dict[str, Any], keys: tuple[str, ...], default: str = "") -> str:
        for key in keys:
            if key in row and row[key] is not None:
                text = str(row[key]).strip()
                if text:
                    return text
        return default

    def _row_to_news_item(self, row: dict[str, Any], source: str) -> NewsItem:
        title = self._pick_first(row, ("title", "headline", "标题", "主题", "新闻标题"), default="(无标题)")
        content = self._pick_first(
            row,
            ("content", "summary", "desc", "正文", "内容", "摘要"),
            default=title,
        )
        published_text = self._pick_first(
            row,
            ("datetime", "pub_time", "发布时间", "time", "date", "created_at", "end_date", "发布时间"),
            default="",
        )
        published_at = self._parse_datetime(published_text) or datetime.now()
        url = self._pick_first(row, ("url", "link", "地址", "来源链接"), default="")

        return NewsItem(
            source=source,
            title=title,
            content=content,
            published_at=published_at,
            url=(url or None),
        )

    @staticmethod
    def _df_to_rows(df: Any, limit: int = 300) -> list[dict[str, Any]]:
        if df is None:
            return []
        if hasattr(df, "head") and hasattr(df, "to_dict"):
            try:
                return df.head(limit).to_dict(orient="records")
            except Exception:
                return []
        return []

    def _fetch_tushare_news(self, start_dt: datetime, errors: dict[str, str]) -> list[NewsItem]:
        try:
            import tushare as ts
        except Exception as exc:
            errors["tushare_import"] = str(exc)
            return []

        token = os.getenv("TUSHARE_TOKEN")
        if not token:
            errors["tushare_token"] = "missing TUSHARE_TOKEN"
            return []

        try:
            pro = ts.pro_api(token)
        except Exception as exc:
            errors["tushare_pro_api"] = str(exc)
            return []

        items: list[NewsItem] = []
        start_str = start_dt.strftime("%Y-%m-%d %H:%M:%S")
        end_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        for src in ("sina", "wallstreetcn"):
            df = None
            try:
                df = pro.news(src=src, start_date=start_str, end_date=end_str, limit=300)
            except Exception:
                try:
                    df = pro.news(src=src, limit=300)
                except Exception as exc:
                    errors[f"tushare_news_{src}"] = str(exc)
                    continue

            for row in self._df_to_rows(df):
                item = self._row_to_news_item(row, source=f"tushare_news:{src}")
                if item.published_at >= start_dt:
                    items.append(item)

        for day in [datetime.now().strftime("%Y%m%d"), (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")]:
            try:
                df = pro.cctv_news(date=day)
            except Exception as exc:
                errors[f"tushare_cctv_news_{day}"] = str(exc)
                continue

            for row in self._df_to_rows(df):
                item = self._row_to_news_item(row, source="tushare_cctv_news")
                if item.published_at >= start_dt:
                    items.append(item)

        return items

    def _fetch_akshare_news(self, start_dt: datetime, errors: dict[str, str]) -> list[NewsItem]:
        try:
            import akshare as ak
        except Exception as exc:
            errors["akshare_import"] = str(exc)
            return []

        items: list[NewsItem] = []
        try:
            df = ak.stock_info_global_em()
            for row in self._df_to_rows(df):
                item = self._row_to_news_item(row, source="akshare_stock_info_global_em")
                if item.published_at >= start_dt:
                    items.append(item)
        except Exception as exc:
            errors["akshare_stock_info_global_em"] = str(exc)

        return items

    @staticmethod
    def _deduplicate(items: list[NewsItem]) -> list[NewsItem]:
        seen: set[tuple[str, str, str]] = set()
        uniq: list[NewsItem] = []
        for item in items:
            key = (item.source, item.title.strip(), item.published_at.strftime("%Y-%m-%d %H:%M"))
            if key in seen:
                continue
            seen.add(key)
            uniq.append(item)
        return uniq

    def _match_items(self, items: list[NewsItem]) -> list[tuple[str, NewsItem, list[str]]]:
        hits: list[tuple[str, NewsItem, list[str]]] = []

        for item in items:
            text = f"{item.title} {item.content}"
            if not self.global_pattern.search(text):
                continue

            for etf_code, pattern in self.etf_patterns.items():
                if not pattern.search(text):
                    continue
                matched_keywords = [kw for kw in self.etf_keywords[etf_code] if kw in text][:6]
                hits.append((etf_code, item, matched_keywords))

        return hits

    def _audit_with_ollama(self, item: NewsItem, etf_code: str, keywords: list[str]) -> dict[str, Any]:
        prompt = (
            "你是量化投研审计器。\n"
            "阅读下面财经快讯，判断其对目标资产的叙事影响。\n"
            "只输出 JSON，不要输出任何额外文字。\n"
            "JSON 格式固定为: {\"impact\":\"利好/利空/中性\",\"score\":1-10,\"reason\":\"一句话理由\"}\n"
            f"目标ETF: {etf_code}\n"
            f"关键词: {','.join(keywords) if keywords else '无'}\n"
            f"标题: {item.title}\n"
            f"内容: {item.content[:1200]}\n"
        )

        request_payload = {
            "model": self.ollama_model,
            "prompt": prompt,
            "stream": False,
            "options": {"temperature": 0.0},
        }

        data = json.dumps(request_payload).encode("utf-8")
        req = urllib.request.Request(
            self.ollama_url,
            data=data,
            headers={"Content-Type": "application/json"},
            method="POST",
        )

        with urllib.request.urlopen(req, timeout=self.ollama_timeout) as resp:
            raw = resp.read().decode("utf-8")
        result = json.loads(raw)
        content = str(result.get("response", "")).strip()

        match = re.search(r"\{[\s\S]*\}", content)
        if not match:
            raise ValueError(f"model output is not valid JSON: {content[:120]}")

        parsed = json.loads(match.group(0))
        impact = str(parsed.get("impact", "中性")).strip()
        if impact not in {"利好", "利空", "中性"}:
            impact = "中性"

        try:
            score = int(round(float(parsed.get("score", 5))))
        except Exception:
            score = 5
        score = max(1, min(10, score))

        reason = str(parsed.get("reason", "模型未提供理由")).strip().replace("\n", " ")
        reason = reason[:160] if reason else "模型未提供理由"

        return {"impact": impact, "score": score, "reason": reason}

    def _audit_with_cloud(
        self,
        etf_code: str,
        item: NewsItem,
        local_sentiment: dict[str, Any],
        keywords: list[str],
    ) -> dict[str, Any] | None:
        bullish = "利好"
        bearish = "利空"
        neutral = "中性"

        if not self.cloud_provider:
            return None

        impact = str(local_sentiment.get("impact", neutral))
        score = float(local_sentiment.get("score", 0))
        if impact not in {bullish, bearish} or score <= 7:
            return None

        prompt = (
            "You are a cloud verifier for macro narratives. "
            "Given the news and local audit result, infer 3-6 month impact on the ETF. "
            "Return JSON only: "
            "{\"impact_3_6m\":\"bullish/bearish/neutral\",\"score\":1-10,\"reason\":\"one sentence\",\"risks\":\"one sentence\"}.\n"
            f"ETF: {etf_code}\n"
            f"Keywords: {', '.join(keywords) if keywords else 'N/A'}\n"
            f"LocalAudit: impact={impact}, score={score}, reason={local_sentiment.get('reason', '')}\n"
            f"Title: {item.title}\n"
            f"Content: {item.content[:1400]}\n"
        )

        base_url = str(self.cloud_provider["base_url"]).rstrip("/")
        api_url = f"{base_url}/v1/chat/completions"
        headers = {
            "Authorization": f"Bearer {self.cloud_provider['api_key']}",
            "Content-Type": "application/json",
        }
        payload = {
            "model": self.cloud_provider["model"],
            "messages": [
                {"role": "system", "content": "Return strict JSON only."},
                {"role": "user", "content": prompt},
            ],
            "temperature": 0.1,
            "max_tokens": 280,
        }

        req = urllib.request.Request(
            api_url,
            data=json.dumps(payload).encode("utf-8"),
            headers=headers,
            method="POST",
        )

        with urllib.request.urlopen(req, timeout=30) as resp:
            raw = resp.read().decode("utf-8")

        outer = json.loads(raw)
        text = str(outer.get("choices", [{}])[0].get("message", {}).get("content", "")).strip()
        match = re.search(r"\{[\s\S]*\}", text)
        if not match:
            raise ValueError(f"cloud output invalid: {text[:120]}")

        parsed = json.loads(match.group(0))
        cloud_impact = str(parsed.get("impact_3_6m", neutral)).strip()
        low = cloud_impact.lower()
        if low in {"bullish", "positive", "up"}:
            cloud_impact = bullish
        elif low in {"bearish", "negative", "down"}:
            cloud_impact = bearish
        elif low in {"neutral", "flat"}:
            cloud_impact = neutral

        if cloud_impact not in {bullish, bearish, neutral}:
            cloud_impact = neutral

        try:
            cloud_score = int(round(float(parsed.get("score", 5))))
        except Exception:
            cloud_score = 5
        cloud_score = max(1, min(10, cloud_score))

        reason = str(parsed.get("reason", "N/A")).strip().replace("\n", " ")[:180]
        risks = str(parsed.get("risks", "")).strip().replace("\n", " ")[:180]

        return {
            "provider": self.cloud_provider["provider"],
            "model": self.cloud_provider["model"],
            "impact_3_6m": cloud_impact,
            "score": cloud_score,
            "reason": reason or "N/A",
            "risks": risks,
        }

    def _persist_macro_event(
        self,
        conn: duckdb.DuckDBPyConnection,
        etf_code: str,
        item: NewsItem,
        sentiment: dict[str, Any],
        keywords: list[str],
        cloud_feedback: dict[str, Any] | None,
    ) -> None:
        payload: dict[str, Any] = {
            "source": item.source,
            "title": item.title,
            "content": item.content[:2200],
            "published_at": item.published_at.strftime("%Y-%m-%d %H:%M:%S"),
            "url": item.url,
            "matched_keywords": keywords,
            "impact": sentiment["impact"],
            "score": sentiment["score"],
            "reason": sentiment["reason"],
        }
        if cloud_feedback is not None:
            payload["cloud_audit_feedback"] = cloud_feedback

        note = f"{sentiment['impact']} {sentiment['score']}/10 {sentiment['reason']}"
        conn.execute(
            EVENT_INSERT_SQL,
            [
                str(uuid4()),
                etf_code,
                "MACRO_SENTIMENT",
                json.dumps(payload, ensure_ascii=False),
                note,
            ],
        )

    def _send_instant_alert(self, high_hits: list[dict[str, Any]], alert_threshold: int) -> bool:
        if not high_hits:
            return False

        pusher_path = PROJECT_ROOT / "utils" / "pusher.py"
        if not pusher_path.exists():
            return False

        try:
            spec = importlib.util.spec_from_file_location("spark_utils_pusher", str(pusher_path))
            if spec is None or spec.loader is None:
                return False
            mod = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(mod)
        except Exception:
            return False

        lines = [f"Hit high-score narrative threshold (>= {alert_threshold}): {len(high_hits)}"]
        for hit in high_hits[:8]:
            lines.append(
                f"- {hit['etf_code']} | {hit['impact']} {hit['score']}/10 | {hit['reason'][:60]}"
            )

        send_fn = getattr(mod, "send_push", None)
        if callable(send_fn):
            return bool(send_fn("[星火即时风口告警]", "\n".join(lines), template="txt"))
        return False

    def run_daily_pipeline(
        self,
        hours: int = 24,
        alert_threshold: int = 8,
        enable_alert: bool = True,
    ) -> dict[str, Any]:
        bullish = "利好"
        bearish = "利空"
        neutral = "中性"

        start_dt = datetime.now() - timedelta(hours=hours)
        errors: dict[str, str] = {}

        raw_items: list[NewsItem] = []
        raw_items.extend(self._fetch_tushare_news(start_dt=start_dt, errors=errors))
        raw_items.extend(self._fetch_akshare_news(start_dt=start_dt, errors=errors))

        uniq_items = self._deduplicate(raw_items)
        matched = self._match_items(uniq_items)

        if len(matched) > self.max_audit_items:
            matched = matched[: self.max_audit_items]

        persisted = 0
        audited = 0
        cloud_verified = 0
        high_hits: list[dict[str, Any]] = []

        conn = duckdb.connect(str(self.db_path))
        try:
            for etf_code, item, keywords in matched:
                try:
                    sentiment = self._audit_with_ollama(item=item, etf_code=etf_code, keywords=keywords)
                except Exception as exc:
                    sentiment = {
                        "impact": neutral,
                        "score": 5,
                        "reason": f"Ollama audit failed: {exc}",
                    }
                    errors[f"ollama:{etf_code}:{item.title[:20]}"] = str(exc)

                cloud_feedback = None
                try:
                    cloud_feedback = self._audit_with_cloud(
                        etf_code=etf_code,
                        item=item,
                        local_sentiment=sentiment,
                        keywords=keywords,
                    )
                    if cloud_feedback is not None:
                        cloud_verified += 1
                except Exception as exc:
                    errors[f"cloud:{etf_code}:{item.title[:20]}"] = str(exc)

                audited += 1

                if str(sentiment.get("impact")) in {bullish, bearish} and float(sentiment.get("score", 0)) >= alert_threshold:
                    high_hits.append(
                        {
                            "etf_code": etf_code,
                            "impact": sentiment.get("impact"),
                            "score": sentiment.get("score"),
                            "reason": sentiment.get("reason", ""),
                        }
                    )

                try:
                    self._persist_macro_event(
                        conn=conn,
                        etf_code=etf_code,
                        item=item,
                        sentiment=sentiment,
                        keywords=keywords,
                        cloud_feedback=cloud_feedback,
                    )
                    persisted += 1
                except Exception as exc:
                    errors[f"persist:{etf_code}:{item.title[:20]}"] = str(exc)
        finally:
            conn.close()

        alert_sent = False
        if enable_alert and high_hits:
            try:
                alert_sent = self._send_instant_alert(high_hits, alert_threshold=alert_threshold)
            except Exception as exc:
                errors["instant_alert"] = str(exc)

        return {
            "window_hours": hours,
            "raw_items": len(raw_items),
            "unique_items": len(uniq_items),
            "matched_items": len(matched),
            "audited_items": audited,
            "cloud_verified": cloud_verified,
            "persisted_events": persisted,
            "high_score_hits": high_hits,
            "alert_sent": alert_sent,
            "errors": errors,
        }

    def get_hot_wind_annotations(self, days: int = 3, min_score: float = 7.0) -> dict[str, str]:
        today = date.today()
        required_days = {today - timedelta(days=i) for i in range(days)}

        result: dict[str, str] = {}

        conn = duckdb.connect(str(self.db_path))
        try:
            for etf_code in self.etf_keywords.keys():
                rows = conn.execute(
                    """
                    SELECT
                        CAST(timestamp AS DATE) AS d,
                        json_extract_string(tushare_snapshot, '$.impact') AS impact,
                        try_cast(json_extract(tushare_snapshot, '$.score') AS DOUBLE) AS score,
                        json_extract_string(tushare_snapshot, '$.reason') AS reason,
                        timestamp
                    FROM spark_event_log
                    WHERE event_type = 'MACRO_SENTIMENT'
                      AND etf_code = ?
                      AND timestamp >= now() - INTERVAL '14 days'
                    ORDER BY timestamp DESC
                    """,
                    [etf_code],
                ).fetchall()

                day_bullish: dict[date, bool] = {}
                latest_reason = ""
                latest_score = 0.0

                for d, impact, score, reason, _ in rows:
                    if isinstance(d, datetime):
                        day_key = d.date()
                    else:
                        day_key = d

                    score_val = float(score) if score is not None else 0.0
                    is_bullish = impact == "利好" and score_val > min_score
                    day_bullish[day_key] = day_bullish.get(day_key, False) or is_bullish

                    if is_bullish and not latest_reason:
                        latest_reason = str(reason or "行业叙事增强")
                        latest_score = score_val

                if required_days.issubset({k for k, v in day_bullish.items() if v}):
                    reason = latest_reason or "行业景气与政策叙事延续"
                    result[etf_code] = f"{reason}（近{days}天连续利好，评分>{min_score:g}）"
        finally:
            conn.close()

        return result


def main() -> int:
    parser = argparse.ArgumentParser(description="Spark news RAG pipeline")
    parser.add_argument("--hours", type=int, default=24, help="Lookback window in hours")
    parser.add_argument("--alert-threshold", type=int, default=8, help="Instant alert score threshold")
    parser.add_argument("--max-audit-items", type=int, default=20, help="Max matched news for LLM audit")
    parser.add_argument("--disable-alert", action="store_true", help="Disable instant alert push")
    args = parser.parse_args()

    service = NewsService(max_audit_items=args.max_audit_items)
    summary = service.run_daily_pipeline(
        hours=args.hours,
        alert_threshold=args.alert_threshold,
        enable_alert=(not args.disable_alert),
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
