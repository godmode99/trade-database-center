# telegram_notifier.py
from __future__ import annotations

import requests
from typing import Any, Dict


def _bool(v: Any, default: bool = False) -> bool:
    if v is None:
        return default
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return bool(v)
    s = str(v).strip().lower()
    return s in ("1", "true", "yes", "y", "on")


def send_telegram_message(cfg: Dict[str, Any], text: str, logger=None) -> None:
    tg = cfg.get("telegram", {}) if cfg else {}
    if not _bool(tg.get("enabled"), False):
        return

    bot_token = tg.get("bot_token")
    chat_id = tg.get("chat_id")

    if not bot_token or not chat_id:
        if logger:
            logger.warning("Telegram enabled but bot_token/chat_id is missing. Skipping telegram notify.")
        return

    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }

    try:
        r = requests.post(url, data=payload, timeout=20)
        if not r.ok and logger:
            detail = r.text
            try:
                response_json = r.json()
                detail = response_json.get("description", detail)
            except ValueError:
                response_json = None
            logger.warning(f"Telegram send failed: HTTP {r.status_code} {detail}")
            if r.status_code == 400 and "chat not found" in str(detail).lower():
                logger.warning(
                    "Telegram chat not found. Check telegram.chat_id, ensure the bot is added to the chat, "
                    "and send /start to the bot for direct messages."
                )
    except Exception as e:
        if logger:
            logger.warning(f"Telegram send exception: {e}")


def classify_manifest(manifest: Dict[str, Any]) -> str:
    """
    Returns: "OK" | "WARN" | "ERROR"
    """
    sources = manifest.get("sources", {}) or {}
    stale = manifest.get("stale_sources", []) or []
    notes = (manifest.get("notes") or "").strip()

    any_fail = any((not (v.get("ok") is True)) for v in sources.values()) if isinstance(sources, dict) else False
    if any_fail:
        return "ERROR"
    if stale or notes:
        return "WARN"
    return "OK"


def format_manifest_message(manifest: Dict[str, Any]) -> str:
    status = classify_manifest(manifest)
    asof_utc = manifest.get("asof_utc")
    asof_th = manifest.get("asof_th")
    if asof_utc:
        asof_label = "asof_utc"
        asof = asof_utc
    elif asof_th:
        asof_label = "asof_th"
        asof = asof_th
    else:
        asof_label = "asof"
        asof = "?"

    if status == "OK":
        head = "✅ <b>MT5 Fetch: OK</b>"
    elif status == "WARN":
        head = "⚠️ <b>MT5 Fetch: WARNING</b>"
    else:
        head = "❌ <b>MT5 Fetch: ERROR</b>"

    lines = [head, f"<b>{asof_label}</b>: {asof}"]

    sources = manifest.get("sources", {}) or {}
    if isinstance(sources, dict) and sources:
        lines.append("<b>Sources</b>:")
        for k, v in sources.items():
            ok = v.get("ok")
            rows = v.get("rows")
            latest = v.get("latest_time") or v.get("latest")
            used_cache = v.get("used_cache")
            error = (v.get("error") or "").strip()
            if ok and error:
                tag = "WARN"
            elif ok:
                tag = "OK"
            else:
                tag = "FAIL"
            cache = " (cache)" if used_cache else ""
            lines.append(f"• {k}: {tag}{cache}, rows={rows}, latest={latest}")
            if error:
                lines.append(f"  ↳ error: {error}")
            day_label = v.get("day")
            if day_label:
                raw_rows = v.get("raw_rows")
                todays_rows = v.get("todays_rows")
                filtered_today_rows = v.get("filtered_today_rows")
                other_today_rows = v.get("other_today_rows")
                other_today_events = v.get("other_today_events")
                if ok and error:
                    lines.append(f"  ↳ Recheck failed; using cache for {day_label}.")
                if raw_rows == 0:
                    lines.append(f"  ↳ Source returned 0 events for {day_label}.")
                if filtered_today_rows == 0 and isinstance(todays_rows, int):
                    lines.append(
                        f"  ↳ No relevant news for {day_label} (total today={todays_rows})."
                    )
                if isinstance(other_today_events, list):
                    if other_today_events:
                        lines.append(f"  ↳ Other events on {day_label}:")
                        for item in other_today_events:
                            lines.append(f"    - {item}")
                    elif isinstance(other_today_rows, int):
                        lines.append(f"  ↳ Other events on {day_label}: none.")
                elif ok is False:
                    lines.append(f"  ↳ Other events on {day_label}: unavailable (fetch failed).")

    stale = manifest.get("stale_sources", []) or []
    if stale:
        lines.append(f"<b>stale_sources</b>: {', '.join(stale)}")

    notes = (manifest.get("notes") or "").strip()
    if notes:
        lines.append(f"<b>notes</b>: {notes}")

    return "\n".join(lines)
