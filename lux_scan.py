"""每天固定时刻扫描飞书项目：指派给我的缺陷，以及评论里新 @ 我的缺陷。

抽出 log_part_lidar_pcap_cut 数据名，本地还没有的才丢进下载队列。
"""

from __future__ import annotations

import argparse
import json
import os
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from pathlib import Path
from dotenv import load_dotenv

from common import REDIS_QUEUE_KEY, SCAN_REQUEST_KEY, get_redis, reply_feishu_text
from feishu_project import (
    FeishuProjectClient,
    claim_download_name,
    clip_already_downloaded,
    comment_mentions_user,
    extract_data_names,
    release_download_name,
    user_is_assignee,
)

_ENV_PATH = Path(__file__).resolve().parent / ".env"
load_dotenv(_ENV_PATH, override=True)

CURSOR_KEY = "lux_scan_cursor"
ASSIGN_PARAMS = ("owner", "issue_operator", "current_status_operator", "role_owners")


def _tz() -> ZoneInfo:
    return ZoneInfo(os.getenv("LUX_SCAN_TZ", "Asia/Shanghai"))


def _scan_times() -> list[tuple[int, int]]:
    raw = os.getenv("LUX_SCAN_TIMES", "22:00,06:00")
    times: list[tuple[int, int]] = []
    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        hour_s, minute_s = part.split(":", 1)
        times.append((int(hour_s), int(minute_s)))
    if not times:
        times = [(22, 0), (6, 0)]
    return times


def next_run(now: datetime | None = None) -> datetime:
    now = now or datetime.now(_tz())
    if now.tzinfo is None:
        now = now.replace(tzinfo=_tz())
    candidates: list[datetime] = []
    for hour, minute in _scan_times():
        slot = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
        if slot <= now:
            slot += timedelta(days=1)
        candidates.append(slot)
    return min(candidates)


def _as_ms(value: object) -> int:
    try:
        stamp = int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return 0
    if 0 < stamp < 10**12:
        return stamp * 1000
    return stamp


def _work_item_url(base: str, item: dict, work_item_id: str) -> tuple[str, str]:
    simple = str(item.get("simple_name") or item.get("project_key") or "")
    url = f"{base.rstrip('/')}/{simple}/issue/detail/{work_item_id}" if simple else ""
    return simple, url


def run_scan(*, dry: bool = False) -> None:
    client = FeishuProjectClient()
    redis_client = get_redis()
    now_ms = int(time.time() * 1000)
    started_ms = now_ms
    raw_cursor = redis_client.get(CURSOR_KEY)
    if raw_cursor:
        since_ms = int(raw_cursor)
    else:
        lookback_h = int(os.getenv("LUX_SCAN_LOOKBACK_HOURS", "48"))
        since_ms = now_ms - lookback_h * 3600 * 1000
        print(f"首次扫描，评论 @ 回溯 {lookback_h} 小时")
    if since_ms >= now_ms:
        since_ms = now_ms - 1000

    projects = client.list_project_keys()
    if not projects:
        print("没有可扫描的飞书项目空间")
        return "没有可扫描的飞书项目空间"
    print(
        f"开始扫描 spaces={len(projects)} since={datetime.fromtimestamp(since_ms / 1000, _tz()):%F %T} "
        f"dry={dry}"
    )

    lines: list[str] = []
    names: list[str] = []
    queued = 0
    skipped = 0
    for project_key in projects:
        stat = _scan_project(
            client,
            redis_client,
            project_key,
            since_ms=since_ms,
            end_ms=now_ms,
            dry=dry,
        )
        lines.append(stat["line"])
        names.extend(stat["download_names"])
        queued += stat["queued"]
        skipped += stat["skipped"]

    if not dry:
        redis_client.set(CURSOR_KEY, str(started_ms))
    summary = f"扫描结束，新入队 {queued} 个工作项，已跳过 {skipped} 条数据\n" + "\n".join(lines)
    if names:
        shown = names[:20]
        summary += "\n将下载：\n" + "\n".join(f"· {name}" for name in shown)
        if len(names) > len(shown):
            summary += f"\n…还有 {len(names) - len(shown)} 条"
    print(summary)
    return summary


def _scan_project(
    client: FeishuProjectClient,
    redis_client,
    project_key: str,
    *,
    since_ms: int,
    end_ms: int,
    dry: bool,
) -> int:
    user_key = client.user_key
    by_id: dict[str, dict] = {}
    reasons: dict[str, set[str]] = {}

    role_search_ok = True
    for param in ASSIGN_PARAMS:
        try:
            items = client.search_issues(project_key, param, [user_key], "=")
        except Exception as exc:  # noqa: BLE001
            if param == "role_owners":
                role_search_ok = False
            print(f"⚠️ 空间 {project_key} 按 {param} 搜索失败，跳过该条件: {exc}")
            continue
        for item in items:
            work_item_id = str(item.get("id") or "")
            if not work_item_id:
                continue
            by_id[work_item_id] = item
            reasons.setdefault(work_item_id, set()).add(param)

    if not role_search_ok:
        try:
            for item in client.list_issues(project_key):
                if not user_is_assignee(item, user_key):
                    continue
                work_item_id = str(item.get("id") or "")
                if not work_item_id:
                    continue
                by_id.setdefault(work_item_id, item)
                reasons.setdefault(work_item_id, set()).add("role_owners")
        except Exception as exc:  # noqa: BLE001
            print(f"⚠️ 空间 {project_key} 按角色负责人补扫失败: {exc}")

    try:
        updated = client.list_updated_issues(project_key, since_ms, end_ms)
    except Exception as exc:  # noqa: BLE001
        print(f"⚠️ 空间 {project_key} 拉取近期更新失败: {exc}")
        updated = []

    comment_cache: dict[str, list] = {}
    for item in updated:
        work_item_id = str(item.get("id") or "")
        if not work_item_id:
            continue
        comments = client.list_comments(project_key, "issue", work_item_id)
        comment_cache[work_item_id] = comments
        mentioned = any(
            _as_ms(comment.get("created_at")) >= since_ms
            and comment_mentions_user(comment, user_key)
            for comment in comments
            if isinstance(comment, dict)
        )
        if mentioned:
            by_id.setdefault(work_item_id, item)
            reasons.setdefault(work_item_id, set()).add("comment_at")

    assigned = sum(1 for reason in reasons.values() if reason & set(ASSIGN_PARAMS))
    mentioned_n = sum(1 for reason in reasons.values() if "comment_at" in reason)
    skipped = 0
    queued = 0
    found_names = 0
    download_names: list[str] = []

    for work_item_id, brief in by_id.items():
        try:
            item = client.get_work_item(project_key, "issue", work_item_id)
        except Exception as exc:  # noqa: BLE001
            print(f"⚠️ 读取工作项 {work_item_id} 失败: {exc}")
            item = brief
        comments = comment_cache.get(work_item_id)
        if comments is None:
            comments = client.list_comments(project_key, "issue", work_item_id)
        names = extract_data_names(item, comments)
        found_names += len(names)
        pending: list[str] = []
        for name in names:
            if clip_already_downloaded(name):
                skipped += 1
                print(f"⏭ 已下载，跳过 {name}")
                continue
            if dry:
                pending.append(name)
                print(f"⬇ 将下载 {name}")
                continue
            if not claim_download_name(redis_client, name):
                print(f"⏭ 已在下载队列，跳过 {name}")
                continue
            pending.append(name)
        if not pending:
            continue
        download_names.extend(pending)
        simple, url = _work_item_url(client.base, item, work_item_id)
        title = str(item.get("name") or work_item_id)
        why = ",".join(sorted(reasons.get(work_item_id) or []))
        print(f"入队 {title} ({work_item_id}) reasons={why} names={len(pending)}")
        if dry:
            queued += 1
            continue
        task = {
            "action": "download",
            "source": "scan",
            "claimed": True,
            "project_url": url,
            "simple_name": simple or project_key,
            "work_item_type_key": "issue",
            "work_item_id": work_item_id,
            "chat_id": os.getenv("LUX_SCAN_NOTIFY_CHAT_ID", "").strip(),
            "message_id": "",
            "sender_open_id": "",
            "raw_text": "",
            "data_names": pending,
        }
        try:
            redis_client.lpush(REDIS_QUEUE_KEY, json.dumps(task, ensure_ascii=False))
            queued += 1
        except Exception as exc:  # noqa: BLE001
            print(f"❌ 入队失败 {work_item_id}: {exc}")
            for name in pending:
                release_download_name(redis_client, name)

    line = (
        f"空间 {project_key}: 指派 {assigned}，新@ {mentioned_n}，"
        f"数据名 {found_names}，已跳过 {skipped}，入队 {queued}"
    )
    print(line)
    return {
        "line": line,
        "queued": queued,
        "skipped": skipped,
        "download_names": download_names,
    }


def _locked_scan() -> str:
    client = get_redis()
    if not client.set("lux_scan_lock", "1", nx=True, ex=3600):
        return "上一轮扫描还没结束。"
    try:
        return run_scan(dry=False)
    finally:
        client.delete("lux_scan_lock")


def _run_requested(raw: str) -> None:
    try:
        req = json.loads(raw)
    except json.JSONDecodeError:
        print(f"❌ 非法扫描请求: {raw[:200]}")
        return
    print(f"收到手动扫描 chat_id={req.get('chat_id')}")
    try:
        summary = _locked_scan()
    except Exception as exc:  # noqa: BLE001
        summary = f"扫描失败：{exc}"
        print(f"❌ {summary}")
    try:
        reply_feishu_text(
            req.get("message_id"),
            summary,
            chat_id=req.get("chat_id") or "",
            reply_in_thread=True,
        )
    except Exception as exc:  # noqa: BLE001
        print(f"⚠️ 扫描结果没能发回飞书: {exc}\n{summary}")


def main() -> None:
    parser = argparse.ArgumentParser(description="定时扫描飞书项目缺陷并下载闭环数据")
    parser.add_argument("--once", action="store_true", help="立刻扫一遍后退出")
    parser.add_argument("--dry", action="store_true", help="只打印，不入队、不推进扫描游标")
    args = parser.parse_args()
    if args.once:
        run_scan(dry=args.dry)
        return

    enabled = os.getenv("LUX_SCAN_ENABLED", "1").strip().lower() not in ("0", "false", "no", "off")
    print(f"🟢 定时扫描启动 times={os.getenv('LUX_SCAN_TIMES', '22:00,06:00')} tz={_tz()} enabled={enabled}")
    redis_client = get_redis()
    announced: datetime | None = None
    while True:
        raw = redis_client.rpop(SCAN_REQUEST_KEY)
        if raw:
            _run_requested(raw)
            continue
        if not enabled:
            time.sleep(5)
            continue
        nxt = next_run()
        if announced != nxt:
            print(f"下次扫描 {nxt:%F %T %Z}")
            announced = nxt
        if datetime.now(_tz()) >= nxt:
            try:
                _locked_scan()
            except Exception as exc:  # noqa: BLE001
                print(f"❌ 本轮扫描失败: {exc}")
            announced = None
        else:
            time.sleep(2)


if __name__ == "__main__":
    main()
