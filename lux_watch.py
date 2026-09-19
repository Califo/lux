"""轮询 Gerrit：被加为 reviewer 先问一句；自己的 change CI 失败或有新评论就短通知。"""

from __future__ import annotations

import argparse
import json
import os
import re
import time
from datetime import datetime, timedelta, timezone

import requests
from pathlib import Path
from dotenv import load_dotenv
from requests.auth import HTTPDigestAuth

from common import (
    GERRIT_HOST,
    get_redis,
    notify_feishu,
    save_review_offer,
)

_ENV_PATH = Path(__file__).resolve().parent / ".env"
load_dotenv(_ENV_PATH, override=True)

GERRIT_USER = os.getenv("GERRIT_USER", "")
GERRIT_PWD = os.getenv("GERRIT_HTTP_PWD", "")
GERRIT_AUTH = HTTPDigestAuth(GERRIT_USER, GERRIT_PWD)

ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")
TS_RE = re.compile(r"^\[[0-9]{4}-[0-9]{2}-[0-9]{2}T[^\]]+\]\s*")
CI_STATUS_RE = re.compile(
    r"https?://\S+/job/([^/\s]+)/(\d+)/?\s*:\s*(FAILURE|SUCCESS|ABORTED|UNSTABLE)\b"
)
JOB_RE = re.compile(r"https?://\S+/job/([^/\s]+)/(\d+)/?")
ERROR_RE = re.compile(
    r"(Failed in branch|ERROR:|error:|fatal:|undefined reference|Build failed|Finished: FAILURE)",
    re.I,
)
NOISE_RE = re.compile(
    r"^(Uploaded patch set|Patch Set \d+: Patch Set .* was rebased|"
    r"Patch Set \d+: Commit message was updated|Patch Set \d+: Patch Set \d+ was rebased)",
    re.I,
)


def gerrit_json(path: str, params: dict | list | None = None):
    resp = requests.get(
        f"{GERRIT_HOST}{path}",
        auth=GERRIT_AUTH,
        params=params,
        timeout=40,
    )
    if resp.status_code >= 400:
        raise RuntimeError(f"Gerrit GET {path} -> {resp.status_code}: {resp.text[:300]}")
    text = resp.text
    if text.startswith(")]}'"):
        text = text[4:]
    return json.loads(text)


def _query(q: str, n: int = 30) -> list[dict]:
    data = gerrit_json(
        "/a/changes/",
        params={
            "q": q,
            "n": n,
            "o": [
                "MESSAGES",
                "DETAILED_LABELS",
                "DETAILED_ACCOUNTS",
                "CURRENT_REVISION",
            ],
        },
    )
    return data if isinstance(data, list) else []


def _msg_time(msg: dict) -> datetime | None:
    raw = str(msg.get("date") or "")[:19]
    try:
        return datetime.strptime(raw, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return None


def _patchset(change: dict) -> int | None:
    current = change.get("current_revision")
    rev = (change.get("revisions") or {}).get(current) or {}
    number = rev.get("_number")
    return int(number) if number else None


def _on_patchset(msg: dict, patchset: int | None) -> bool:
    if patchset is None:
        return True
    rev = msg.get("_revision_number")
    return rev in (None, patchset)


def _username(msg: dict) -> str:
    author = msg.get("author") or {}
    return str(author.get("username") or "")


def _self_voted(change: dict, username: str) -> bool:
    cr = (change.get("labels") or {}).get("Code-Review") or {}
    for item in cr.get("all") or []:
        if item.get("username") == username and item.get("value") not in (None, 0):
            return True
    return False


def _is_reviewer(change: dict, username: str) -> bool:
    people = (change.get("reviewers") or {}).get("REVIEWER") or []
    return any(person.get("username") == username for person in people)


def _parse_ci(msg: dict) -> tuple[str, str, str, str] | None:
    text = msg.get("message") or ""
    tag = str(msg.get("tag") or "")
    author = _username(msg)
    if "jenkins" not in tag and author != "jenkins" and "Build" not in text and ": FAILURE" not in text:
        return None
    match = CI_STATUS_RE.search(text)
    if not match:
        if re.search(r"\bBuild Failed\b", text) and "ABORTED" not in text:
            job_match = JOB_RE.search(text)
            job = job_match.group(1) if job_match else "jenkins"
            build = job_match.group(2) if job_match else ""
            url = job_match.group(0) if job_match else ""
            return job, "FAILURE", url, build
        return None
    return match.group(1), match.group(3), match.group(0).split(" : ")[0].strip(), match.group(2)


def _latest_ci_failures(change: dict) -> list[dict]:
    patchset = _patchset(change)
    latest: dict[str, tuple[datetime, dict, str, str, str]] = {}
    for msg in change.get("messages") or []:
        if not _on_patchset(msg, patchset):
            continue
        parsed = _parse_ci(msg)
        if not parsed:
            continue
        job, status, url, build = parsed
        stamp = _msg_time(msg) or datetime.min
        prev = latest.get(job)
        if prev is None or stamp >= prev[0]:
            latest[job] = (stamp, msg, status, url, build)
    failed = []
    for job, (stamp, msg, status, url, build) in latest.items():
        if status in ("FAILURE", "UNSTABLE"):
            failed.append(
                {
                    "job": job,
                    "status": status,
                    "url": url,
                    "build": build,
                    "msg": msg,
                    "time": stamp,
                }
            )
    return failed


def _is_human_comment(msg: dict, username: str) -> bool:
    author = _username(msg)
    tag = str(msg.get("tag") or "")
    text = (msg.get("message") or "").strip()
    if not text or not author or author in (username, "jenkins"):
        return False
    if tag.startswith("autogenerated:"):
        return False
    if NOISE_RE.search(text):
        return False
    if text.startswith("Uploaded patch set"):
        return False
    return True


def _excerpt(text: str, limit: int = 400) -> str:
    lines = []
    for raw in (text or "").splitlines():
        line = raw.strip()
        if not line or line.startswith("Patch Set "):
            continue
        lines.append(line)
        if sum(len(item) for item in lines) > limit:
            break
    body = "\n".join(lines).strip()
    if len(body) > limit:
        body = body[: limit - 1] + "…"
    return body or (text or "").strip()[:limit]


def _log_tail(build_url: str) -> str:
    api = build_url.rstrip("/") + "/logText/progressiveText"
    head = requests.get(api, params={"start": 0}, timeout=20)
    head.raise_for_status()
    size = int(head.headers.get("X-Text-Size") or 0)
    start = max(0, size - 80000)
    if start == 0:
        return head.text
    resp = requests.get(api, params={"start": start}, timeout=30)
    resp.raise_for_status()
    return resp.text


def _key_errors(build_url: str) -> tuple[str | None, list[str]]:
    try:
        text = _log_tail(build_url)
    except Exception as exc:  # noqa: BLE001
        return None, [f"(日志没拉到: {exc})"]
    stage = None
    picked: list[str] = []
    for raw in text.splitlines():
        line = ANSI_RE.sub("", raw).strip()
        line = TS_RE.sub("", line)
        if line.startswith("[Pipeline]"):
            continue
        found = re.search(r"Failed in branch.*stage:\s*(.+)", line)
        if found:
            stage = found.group(1).strip()
            if line not in picked:
                picked.append(line[:220])
            continue
        if ERROR_RE.search(line) and "If status" not in line:
            if line not in picked:
                picked.append(line[:220])
    return stage, picked[-6:]


def _change_url(number: int | str) -> str:
    return f"{GERRIT_HOST}/#/c/{number}/"


def _can_notify() -> bool:
    if (os.getenv("LUX_NOTIFY_CHAT_ID") or "").strip() or (os.getenv("LUX_NOTIFY_OPEN_ID") or "").strip():
        return True
    client = get_redis()
    return bool(client.get("lux_notify_chat") or client.get("lux_notify_open_id"))


def _mark_old(client, key: str, ttl: int, dry: bool) -> None:
    if not dry:
        client.set(key, "1", ex=ttl)


def _within_lookback(stamp: datetime | None, cutoff: datetime | None) -> bool:
    if cutoff is None or stamp is None:
        return True
    return stamp >= cutoff


def poll_once(*, dry: bool = False) -> None:
    if not GERRIT_USER or not GERRIT_PWD:
        print("⚠️ 未配置 GERRIT_USER / GERRIT_HTTP_PWD，跳过")
        return
    me = gerrit_json("/a/accounts/self")
    username = str(me.get("username") or GERRIT_USER)
    client = get_redis()
    booted = bool(client.get("lux_watch_booted"))
    lookback_h = int(os.getenv("LUX_WATCH_LOOKBACK_HOURS", "24"))
    cutoff = None if booted else datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(hours=lookback_h)
    ttl = 30 * 24 * 3600
    age = os.getenv("LUX_REVIEW_MAX_AGE", "30d").strip() or "30d"
    print(f"轮询 Gerrit user={username} booted={booted} dry={dry}")

    _poll_reviewer(client, username, age, ttl, dry)
    _poll_own(client, username, cutoff, ttl, dry)

    if not dry and not booted and _can_notify():
        client.set("lux_watch_booted", "1")


def _poll_reviewer(client, username: str, age: str, ttl: int, dry: bool) -> None:
    query = f"reviewer:{username} status:open -owner:{username} -age:{age}"
    try:
        changes = _query(query, n=40)
    except Exception as exc:  # noqa: BLE001
        print(f"⚠️ reviewer 查询失败: {exc}")
        return
    sent = 0
    limit = int(os.getenv("LUX_REVIEW_PING_LIMIT", "8"))
    for change in changes:
        if change.get("work_in_progress"):
            continue
        number = change.get("_number")
        if not number or not _is_reviewer(change, username) or _self_voted(change, username):
            continue
        revision = str(change.get("current_revision") or "")
        key = f"lux_review_pinged:{number}:{revision}"
        if client.get(key):
            continue
        subject = (change.get("subject") or "").strip()
        text = (
            f"你被加为 reviewer，这条还没评。\n"
            f"Change: {number}\n"
            f"标题: {subject}\n"
            f"链接: {_change_url(number)}\n"
            f"回「评」我就开始评审。"
        )
        print(f"评审提醒 {number} {subject}")
        if dry:
            sent += 1
            continue
        if sent >= limit:
            break
        if not _can_notify():
            if client.set("lux_watch_need_target", "1", nx=True, ex=86400):
                print("⚠️ 还没有飞书通知对象。先在飞书里跟机器人说一句话，或设置 LUX_NOTIFY_CHAT_ID")
            return
        try:
            message_id = notify_feishu(text)
        except Exception as exc:  # noqa: BLE001
            print(f"⚠️ 评审提醒发送失败 {number}: {exc}")
            break
        if not message_id:
            print("⚠️ 还没有飞书通知对象。先在飞书里跟机器人说一句话，或设置 LUX_NOTIFY_CHAT_ID")
            return
        save_review_offer(
            client,
            {
                "message_id": message_id,
                "change_id": str(number),
                "revision": revision,
                "subject": subject,
            },
        )
        client.set(key, "1", ex=ttl)
        sent += 1
    if sent:
        print(f"评审提醒 {sent} 条")


def _poll_own(client, username: str, cutoff: datetime | None, ttl: int, dry: bool) -> None:
    try:
        changes = _query(f"owner:{username} status:open", n=30)
    except Exception as exc:  # noqa: BLE001
        print(f"⚠️ 自己的 change 查询失败: {exc}")
        return
    for change in changes:
        number = change.get("_number")
        if not number:
            continue
        subject = (change.get("subject") or "").strip()
        patchset = _patchset(change)
        for item in _latest_ci_failures(change):
            msg = item["msg"]
            msg_id = str(msg.get("id") or "")
            if not msg_id:
                continue
            key = f"lux_watch_sent:{msg_id}"
            if client.get(key):
                continue
            if not _within_lookback(item["time"], cutoff):
                _mark_old(client, key, ttl, dry)
                continue
            stage, errors = (item["job"], [])
            if item["url"]:
                found_stage, errors = _key_errors(item["url"])
                if found_stage:
                    stage = found_stage
            stage_line = stage
            if item["build"]:
                stage_line = f"{stage}（{item['job']} #{item['build']}）"
            body = (
                f"CI 失败\n"
                f"Change: {number}\n"
                f"阶段: {stage_line}\n"
                f"链接: {_change_url(number)}\n"
                f"标题: {subject}"
            )
            if errors:
                body += "\n报错:\n" + "\n".join(errors)
            print(f"CI {number} {stage_line}")
            if dry:
                print(body)
                continue
            if _send_mark(client, key, body, ttl):
                continue
            return
        for msg in change.get("messages") or []:
            if not _on_patchset(msg, patchset) or not _is_human_comment(msg, username):
                continue
            msg_id = str(msg.get("id") or "")
            if not msg_id or client.get(f"lux_watch_sent:{msg_id}"):
                continue
            if not _within_lookback(_msg_time(msg), cutoff):
                _mark_old(client, f"lux_watch_sent:{msg_id}", ttl, dry)
                continue
            excerpt = _excerpt(msg.get("message") or "")
            author = _username(msg) or "某人"
            body = (
                f"新评论\n"
                f"Change: {number}\n"
                f"作者: {author}\n"
                f"链接: {_change_url(number)}\n"
                f"标题: {subject}\n"
                f"摘录:\n{excerpt}"
            )
            print(f"评论 {number} by {author}")
            if dry:
                print(body)
                continue
            if _send_mark(client, f"lux_watch_sent:{msg_id}", body, ttl):
                continue
            return


def _send_mark(client, key: str, text: str, ttl: int) -> bool:
    try:
        message_id = notify_feishu(text)
    except Exception as exc:  # noqa: BLE001
        print(f"⚠️ 通知发送失败: {exc}")
        return False
    if not message_id:
        print("⚠️ 还没有飞书通知对象。先在飞书里跟机器人说一句话，或设置 LUX_NOTIFY_CHAT_ID")
        return False
    client.set(key, "1", ex=ttl)
    return True


def main() -> None:
    parser = argparse.ArgumentParser(description="轮询 Gerrit reviewer / CI / 评论")
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--dry", action="store_true")
    args = parser.parse_args()
    if os.getenv("LUX_WATCH_ENABLED", "1").strip().lower() in ("0", "false", "no", "off"):
        print("LUX_WATCH_ENABLED 已关闭")
        if args.once:
            return
        while True:
            time.sleep(3600)
    interval = int(os.getenv("LUX_GERRIT_POLL_SECONDS", "3600"))
    if args.once:
        poll_once(dry=args.dry)
        return
    print(f"🟢 Gerrit 轮询启动 interval={interval}s")
    while True:
        try:
            poll_once(dry=False)
        except Exception as exc:  # noqa: BLE001
            print(f"❌ 本轮轮询失败: {exc}")
        time.sleep(max(30, interval))


if __name__ == "__main__":
    main()
