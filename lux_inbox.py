"""飞书消息轮询兜底：长连接收不到 receive_v1 时，仍能扫到 @lux 的下载/扫描/评审。

飞书若启用了「Agent / 智能助手」，可能先自动回旧话术，且不把事件推给本机长连接。
本进程直接拉群聊消息列表，按 message_id 去重后入队。
"""

from __future__ import annotations

import json
import os
import time
from datetime import datetime, timedelta
from pathlib import Path

import requests
from dotenv import load_dotenv

_ENV_PATH = Path(__file__).resolve().parent / ".env"
load_dotenv(_ENV_PATH, override=True)

from common import (
    REDIS_QUEUE_KEY,
    SCAN_REQUEST_KEY,
    claim_message,
    extract_change_id,
    extract_project_link,
    get_feishu_token,
    get_redis,
    has_download_intent,
    has_review_intent,
    has_scan_intent,
    parse_message_text,
    remember_notify_target,
    reply_feishu_text,
    review_go_change,
)

POLL_CHATS_KEY = "lux_poll_chats"
CURSOR_PREFIX = "lux_inbox_seen"


def _headers() -> dict[str, str]:
    return {"Authorization": f"Bearer {get_feishu_token()}"}


def _chat_ids(redis_client) -> list[str]:
    ids: list[str] = []
    env = os.getenv("LUX_POLL_CHAT_IDS", "").strip()
    if env:
        ids.extend(part.strip() for part in env.split(",") if part.strip())
    notify = (os.getenv("LUX_NOTIFY_CHAT_ID") or "").strip() or (redis_client.get("lux_notify_chat") or "")
    if notify:
        ids.append(notify)
    cached = redis_client.smembers(POLL_CHATS_KEY) or set()
    ids.extend(str(item) for item in cached)
    # 去重保序
    seen: set[str] = set()
    out: list[str] = []
    for chat_id in ids:
        if chat_id and chat_id not in seen:
            seen.add(chat_id)
            out.append(chat_id)
    return out


def _list_messages(chat_id: str, page_size: int = 20) -> list[dict]:
    resp = requests.get(
        "https://open.feishu.cn/open-apis/im/v1/messages",
        headers=_headers(),
        params={
            "container_id_type": "chat",
            "container_id": chat_id,
            "page_size": page_size,
            "sort_type": "ByCreateTimeDesc",
        },
        timeout=20,
    )
    resp.raise_for_status()
    data = resp.json()
    if data.get("code") != 0:
        raise RuntimeError(f"拉消息失败 chat={chat_id}: {data}")
    return list((data.get("data") or {}).get("items") or [])


def _message_text(msg: dict) -> str:
    body = msg.get("body") or {}
    content = body.get("content")
    return parse_message_text(content)


def _sender_open_id(msg: dict) -> str:
    sender = msg.get("sender") or {}
    if sender.get("sender_type") != "user":
        return ""
    return str(sender.get("id") or "")


def _enqueue(redis_client, task: dict) -> None:
    redis_client.lpush(REDIS_QUEUE_KEY, json.dumps(task, ensure_ascii=False))


def _handle(redis_client, chat_id: str, msg: dict) -> None:
    message_id = msg.get("message_id") or ""
    if not message_id:
        return
    if not claim_message(redis_client, message_id):
        return

    text = _message_text(msg)
    sender = _sender_open_id(msg)
    remember_notify_target(chat_id, sender or None)
    redis_client.sadd(POLL_CHATS_KEY, chat_id)

    print(f"📥 inbox 命中 chat={chat_id} message_id={message_id} text={text[:200]!r}")

    if has_scan_intent(text) and not has_download_intent(text):
        redis_client.lpush(
            SCAN_REQUEST_KEY,
            json.dumps(
                {
                    "chat_id": chat_id,
                    "message_id": message_id,
                    "sender_open_id": sender,
                    "source": "inbox",
                },
                ensure_ascii=False,
            ),
        )
        reply_feishu_text(
            message_id,
            "收到，开始扫描飞书项目。已下载的闭环数据会跳过。",
            chat_id=chat_id,
            reply_in_thread=True,
        )
        print("✅ inbox 扫描请求入队")
        return

    project = extract_project_link(text)
    if project and (has_download_intent(text) or not has_review_intent(text)):
        _enqueue(
            redis_client,
            {
                "action": "download",
                "project_url": project["url"],
                "simple_name": project["simple_name"],
                "work_item_type_key": project["work_item_type_key"],
                "work_item_id": project["work_item_id"],
                "chat_id": chat_id,
                "sender_open_id": sender,
                "message_id": message_id,
                "raw_text": text[:2000],
                "source": "inbox",
            },
        )
        reply_feishu_text(
            message_id,
            "收到，开始从飞书项目里找数据并下载…",
            chat_id=chat_id,
            reply_in_thread=True,
        )
        print(f"✅ inbox 下载入队 work_item={project['work_item_id']}")
        return

    go, go_change = review_go_change(text)
    if go and go_change:
        _enqueue(
            redis_client,
            {
                "action": "review",
                "change_id": go_change,
                "chat_id": chat_id,
                "sender_open_id": sender,
                "message_id": message_id,
                "raw_text": text[:500],
                "source": "inbox",
            },
        )
        reply_feishu_text(
            message_id,
            f"收到，开始评审 Gerrit {go_change}…",
            chat_id=chat_id,
            reply_in_thread=True,
        )
        return

    if has_review_intent(text):
        change_id = extract_change_id(text)
        if change_id:
            _enqueue(
                redis_client,
                {
                    "action": "review",
                    "change_id": change_id,
                    "chat_id": chat_id,
                    "sender_open_id": sender,
                    "message_id": message_id,
                    "raw_text": text[:500],
                    "source": "inbox",
                },
            )
            reply_feishu_text(
                message_id,
                f"收到，开始评审 Gerrit {change_id}…",
                chat_id=chat_id,
                reply_in_thread=True,
            )
            print(f"✅ inbox 评审入队 change_id={change_id}")


def poll_once(redis_client, *, lookback_minutes: int) -> None:
    cutoff = datetime.now() - timedelta(minutes=lookback_minutes)
    chats = _chat_ids(redis_client)
    if not chats:
        print("⚠️ inbox 没有可轮询的 chat_id。请在 .env 设 LUX_POLL_CHAT_IDS，或先在群里跟机器人说过话。")
        return
    for chat_id in chats:
        try:
            messages = _list_messages(chat_id)
        except Exception as exc:  # noqa: BLE001
            print(f"⚠️ inbox 拉消息失败 chat={chat_id}: {exc}")
            continue
        for msg in messages:
            sender = msg.get("sender") or {}
            if sender.get("sender_type") != "user":
                continue
            try:
                created = datetime.fromtimestamp(int(msg.get("create_time") or 0) / 1000.0)
            except (TypeError, ValueError, OSError):
                continue
            if created < cutoff:
                continue
            text = _message_text(msg)
            if not (
                has_scan_intent(text)
                or has_download_intent(text)
                or has_review_intent(text)
                or extract_project_link(text)
                or review_go_change(text)[0]
            ):
                continue
            try:
                _handle(redis_client, chat_id, msg)
            except Exception as exc:  # noqa: BLE001
                print(f"❌ inbox 处理失败 message_id={msg.get('message_id')}: {exc}")


def main() -> None:
    mode = os.getenv("LUX_INBOX_ENABLED", "auto").strip().lower()
    if mode in ("0", "false", "no", "off"):
        print("LUX_INBOX_ENABLED=off，inbox 不轮询（依赖飞书长连接推送）")
        while True:
            time.sleep(3600)

    interval = int(os.getenv("LUX_INBOX_POLL_SECONDS", "120"))
    lookback = int(os.getenv("LUX_INBOX_LOOKBACK_MINUTES", "10"))
    stale_after = int(os.getenv("LUX_INBOX_WS_STALE_SECONDS", "300"))
    redis_client = get_redis()
    known = "oc_43eda33fff1734bedbbf9c4a9f0e4ede"
    redis_client.sadd(POLL_CHATS_KEY, known)
    print(
        f"🟢 lux inbox 启动 mode={mode} interval={interval}s "
        f"lookback={lookback}m stale_after={stale_after}s "
        f"chats={_chat_ids(redis_client)}"
    )
    print(
        "说明：inbox 只是长连接收不到 im.message.receive_v1 时的降级。"
        "正常应靠 feishu_bot 的 WS 推送；请保持只跑一份 lux，并关掉飞书应用里的 Agent/智能助手抢答。"
    )
    while True:
        try:
            if mode == "auto":
                last = redis_client.get("lux_ws_last_receive_v1")
                if last:
                    age = time.time() - float(last)
                    if age < stale_after:
                        print(f"⏭ WS 最近 {age:.0f}s 内收到过 receive_v1，跳过轮询")
                        time.sleep(max(5, interval))
                        continue
                    print(f"⚠️ WS 已 {age:.0f}s 未收到 receive_v1，启用 inbox 降级轮询")
                else:
                    # 启动后先等一会儿，给长连接机会
                    started = float(redis_client.get("lux_ws_started_at") or 0)
                    if started and time.time() - started < stale_after:
                        time.sleep(max(5, interval))
                        continue
                    print("⚠️ 尚未见到 WS receive_v1，启用 inbox 降级轮询")
            poll_once(redis_client, lookback_minutes=lookback)
        except Exception as exc:  # noqa: BLE001
            print(f"❌ inbox 本轮失败: {exc}")
        time.sleep(max(30, interval))


if __name__ == "__main__":
    main()
