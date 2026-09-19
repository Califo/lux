"""飞书长连接：收消息入队 + 监听表情确认（发布/取消/重评）。"""

from __future__ import annotations

import json
import os
import threading
import time
from pathlib import Path

import lark_oapi as lark
from dotenv import load_dotenv

_ENV_PATH = Path(__file__).resolve().parent / ".env"
load_dotenv(_ENV_PATH, override=True)

from common import (
    EMOJI_DISCARD,
    EMOJI_PUBLISH,
    EMOJI_REREVIEW,
    REDIS_QUEUE_KEY,
    SCAN_REQUEST_KEY,
    add_feishu_reaction,
    claim_message,
    consume_review_offer,
    extract_change_id,
    extract_project_link,
    format_capabilities_text,
    get_bot_open_id,
    get_redis,
    has_download_intent,
    has_review_intent,
    has_scan_intent,
    list_review_offers,
    load_review_offer,
    parse_message_text,
    remember_notify_target,
    reply_feishu_text,
    resolve_pending_review,
    review_go_change,
    take_pending_review,
    wants_capability_help,
)

# common 已 load 过；再覆盖一次，保证本进程一定读到 lux/.env
load_dotenv(_ENV_PATH, override=True)

FEISHU_APP_ID = os.getenv("FEISHU_APP_ID", "")
FEISHU_APP_SECRET = os.getenv("FEISHU_APP_SECRET", "")
FEISHU_ACK_EMOJI = os.getenv("FEISHU_ACK_EMOJI", "OnIt")

BOT_OPEN_ID = get_bot_open_id()


def _redis():
    return get_redis()


def _is_addressed_to_bot(message) -> bool:
    """群聊里飞书若已把事件推过来，通常已是 @ 本机器人；不要因 open_id 配错把消息丢掉。"""
    chat_type = getattr(message, "chat_type", None) or ""
    if chat_type in ("p2p", "private"):
        return True
    mentions = getattr(message, "mentions", None) or []
    if mentions:
        if not BOT_OPEN_ID:
            return True
        for mention in mentions:
            mid = getattr(mention, "id", None)
            open_id = getattr(mid, "open_id", None) if mid is not None else None
            if open_id == BOT_OPEN_ID:
                return True
        # open_id 对不上也继续：事件能到本连接，基本就是找我们的
        print(
            f"⚠️ mentions open_id 与 BOT_OPEN_ID={BOT_OPEN_ID} 不一致，仍继续处理"
        )
        return True
    text = parse_message_text(getattr(message, "content", None))
    if (
        has_scan_intent(text)
        or has_download_intent(text)
        or has_review_intent(text)
        or extract_project_link(text)
        or review_go_change(text)[0]
    ):
        return True
    return False


def _enqueue_task(task: dict) -> None:
    _redis().lpush(REDIS_QUEUE_KEY, json.dumps(task, ensure_ascii=False))


def _reply(message_id: str | None, chat_id: str | None, text: str) -> None:
    try:
        reply_feishu_text(message_id, text, chat_id=chat_id, reply_in_thread=True)
    except Exception as exc:  # noqa: BLE001
        print(f"回执失败: {exc}")


def _capability_reply(prefix: str = "") -> str:
    body = format_capabilities_text()
    if prefix:
        return f"{prefix.rstrip()}\n\n{body}"
    return body


def _resolve_review_offer(message, change_id: str | None):
    client = _redis()
    for mid in (
        getattr(message, "parent_id", None),
        getattr(message, "root_id", None),
        getattr(message, "thread_id", None),
    ):
        offer = load_review_offer(client, mid)
        if offer:
            return offer
    if change_id:
        pointer = client.hget("lux_review_offers", f"change:{change_id}")
        offer = load_review_offer(client, pointer)
        if offer:
            return offer
        return {
            "change_id": change_id,
            "message_id": "",
            "subject": "",
        }
    offers = list_review_offers(client)
    if len(offers) == 1:
        return offers[0]
    if len(offers) > 1:
        return offers
    return None


def _handle_message(data: lark.im.v1.P2ImMessageReceiveV1) -> None:
    """飞书要求 3 秒内 ACK；业务一律丢线程，避免同步调 API 触发重推/丢到其它连接。"""
    try:
        _redis().set("lux_ws_last_receive_v1", str(time.time()))
    except Exception:
        pass
    threading.Thread(target=_handle_message_work, args=(data,), daemon=True).start()


def _handle_message_work(data: lark.im.v1.P2ImMessageReceiveV1) -> None:
    event = data.event
    if event is None or event.message is None:
        return
    message = event.message
    text_preview = parse_message_text(message.content)[:200]
    print(
        f"📩 事件到达 chat={getattr(message, 'chat_id', None)} "
        f"type={getattr(message, 'chat_type', None)} "
        f"mentions={len(getattr(message, 'mentions', None) or [])} "
        f"text={text_preview!r}"
    )
    if not _is_addressed_to_bot(message):
        print("⏭ 未判定为对本机器人说话，忽略")
        return

    text = parse_message_text(message.content)
    chat_id = message.chat_id
    message_id = getattr(message, "message_id", None)
    sender = None
    if event.sender and event.sender.sender_id:
        sender = event.sender.sender_id.open_id
    print(f"📩 处理消息 message_id={message_id} text={text[:400]!r}")
    remember_notify_target(chat_id, sender)

    if wants_capability_help(text) and not has_review_intent(text) and not has_download_intent(text) and not has_scan_intent(text):
        _reply(message_id, chat_id, _capability_reply("你好～"))
        return

    if has_scan_intent(text) and not has_download_intent(text):
        def _scan_work() -> None:
            try:
                if not claim_message(_redis(), message_id):
                    print(f"⏭ 重复消息已忽略 message_id={message_id}")
                    return
                _redis().lpush(
                    SCAN_REQUEST_KEY,
                    json.dumps(
                        {
                            "chat_id": chat_id,
                            "message_id": message_id,
                            "sender_open_id": sender,
                        },
                        ensure_ascii=False,
                    ),
                )
                print(f"✅ 扫描请求入队 chat_id={chat_id}")
                _reply(message_id, chat_id, "收到，开始扫描飞书项目。已下载的闭环数据会跳过。")
            except Exception as exc:  # noqa: BLE001
                print(f"❌ 扫描入队失败: {exc}")
                _reply(message_id, chat_id, f"扫描没能开始：{exc}")

        threading.Thread(target=_scan_work, daemon=True).start()
        return

    project = extract_project_link(text)
    if has_download_intent(text) and not project:
        _reply(
            message_id,
            chat_id,
            _capability_reply(
                "看起来你想下载数据，但我没找到飞书项目链接。"
                "把工作项链接发我就行，例如：\n"
                "· 下载 https://project.feishu.cn/空间/issue/detail/123456789"
            ),
        )
        return
    if project and (has_download_intent(text) or not has_review_intent(text)):
        task = {
            "action": "download",
            "project_url": project["url"],
            "simple_name": project["simple_name"],
            "work_item_type_key": project["work_item_type_key"],
            "work_item_id": project["work_item_id"],
            "chat_id": chat_id,
            "sender_open_id": sender,
            "message_id": message_id,
            "raw_text": text[:2000],
        }

        def _download_work() -> None:
            try:
                if not claim_message(_redis(), message_id):
                    print(f"⏭ 重复消息已忽略 message_id={message_id}")
                    return
                _enqueue_task(task)
                print(
                    f"✅ 下载任务入队 work_item={project['work_item_id']} "
                    f"chat_id={chat_id}"
                )
                try:
                    add_feishu_reaction(message_id, FEISHU_ACK_EMOJI)
                except Exception as react_exc:  # noqa: BLE001
                    print(f"⚠️ 表情回应失败，回退话题文字: {react_exc}")
                    _reply(message_id, chat_id, "收到，开始从飞书项目里找数据并下载…")
            except Exception as exc:  # noqa: BLE001
                print(f"❌ 下载入队失败: {exc}")
                _reply(message_id, chat_id, f"入队失败：{exc}")

        threading.Thread(target=_download_work, daemon=True).start()
        return

    go, go_change = review_go_change(text)
    if go:
        def _go_work() -> None:
            try:
                if not claim_message(_redis(), message_id):
                    print(f"⏭ 重复消息已忽略 message_id={message_id}")
                    return
                offer = _resolve_review_offer(message, go_change)
                if isinstance(offer, list):
                    lines = [
                        f"· {item.get('change_id')} {item.get('subject') or ''}".rstrip()
                        for item in offer[:8]
                    ]
                    _reply(
                        message_id,
                        chat_id,
                        "有多条还没评，回复「评 123456」指定一条：\n" + "\n".join(lines),
                    )
                    return
                if not offer:
                    _reply(
                        message_id,
                        chat_id,
                        "没有待确认的评审。等我通知你被加为 reviewer 后回「评」，或直接发 change 链接。",
                    )
                    return
                change_id = str(offer["change_id"])
                task = {
                    "action": "review",
                    "change_id": change_id,
                    "chat_id": chat_id,
                    "sender_open_id": sender,
                    "message_id": message_id,
                    "raw_text": text[:500],
                }
                consume_review_offer(_redis(), offer)
                _enqueue_task(task)
                print(f"✅ 确认评审入队 change_id={change_id}")
                _reply(message_id, chat_id, f"收到，开始评审 Gerrit {change_id}…")
            except Exception as exc:  # noqa: BLE001
                print(f"❌ 确认评审失败: {exc}")
                _reply(message_id, chat_id, f"没能开始评审：{exc}")

        threading.Thread(target=_go_work, daemon=True).start()
        return

    if not has_review_intent(text):
        if text.strip():
            _reply(
                message_id,
                chat_id,
                _capability_reply(
                    "这个问题我暂时还不会处理，不过你可以试试下面这些我已经会的："
                ),
            )
        return

    change_id = extract_change_id(text)
    if not change_id:
        _reply(
            message_id,
            chat_id,
            _capability_reply(
                "看起来你想做代码评审，但我没找到 Gerrit change 号。"
                "把链接或数字 change id 发我就行，例如：\n"
                "· review https://gerrit.uisee.ai/#/c/123456/\n"
                "· 检查代码 123456"
            ),
        )
        return

    task = {
        "action": "review",
        "change_id": change_id,
        "chat_id": chat_id,
        "sender_open_id": sender,
        "message_id": message_id,
        "raw_text": text[:500],
    }

    def _work() -> None:
        try:
            if not claim_message(_redis(), message_id):
                print(f"⏭ 重复消息已忽略 message_id={message_id}")
                return
            _enqueue_task(task)
            print(f"✅ 任务入队 change_id={change_id}, chat_id={chat_id}")
            try:
                add_feishu_reaction(message_id, FEISHU_ACK_EMOJI)
            except Exception as react_exc:  # noqa: BLE001
                print(f"⚠️ 表情回应失败，回退话题文字: {react_exc}")
                _reply(message_id, chat_id, f"收到，开始评审 Gerrit {change_id}…")
        except Exception as exc:  # noqa: BLE001
            print(f"❌ 入队/回执失败: {exc}")
            _reply(message_id, chat_id, f"入队失败：{exc}")

    threading.Thread(target=_work, daemon=True).start()


def _handle_reaction(data: lark.im.v1.P2ImMessageReactionCreatedV1) -> None:
    """用户在预览/原消息上点 ✅ / ❌ / OnIt。"""
    event = data.event
    if event is None:
        return

    operator_type = (event.operator_type or "").lower()
    # 忽略机器人自己预置的表情
    if operator_type == "app":
        print(f"⏭ 忽略 app 表情 message_id={event.message_id} emoji={getattr(getattr(event, 'reaction_type', None), 'emoji_type', None)}")
        return

    message_id = event.message_id
    emoji = None
    if event.reaction_type is not None:
        emoji = event.reaction_type.emoji_type
    operator = None
    if event.user_id is not None:
        operator = event.user_id.open_id

    print(
        f"📩 收到表情 message_id={message_id} emoji={emoji} "
        f"operator={operator} operator_type={operator_type}"
    )
    if not message_id or not emoji:
        return

    preview_id, pending = resolve_pending_review(_redis(), message_id)
    if not pending or not preview_id:
        if emoji in (EMOJI_PUBLISH, EMOJI_DISCARD, EMOJI_REREVIEW):
            print(f"⏭ 表情无对应 pending message_id={message_id} emoji={emoji}")
        return

    requester = pending.get("requester_open_id")
    if requester and operator and operator != requester:
        _reply(
            pending.get("root_message_id"),
            pending.get("chat_id"),
            "只有发起这次 review 的人可以确认发布/取消哦。",
        )
        return

    root_id = pending.get("root_message_id")
    chat_id = pending.get("chat_id")

    def _work() -> None:
        try:
            if emoji == EMOJI_PUBLISH:
                # 先占住 pending，整包交给 local_agent，避免两边抢删 Redis
                taken = take_pending_review(_redis(), preview_id)
                if not taken:
                    print(f"⏭ publish 时 pending 已被取走 preview_id={preview_id}")
                    return
                _enqueue_task(
                    {
                        "action": "publish",
                        "preview_message_id": preview_id,
                        "chat_id": chat_id,
                        "message_id": root_id,
                        "pending": taken,
                    }
                )
                _reply(root_id, chat_id, "收到 ✅，正在发布到 Gerrit…")
                print(f"✅ 确认发布 change={taken.get('change_id')}")
            elif emoji == EMOJI_DISCARD:
                taken = take_pending_review(_redis(), preview_id)
                if not taken:
                    return
                _reply(root_id, chat_id, "已取消 ❌，不会发布到 Gerrit。")
                print(f"⏭ 取消发布 change={taken.get('change_id')}")
            elif emoji == EMOJI_REREVIEW:
                taken = take_pending_review(_redis(), preview_id)
                if not taken:
                    return
                _enqueue_task(
                    {
                        "action": "rereview",
                        "change_id": taken["change_id"],
                        "chat_id": taken.get("chat_id"),
                        "message_id": taken.get("root_message_id"),
                        "sender_open_id": taken.get("requester_open_id"),
                    }
                )
                _reply(root_id, chat_id, "好的，重新评审中…")
                print(f"🔄 重新评审 change={taken.get('change_id')}")
            else:
                print(f"⏭ 忽略无关表情 emoji={emoji}")
                return
        except Exception as exc:  # noqa: BLE001
            print(f"❌ 处理确认表情失败: {exc}")
            _reply(root_id, chat_id, f"处理确认失败：{exc}")

    threading.Thread(target=_work, daemon=True).start()


def main() -> None:
    if not FEISHU_APP_ID or not FEISHU_APP_SECRET:
        raise SystemExit("请在 .env 中配置 FEISHU_APP_ID / FEISHU_APP_SECRET")

    try:
        client = _redis()
        if not client.ping():
            raise RuntimeError("Redis ping 失败")
        print("✅ Redis 连接正常 (protocol=2)")
    except Exception as exc:  # noqa: BLE001
        raise SystemExit(f"Redis 不可用，请先修好再启动 bot: {exc}") from exc

    print(f"🟢 飞书长连接启动中… app_id={FEISHU_APP_ID} bot_open_id={BOT_OPEN_ID}")
    print(
        "⚠️ 飞书长连接是集群模式：同一 app 只能有一条有效消费者。"
        "本机/其它电脑不要再跑旧 reviewer/lux，也不要频繁重启 bot，否则 receive_v1 会被幽灵连接抢走。"
    )
    print(f"   env={_ENV_PATH}")
    try:
        _redis().set("lux_ws_started_at", str(time.time()))
    except Exception as exc:  # noqa: BLE001
        print(f"⚠️ 写 WS 启动标记失败: {exc}")
    print(
        "请订阅事件: im.message.receive_v1 + im.message.reaction.created_v1"
    )
    print(
        f"确认表情: 发布={EMOJI_PUBLISH} 取消={EMOJI_DISCARD} 重评={EMOJI_REREVIEW}"
    )

    def _ignore_message_read(_data) -> None:
        return

    event_handler = (
        lark.EventDispatcherHandler.builder("", "")
        .register_p2_im_message_receive_v1(_handle_message)
        .register_p2_im_message_reaction_created_v1(_handle_reaction)
        .register_p2_im_message_message_read_v1(_ignore_message_read)
        .build()
    )

    _orig_do = event_handler._do_without_validation

    def _log_all_events(payload: bytes):
        try:
            pl = json.loads(payload.decode("utf-8"))
            header = pl.get("header") or {}
            event_type = header.get("event_type") or (pl.get("event") or {}).get("type")
            event = pl.get("event") or {}
            extra = ""
            if event_type == "im.message.message_read_v1":
                # 读回执里常带 message_id_list，可对照是否有人已读了我们没收到的 @ 消息
                ids = (
                    event.get("message_id_list")
                    or (event.get("reader") or {}).get("message_id_list")
                    or []
                )
                if not ids and isinstance(event.get("message_ids"), list):
                    ids = event.get("message_ids")
                # 兜底：从整段 JSON 里抓 om_ 前缀
                if not ids:
                    import re as _re
                    ids = list(dict.fromkeys(_re.findall(r"om_[a-z0-9]+", payload.decode("utf-8", "ignore"))))[:8]
                reader = event.get("reader") or {}
                extra = f" reader={reader.get('open_id') or reader.get('user_id') or '?'} ids={ids}"
            elif event_type == "im.message.receive_v1":
                msg = (event.get("message") or {})
                extra = f" chat={msg.get('chat_id')} mid={msg.get('message_id')} ctype={msg.get('chat_type')}"
                try:
                    _redis().set("lux_ws_last_receive_v1", str(time.time()))
                except Exception:
                    pass
            print(f"📡 WS 原始事件 type={event_type} bytes={len(payload)}{extra}")
            if event_type not in (
                "im.message.message_read_v1",
                "im.message.receive_v1",
                "im.message.reaction.created_v1",
                None,
            ):
                # 未知事件整包落盘，方便查 Agent/其它订阅
                print(f"📡 WS 其它事件 body={json.dumps(pl, ensure_ascii=False)[:800]}")
        except Exception as exc:  # noqa: BLE001
            print(f"📡 WS 原始事件解析失败: {exc} bytes={len(payload)}")
        return _orig_do(payload)

    event_handler._do_without_validation = _log_all_events  # type: ignore[method-assign]

    cli = lark.ws.Client(
        FEISHU_APP_ID,
        FEISHU_APP_SECRET,
        event_handler=event_handler,
        log_level=lark.LogLevel.INFO,
    )
    cli.start()


if __name__ == "__main__":
    main()
