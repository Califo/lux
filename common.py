"""共享工具：Redis、飞书发消息、Gerrit 链接解析。"""

from __future__ import annotations

import json
import os
import re
import time
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import redis
import requests
from dotenv import load_dotenv

# 必须用本文件旁的 .env，避免 cwd / 外部环境里的旧 FEISHU_APP_ID 干扰
_ENV_PATH = Path(__file__).resolve().parent / ".env"
load_dotenv(_ENV_PATH, override=True)

REDIS_QUEUE_KEY = os.getenv("REDIS_QUEUE_KEY", "lux_tasks")
REDIS_DONE_PREFIX = os.getenv("REDIS_DONE_PREFIX", "lux_done")
REDIS_MSG_DEDUP_PREFIX = os.getenv("REDIS_MSG_DEDUP_PREFIX", "lux_msg")
REDIS_PENDING_PREFIX = os.getenv("REDIS_PENDING_PREFIX", "lux_pending")
DIFF_MAX_CHARS = int(os.getenv("DIFF_MAX_CHARS", "80000"))
DONE_TTL_SECONDS = int(os.getenv("DONE_TTL_SECONDS", str(7 * 24 * 3600)))
MSG_DEDUP_TTL_SECONDS = int(os.getenv("MSG_DEDUP_TTL_SECONDS", str(24 * 3600)))
PENDING_TTL_SECONDS = int(os.getenv("PENDING_TTL_SECONDS", str(24 * 3600)))

# 预览消息上的确认表情
EMOJI_PUBLISH = os.getenv("FEISHU_EMOJI_PUBLISH", "CheckMark")  # ✅ 发布到 Gerrit
EMOJI_DISCARD = os.getenv("FEISHU_EMOJI_DISCARD", "CrossMark")  # ❌ 不发布
EMOJI_REREVIEW = os.getenv("FEISHU_EMOJI_REREVIEW", "OnIt")  # 🔄 重新 Review

# Gerrit / Cursor 用英文；飞书预览用中文说明必修与否
SEVERITY_LEGEND_EN = (
    "Severity (must-fix vs optional):\n"
    "[P0] MUST FIX before merge — crash / correctness critical\n"
    "[P1] MUST FIX before merge — high risk / likely functional bug\n"
    "[P2] SHOULD FIX — recommended improvement; not merge-blocking\n"
    "[P3] OPTIONAL — nit / style / comment; nice-to-have only"
)
SEVERITY_LEGEND_ZH = (
    "严重级别（是否必修）：\n"
    "[P0] 必修 — 崩溃/致命正确性问题，合入前必须修\n"
    "[P1] 必修 — 高风险/很可能有功能 bug，合入前必须修\n"
    "[P2] 建议修 — 推荐改，可不阻塞合入\n"
    "[P3] 非必修 — 风格/注释等小建议，可选"
)
# 兼容旧引用
SEVERITY_LEGEND = SEVERITY_LEGEND_ZH


GERRIT_HOST = os.getenv("GERRIT_HOST", "https://gerrit.uisee.ai").rstrip("/")
_GERRIT_HOST_RE = re.escape(urlparse(GERRIT_HOST).netloc or "gerrit.uisee.ai")

# 支持: #/c/123/ 、/c/123 、/c/project/+/123 、纯数字 change id
CHANGE_PATTERNS = [
    re.compile(rf"{_GERRIT_HOST_RE}/(?:#/)?c/(?:[^/\s]+/+/)?(\d+)", re.I),
    re.compile(r"(?:gerrit\s+)?(?:change|review)\s*[#:]?\s*(\d{4,})", re.I),
]

# 触发评审的意图词（含 gerrit 链接本身）
REVIEW_INTENT_RE = re.compile(
    r"("
    r"review|code\s*review|\bcr\b|gerrit|"
    r"检查代码|检查一下|检查下|帮忙看|帮我看|看看代码|看下代码|"
    r"评审|审一下|审下|查一下|查下|代码审查|帮我审|帮忙审"
    r")",
    re.I,
)

FEISHU_APP_ID = os.getenv("FEISHU_APP_ID", "")
FEISHU_APP_SECRET = os.getenv("FEISHU_APP_SECRET", "")

# 飞书项目工作项链接：https://project.feishu.cn/{空间}/{类型}/detail/{id}
_PROJECT_HOSTS = r"(?:project\.feishu\.cn|meego\.feishu\.cn|project\.larksuite\.com)"
PROJECT_URL_RE = re.compile(
    rf"(?:https?://)?{_PROJECT_HOSTS}/([A-Za-z0-9_-]+)/([A-Za-z0-9_-]+)/detail/(\d+)",
    re.I,
)
DOWNLOAD_INTENT_RE = re.compile(
    r"(下载|拉数据|拉取数据|下数据|download|拉一下|帮我下|帮忙下)",
    re.I,
)
SCAN_INTENT_RE = re.compile(
    r"(扫描|扫一下|扫一遍|扫一次|扫飞书|扫项目|扫缺陷|扫下|扫一扫|\bscan\b)",
    re.I,
)
REVIEW_GO_RE = re.compile(r"^(评|评审|review|开始评|评一下)$", re.I)
REVIEW_GO_CHANGE_RE = re.compile(r"^(评|评审|review)\s+#?(\d{4,8})$", re.I)
SCAN_REQUEST_KEY = "lux_scan_requests"
SCAN_LOCK_KEY = "lux_scan_lock"
RUNNING_TASK_KEY = "lux_running"
NOTIFY_CHAT_KEY = "lux_notify_chat"
NOTIFY_OPEN_ID_KEY = "lux_notify_open_id"
REVIEW_OFFER_HASH = "lux_review_offers"

# 机器人能力清单（后续加功能只改这里）
BOT_CAPABILITIES = [
    {
        "name": "Gerrit 代码评审",
        "desc": "对 Gerrit change 做 AI Code Review；先发预览，你点表情确认后再发布到 Gerrit",
        "examples": [
            "@我 review https://gerrit.uisee.ai/#/c/123456/",
            "被加为 reviewer 时我会先问你，回「评」再开始",
            "预览消息上点 ✅ 发布 / ❌ 取消 / OnIt 重新评审",
        ],
    },
    {
        "name": "飞书项目数据下载",
        "desc": "打开飞书项目工作项，抽出 log_part_lidar_pcap_cut 数据名，调用本机 MeData 下载。每天 06:00 和 22:00 自动扫，也可以随时让我扫",
        "examples": [
            "@我 下载 https://project.feishu.cn/空间/issue/detail/123456789",
            "@我 扫描",
        ],
    },
    {
        "name": "Gerrit 变动提醒",
        "desc": "你自己的 change CI 失败，或有人留了新评论，我会发一条短通知：change 号、失败阶段、日志里最后几行报错",
        "examples": [
            "不用 @ 我，轮询到就会发到你最近跟我说话的会话",
        ],
    },
    {
        "name": "查看任务队列",
        "desc": "问我当前在干什么、队列里还排着什么",
        "examples": [
            "@我 目前哪些任务在进行中",
            "@我 队列里有什么 / 任务状态",
        ],
    },
]


def format_capabilities_text() -> str:
    lines = ["我目前可以帮你做这些事：", ""]
    for i, cap in enumerate(BOT_CAPABILITIES, 1):
        lines.append(f"{i}. {cap['name']}")
        lines.append(f"   {cap['desc']}")
        for ex in cap.get("examples") or []:
            lines.append(f"   例：{ex}")
        lines.append("")
    lines.append("更多能力还在路上～有想加的功能可以直接跟我说。")
    return "\n".join(lines).rstrip()


_token_cache: dict[str, Any] = {"token": None, "expire_at": 0.0}


def get_redis() -> redis.Redis:
    password = os.getenv("REDIS_PASSWORD") or None
    # protocol=2：兼容不支持 HELLO/RESP3 的旧版 Redis
    # socket_timeout=None：BRPOP 阻塞等待时不能被客户端读超时打断
    return redis.Redis(
        host=os.getenv("REDIS_HOST", "127.0.0.1"),
        port=int(os.getenv("REDIS_PORT", "6379")),
        password=password,
        decode_responses=True,
        protocol=2,
        socket_connect_timeout=5,
        socket_timeout=None,
        health_check_interval=30,
    )


def has_download_intent(text: str) -> bool:
    return bool(DOWNLOAD_INTENT_RE.search(text or ""))


def normalize_command(text: str) -> str:
    text = re.sub(r"@_user_\d+", " ", text or "")
    text = re.sub(r"<at[^>]*>.*?</at>", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def has_scan_intent(text: str) -> bool:
    return bool(SCAN_INTENT_RE.search(normalize_command(text)))


STATUS_INTENT_RE = re.compile(
    r"("
    r"进行中|在执行|正在执行|在跑|正在跑|跑着|"
    r"在忙什么|在干什么|在做什么|忙什么|正在做|"
    r"任务状态|当前任务|排队|队列|"
    r"哪些任务|有哪些任务|有什么任务|还有什么任务|任务有哪些|任务有什么|"
    r"执行的任务|下载队列|评审队列|任务列表|进度怎么样|跑到哪了|"
    r"\bstatus\b|\bqueue\b|\btasks?\b"
    r")",
    re.I,
)


def has_status_intent(text: str) -> bool:
    return bool(STATUS_INTENT_RE.search(normalize_command(text)))


def summarize_task(task: dict | None, *, index: int | None = None) -> str:
    """把队列/运行中的任务收成一行给人看。"""
    if not task:
        return "(空)"
    action = str(task.get("action") or "review")
    prefix = f"{index}. " if index is not None else "· "
    if action == "download":
        wid = task.get("work_item_id") or "?"
        name = task.get("data_name") or task.get("simple_name") or ""
        extra = f" {name}" if name else ""
        return f"{prefix}下载 工作项 {wid}{extra}".rstrip()
    if action == "publish":
        return f"{prefix}发布评审到 Gerrit {task.get('change_id') or '?'}"
    if action == "rereview":
        return f"{prefix}重新评审 Gerrit {task.get('change_id') or '?'}"
    if action == "scan":
        return f"{prefix}扫描飞书项目"
    return f"{prefix}评审 Gerrit {task.get('change_id') or '?'}"


def set_running_task(client: redis.Redis, task: dict) -> None:
    payload = dict(task)
    payload["_started_at"] = time.time()
    client.set(RUNNING_TASK_KEY, json.dumps(payload, ensure_ascii=False))


def clear_running_task(client: redis.Redis) -> None:
    client.delete(RUNNING_TASK_KEY)


def get_running_task(client: redis.Redis) -> dict | None:
    raw = client.get(RUNNING_TASK_KEY)
    if not raw:
        return None
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return None
    return data if isinstance(data, dict) else None


def list_waiting_tasks(client: redis.Redis) -> list[dict]:
    """按即将执行的顺序返回等待队列（最前面的最先跑）。"""
    raws = client.lrange(REDIS_QUEUE_KEY, 0, -1) or []
    # LPUSH + BRPOP：列表头是最新入队，尾是下一条要跑的
    tasks: list[dict] = []
    for raw in reversed(raws):
        try:
            item = json.loads(raw)
        except json.JSONDecodeError:
            continue
        if isinstance(item, dict):
            tasks.append(item)
    return tasks


def list_waiting_scans(client: redis.Redis) -> list[dict]:
    raws = client.lrange(SCAN_REQUEST_KEY, 0, -1) or []
    items: list[dict] = []
    for raw in reversed(raws):
        try:
            item = json.loads(raw)
        except json.JSONDecodeError:
            continue
        if isinstance(item, dict):
            items.append(item)
    return items


def format_task_status_text(client: redis.Redis | None = None) -> str:
    """回答「目前哪些任务在进行中」。"""
    client = client or get_redis()
    lines: list[str] = []

    running = get_running_task(client)
    scanning = bool(client.get(SCAN_LOCK_KEY))
    lines.append("【进行中】")
    if running:
        started = running.get("_started_at")
        ago = ""
        try:
            if started:
                sec = max(0, int(time.time() - float(started)))
                if sec < 60:
                    ago = f"（已跑 {sec}s）"
                elif sec < 3600:
                    ago = f"（已跑 {sec // 60} 分钟）"
                else:
                    ago = f"（已跑 {sec // 3600} 小时 {(sec % 3600) // 60} 分）"
        except (TypeError, ValueError):
            ago = ""
        lines.append(f"{summarize_task(running)}{ago}")
    if scanning:
        lines.append("· 正在扫描飞书项目")
    if not running and not scanning:
        lines.append("· 没有正在执行的任务")

    waiting = list_waiting_tasks(client)
    lines.append("")
    lines.append(f"【等待中】任务队列 {len(waiting)} 条")
    if waiting:
        for i, task in enumerate(waiting[:20], 1):
            lines.append(summarize_task(task, index=i))
        if len(waiting) > 20:
            lines.append(f"… 还有 {len(waiting) - 20} 条")
    else:
        lines.append("· 空")

    scans = list_waiting_scans(client)
    lines.append("")
    lines.append(f"【等待中】扫描请求 {len(scans)} 条")
    if scans:
        for i, _ in enumerate(scans[:10], 1):
            lines.append(f"{i}. 手动扫描飞书项目")
        if len(scans) > 10:
            lines.append(f"… 还有 {len(scans) - 10} 条")
    else:
        lines.append("· 空")

    offers = list_review_offers(client)
    lines.append("")
    lines.append(f"【等待你确认】评审邀请 {len(offers)} 条")
    if offers:
        for i, offer in enumerate(offers[:10], 1):
            cid = offer.get("change_id") or "?"
            subj = (offer.get("subject") or "").strip()
            extra = f" {subj}" if subj else ""
            lines.append(f"{i}. Gerrit {cid}{extra}".rstrip())
            lines.append("   回「评」或点预览上的表情确认")
        if len(offers) > 10:
            lines.append(f"… 还有 {len(offers) - 10} 条")
    else:
        lines.append("· 空")

    return "\n".join(lines)


def review_go_change(text: str) -> tuple[bool, str | None]:
    """「评」或「评 123456」。不是确认时返回 (False, None)。"""
    cleaned = normalize_command(text)
    if REVIEW_GO_RE.fullmatch(cleaned):
        return True, None
    match = REVIEW_GO_CHANGE_RE.fullmatch(cleaned)
    if match:
        return True, match.group(2)
    return False, None


def remember_notify_target(chat_id: str | None, open_id: str | None) -> None:
    client = get_redis()
    if chat_id:
        client.set(NOTIFY_CHAT_KEY, chat_id)
    if open_id:
        client.set(NOTIFY_OPEN_ID_KEY, open_id)


def save_review_offer(client: redis.Redis, payload: dict) -> None:
    message_id = str(payload.get("message_id") or "")
    change_id = str(payload.get("change_id") or "")
    if not message_id or not change_id:
        return
    client.hset(REVIEW_OFFER_HASH, message_id, json.dumps(payload, ensure_ascii=False))
    client.hset(REVIEW_OFFER_HASH, f"change:{change_id}", message_id)
    client.expire(REVIEW_OFFER_HASH, 14 * 24 * 3600)


def load_review_offer(client: redis.Redis, message_id: str | None) -> dict | None:
    if not message_id:
        return None
    raw = client.hget(REVIEW_OFFER_HASH, message_id)
    if not raw:
        return None
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return None
    return data if isinstance(data, dict) else None


def list_review_offers(client: redis.Redis) -> list[dict]:
    data = client.hgetall(REVIEW_OFFER_HASH) or {}
    offers: list[dict] = []
    for key, raw in data.items():
        if str(key).startswith("change:"):
            continue
        try:
            item = json.loads(raw)
        except json.JSONDecodeError:
            continue
        if isinstance(item, dict):
            offers.append(item)
    return offers


def consume_review_offer(client: redis.Redis, offer: dict) -> None:
    message_id = str(offer.get("message_id") or "")
    change_id = str(offer.get("change_id") or "")
    if message_id:
        client.hdel(REVIEW_OFFER_HASH, message_id)
    if change_id:
        client.hdel(REVIEW_OFFER_HASH, f"change:{change_id}")


def extract_project_link(text: str) -> dict[str, str] | None:
    """从消息里取出飞书项目工作项链接。"""
    match = PROJECT_URL_RE.search(text or "")
    if not match:
        return None
    url = match.group(0)
    if not url.lower().startswith("http"):
        url = "https://" + url
    return {
        "url": url,
        "simple_name": match.group(1),
        "work_item_type_key": match.group(2),
        "work_item_id": match.group(3),
    }


def extract_change_id(text: str) -> str | None:
    if not text:
        return None
    for pattern in CHANGE_PATTERNS:
        match = pattern.search(text)
        if match:
            return match.group(1)
    # 有评审意图时，允许「检查代码 114626」这类裸 change 号
    if has_review_intent(text, allow_host_only=True):
        match = re.search(r"\b(\d{4,8})\b", text)
        if match:
            return match.group(1)
    return None


def _append_embedded_urls(text: str, raw: str) -> str:
    """文本字段里没有的链接（富文本 href）补回来。"""
    urls = re.findall(r"https?://[^\s\"'<>\\]+", raw or "")
    extra = []
    for url in urls:
        url = url.rstrip(".,);]")
        if url and url not in text:
            extra.append(url)
    if not extra:
        return text
    return (text + "\n" + "\n".join(extra)).strip()


def parse_message_text(content: str | dict | None) -> str:
    """飞书 message.content 通常是 JSON 字符串，如 {"text":"..."}。"""
    if content is None:
        return ""
    if isinstance(content, dict):
        data = content
    else:
        raw = str(content).strip()
        if not raw:
            return ""
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            return raw
    text = ""
    if isinstance(data, dict):
        if "text" in data:
            text = str(data.get("text") or "")
        else:
            # 富文本 post：拼出可读文本，并带上超链接
            post = data.get("post") or {}
            if isinstance(post, dict):
                parts: list[str] = []
                for lang_body in post.values():
                    if not isinstance(lang_body, dict):
                        continue
                    for line in lang_body.get("content") or []:
                        if not isinstance(line, list):
                            continue
                        for seg in line:
                            if not isinstance(seg, dict):
                                continue
                            if seg.get("text"):
                                parts.append(str(seg.get("text") or ""))
                            href = seg.get("href")
                            if href:
                                parts.append(str(href))
                if parts:
                    text = "".join(parts)
    if not text:
        text = raw if not isinstance(content, dict) else ""
    return _append_embedded_urls(text, raw if not isinstance(content, dict) else json.dumps(data, ensure_ascii=False))


def has_review_intent(text: str, *, allow_host_only: bool = True) -> bool:
    """是否表达了「请评审」意图。粘贴 Gerrit 链接也算。"""
    if not text:
        return False
    if REVIEW_INTENT_RE.search(text):
        return True
    if allow_host_only and re.search(_GERRIT_HOST_RE, text, re.I):
        return True
    return False


def claim_message(r: redis.Redis, message_id: str | None) -> bool:
    """按飞书 message_id 去重，防止事件重推重复入队。新消息返回 True。"""
    if not message_id:
        return True
    key = f"{REDIS_MSG_DEDUP_PREFIX}:{message_id}"
    return bool(r.set(key, "1", nx=True, ex=MSG_DEDUP_TTL_SECONDS))


def pending_key(preview_message_id: str) -> str:
    return f"{REDIS_PENDING_PREFIX}:{preview_message_id}"


def pending_root_key(root_message_id: str) -> str:
    """root 消息 → preview_id，方便用户点在原 @ 消息上也能确认。"""
    return f"{REDIS_PENDING_PREFIX}:root:{root_message_id}"


def save_pending_review(r: redis.Redis, preview_message_id: str, payload: dict) -> None:
    r.set(
        pending_key(preview_message_id),
        json.dumps(payload, ensure_ascii=False),
        ex=PENDING_TTL_SECONDS,
    )
    root_id = (payload.get("root_message_id") or "").strip()
    if root_id:
        r.set(pending_root_key(root_id), preview_message_id, ex=PENDING_TTL_SECONDS)


def load_pending_review(r: redis.Redis, preview_message_id: str) -> dict | None:
    raw = r.get(pending_key(preview_message_id))
    if not raw:
        return None
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return None
    return data if isinstance(data, dict) else None


def resolve_pending_review(
    r: redis.Redis, message_id: str | None
) -> tuple[str | None, dict | None]:
    """按预览消息 id 或原 @ 消息 id 解析 pending，返回 (preview_id, pending)。"""
    if not message_id:
        return None, None
    pending = load_pending_review(r, message_id)
    if pending:
        return message_id, pending
    preview_id = r.get(pending_root_key(message_id))
    if isinstance(preview_id, bytes):
        preview_id = preview_id.decode()
    if not preview_id:
        return None, None
    pending = load_pending_review(r, preview_id)
    if pending:
        return preview_id, pending
    return None, None


def take_pending_review(r: redis.Redis, preview_message_id: str) -> dict | None:
    """取出并删除 pending，避免重复确认。"""
    key = pending_key(preview_message_id)
    raw = r.get(key)
    if not raw:
        return None
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        r.delete(key)
        return None
    if not isinstance(data, dict):
        r.delete(key)
        return None
    r.delete(key)
    root_id = (data.get("root_message_id") or "").strip()
    if root_id:
        r.delete(pending_root_key(root_id))
    return data


def done_key(change_id: str, revision: str) -> str:
    return f"{REDIS_DONE_PREFIX}:{change_id}:{revision}"


def mark_reviewed(r: redis.Redis, change_id: str, revision: str) -> None:
    """记录最近评审过的 revision，仅作状态标记，不拦截再次评审。"""
    r.set(done_key(change_id, revision), "1", ex=DONE_TTL_SECONDS)


def get_feishu_token() -> str:
    now = time.time()
    if _token_cache["token"] and now < _token_cache["expire_at"] - 60:
        return _token_cache["token"]
    url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
    resp = requests.post(
        url,
        json={"app_id": FEISHU_APP_ID, "app_secret": FEISHU_APP_SECRET},
        timeout=15,
    )
    resp.raise_for_status()
    data = resp.json()
    if data.get("code") != 0:
        raise RuntimeError(f"获取飞书 token 失败: {data}")
    _token_cache["token"] = data["tenant_access_token"]
    _token_cache["expire_at"] = now + int(data.get("expire", 7200))
    return _token_cache["token"]


def send_feishu_text(chat_id: str, text: str) -> str | None:
    if not chat_id:
        return None
    if len(text) > 3500:
        text = text[:3400] + "\n…(已截断)"
    token = get_feishu_token()
    url = "https://open.feishu.cn/open-apis/im/v1/messages?receive_id_type=chat_id"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json; charset=utf-8",
    }
    payload = {
        "receive_id": chat_id,
        "msg_type": "text",
        "content": json.dumps({"text": text}, ensure_ascii=False),
    }
    resp = requests.post(url, headers=headers, json=payload, timeout=15)
    resp.raise_for_status()
    data = resp.json()
    if data.get("code") != 0:
        raise RuntimeError(f"发送飞书消息失败: {data}")
    return (data.get("data") or {}).get("message_id")


def notify_feishu(text: str) -> str | None:
    """发到 LUX_NOTIFY_CHAT_ID，否则发到最近一次跟机器人说话的会话。"""
    client = get_redis()
    chat_id = (os.getenv("LUX_NOTIFY_CHAT_ID") or "").strip() or (client.get(NOTIFY_CHAT_KEY) or "")
    open_id = (os.getenv("LUX_NOTIFY_OPEN_ID") or "").strip() or (client.get(NOTIFY_OPEN_ID_KEY) or "")
    if chat_id:
        return send_feishu_text(chat_id, text)
    if not open_id:
        return None
    if len(text) > 3500:
        text = text[:3400] + "\n…(已截断)"
    token = get_feishu_token()
    url = "https://open.feishu.cn/open-apis/im/v1/messages?receive_id_type=open_id"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json; charset=utf-8",
    }
    payload = {
        "receive_id": open_id,
        "msg_type": "text",
        "content": json.dumps({"text": text}, ensure_ascii=False),
    }
    resp = requests.post(url, headers=headers, json=payload, timeout=15)
    resp.raise_for_status()
    data = resp.json()
    if data.get("code") != 0:
        raise RuntimeError(f"发送飞书消息失败: {data}")
    return (data.get("data") or {}).get("message_id")


def reply_feishu_text(
    message_id: str | None,
    text: str,
    *,
    chat_id: str | None = None,
    reply_in_thread: bool = True,
) -> str | None:
    """优先以话题形式回复原消息；失败则退回普通群消息。返回新消息 message_id。"""
    if len(text) > 3500:
        text = text[:3400] + "\n…(已截断)"
    if message_id:
        token = get_feishu_token()
        url = f"https://open.feishu.cn/open-apis/im/v1/messages/{message_id}/reply"
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json; charset=utf-8",
        }
        payload = {
            "msg_type": "text",
            "content": json.dumps({"text": text}, ensure_ascii=False),
            "reply_in_thread": reply_in_thread,
        }
        resp = requests.post(url, headers=headers, json=payload, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        if data.get("code") == 0:
            return (data.get("data") or {}).get("message_id")
        print(f"⚠️ 话题回复失败 code={data.get('code')}: {data.get('msg')}")
        if reply_in_thread:
            payload["reply_in_thread"] = False
            resp2 = requests.post(url, headers=headers, json=payload, timeout=15)
            resp2.raise_for_status()
            data2 = resp2.json()
            if data2.get("code") == 0:
                return (data2.get("data") or {}).get("message_id")
            print(f"⚠️ 普通回复也失败: {data2}")
    if chat_id:
        return send_feishu_text(chat_id, text)
    raise RuntimeError("无法发送飞书回复：缺少 message_id 与 chat_id")


def add_feishu_reaction(message_id: str | None, emoji_type: str) -> None:
    """给原消息贴表情回应（如 OK / OnIt / DONE）。"""
    if not message_id or not emoji_type:
        return
    token = get_feishu_token()
    url = f"https://open.feishu.cn/open-apis/im/v1/messages/{message_id}/reactions"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json; charset=utf-8",
    }
    payload = {"reaction_type": {"emoji_type": emoji_type}}
    resp = requests.post(url, headers=headers, json=payload, timeout=15)
    resp.raise_for_status()
    data = resp.json()
    if data.get("code") != 0:
        raise RuntimeError(f"添加表情回应失败: {data}")


def seed_confirm_reactions(message_id: str | None) -> None:
    """预置取消/重评表情。

    注意：不要预置「发布」CheckMark。飞书上点机器人已贴的表情，有时只会触发
    reaction.deleted（揭掉）而不触发 created，导致确认无响应。发布需用户自己点 ✅。
    """
    for emoji in (EMOJI_DISCARD, EMOJI_REREVIEW):
        try:
            add_feishu_reaction(message_id, emoji)
        except Exception as exc:  # noqa: BLE001
            print(f"⚠️ 预置表情 {emoji} 失败: {exc}")


def mention_tag(open_id: str | None) -> str:
    if not open_id:
        return ""
    return f'<at user_id="{open_id}"></at> '


def wants_capability_help(text: str) -> bool:
    return bool(
        re.search(
            r"(你会什么|能做什么|有什么功能|帮助|帮忙|help|能力|功能列表|怎么用)",
            text or "",
            re.I,
        )
    )


def get_bot_open_id() -> str | None:
    """查询机器人自身 open_id，用于群聊 @ 过滤。"""
    override = os.getenv("FEISHU_BOT_OPEN_ID", "").strip()
    if override:
        return override
    try:
        token = get_feishu_token()
        resp = requests.get(
            "https://open.feishu.cn/open-apis/bot/v3/info",
            headers={"Authorization": f"Bearer {token}"},
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()
        if data.get("code") not in (None, 0):
            print(
                f"⚠️ bot/v3/info code={data.get('code')} msg={data.get('msg')}；"
                "请确认飞书应用已开通「机器人」能力，或在 .env 设置 FEISHU_BOT_OPEN_ID。"
            )
            return None
        bot = data.get("bot") or data.get("data") or {}
        return bot.get("open_id")
    except Exception as exc:  # noqa: BLE001
        print(f"⚠️ 获取 bot open_id 失败（将仅按 mentions 非空判断）: {exc}")
        return None
