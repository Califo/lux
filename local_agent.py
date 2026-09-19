"""本地 Agent：从 Redis 取任务，用 Cursor SDK 评审，结果写回 Gerrit 并通知飞书。"""

from __future__ import annotations

import base64
import gzip
import json
import os
import re
import tempfile
import time
from typing import Any

import redis
import requests
from cursor_sdk import (
    Agent,
    AgentOptions,
    CursorAgentError,
    LocalAgentOptions,
    ModelParameterValue,
    ModelSelection,
)
from pathlib import Path
from dotenv import load_dotenv
from requests.auth import HTTPDigestAuth

from common import (
    DIFF_MAX_CHARS,
    EMOJI_DISCARD,
    EMOJI_PUBLISH,
    EMOJI_REREVIEW,
    GERRIT_HOST,
    REDIS_QUEUE_KEY,
    SEVERITY_LEGEND_EN,
    SEVERITY_LEGEND_ZH,
    add_feishu_reaction,
    get_redis,
    mark_reviewed,
    mention_tag,
    reply_feishu_text,
    save_pending_review,
    seed_confirm_reactions,
    take_pending_review,
)
from feishu_project import (
    FeishuProjectClient,
    clip_already_downloaded,
    extract_data_names,
    release_download_name,
    run_medata_download,
)

_ENV_PATH = Path(__file__).resolve().parent / ".env"
load_dotenv(_ENV_PATH, override=True)

GERRIT_USER = os.getenv("GERRIT_USER", "")
GERRIT_PWD = os.getenv("GERRIT_HTTP_PWD", "")
CURSOR_API_KEY = os.getenv("CURSOR_API_KEY", "")
CURSOR_MODEL = os.getenv("CURSOR_MODEL", "grok-4.6")
CURSOR_MODEL_EFFORT = os.getenv("CURSOR_MODEL_EFFORT", "high").strip()
CURSOR_MODEL_THINKING = os.getenv("CURSOR_MODEL_THINKING", "true").strip().lower()
CURSOR_WORKSPACE = os.getenv("CURSOR_WORKSPACE", "").strip()

# 只读探索本地仓；用 allowlist，勿在 disallowed 里写未知工具名（SDK 会直接报错）
REVIEW_TOOLS = ["read", "grep", "glob", "ls", "semSearch"]

REVIEW_PROMPT_PREFIX = """You are a senior C++ code reviewer for an autonomous-driving LiDAR MOT module.

You are given a Gerrit Diff. Before commenting, use read-only tools (read / grep / glob / ls)
to inspect the local workspace: call sites, headers, related helpers, macros, and types.
Do NOT modify any files. Do NOT run shell commands.

Focus on REAL defects only:
1. Algorithm correctness (association / tracking)
2. C++ memory safety (null, lifetime, dangling pointers)
3. Threading / races / lock order
4. Crash risk / wrong behavior / broken edge cases
5. Clear performance bugs in hot paths (not micro-style nits)

COMMENT POLICY (strict — follow exactly):
- Do NOT comment on every changed line. Silence is correct when the change is fine.
- Inline comments ONLY for concrete bugs / high-risk issues you can justify.
- If the change looks correct, return "comments": [] and say so in summary (LGTM).
- Prefer fewer, higher-signal comments over many weak ones.
- Do NOT leave P3 / style / naming / comment-wording / preference nits as inline comments.
- Do NOT ask rhetorical questions or speculate ("maybe", "consider", "nit") unless it is a real bug.
- Parameter plumbing / simple getters-setters / straightforward renames usually need ZERO comments.

Severity tags (only for real issues you do comment on):
  [P0] MUST FIX — crash / correctness critical
  [P1] MUST FIX — high risk / likely functional bug
  [P2] SHOULD FIX — real issue, not merge-blocking
- In "summary", count P0/P1 vs P2; if comments is empty, summary should be a short LGTM.

IMPORTANT:
- Write ALL review text in English (Gerrit UI encoding).
- Line numbers MUST be NEW-file line numbers from the diff (right-hand / +++ side).
- Ground claims in local code when relevant (e.g. existing invariants, callers).

Respond with ONLY one JSON object (no markdown fences, no extra prose):
{
  "summary": "2-5 sentence overall review in English",
  "comments": [
    {
      "path": "uos_mot_lidar/src/foo.cc",
      "line": 116,
      "message": "[P1] Concrete issue and suggested fix."
    }
  ]
}

Rules for comments:
- path must match a file path appearing in the Diff (no a/ or b/ prefix).
- line must be an integer on the new file.
- Default to "comments": [] when unsure or when issues are only stylistic.
- Keep each message concise (1-3 sentences).

===== Diff Begin =====
"""


def build_model_selection() -> str | ModelSelection:
    """按模型族附加参数；本区 Claude/GPT 常不可用，默认走 grok-4.6。"""
    params: list[ModelParameterValue] = []
    mid = CURSOR_MODEL

    if mid.startswith("claude"):
        thinking = CURSOR_MODEL_THINKING
        if thinking in ("1", "true", "yes", "on"):
            params.append(ModelParameterValue(id="thinking", value="true"))
        elif thinking in ("0", "false", "no", "off"):
            params.append(ModelParameterValue(id="thinking", value="false"))

    effort = CURSOR_MODEL_EFFORT
    if effort:
        if mid.startswith(("gpt", "kimi", "glm")):
            param_id = "reasoning"
        elif mid in ("auto-smart", "default"):
            param_id = ""
        else:
            param_id = "effort"
        if param_id:
            params.append(ModelParameterValue(id=param_id, value=effort))

    if mid == "auto-smart" and not any(p.id == "optimize_for" for p in params):
        params.append(ModelParameterValue(id="optimize_for", value="intelligence"))

    if not params:
        return CURSOR_MODEL
    return ModelSelection(id=CURSOR_MODEL, params=tuple(params))

r = get_redis()
GERRIT_AUTH = HTTPDigestAuth(GERRIT_USER, GERRIT_PWD)


def gerrit_request(method: str, path: str, **kwargs) -> requests.Response:
    url = f"{GERRIT_HOST}{path}"
    resp = requests.request(
        method,
        url,
        auth=GERRIT_AUTH,
        timeout=60,
        **kwargs,
    )
    if resp.status_code >= 400:
        raise RuntimeError(f"Gerrit {method} {path} -> {resp.status_code}: {resp.text[:500]}")
    return resp


def gerrit_json(path: str):
    raw = gerrit_request("GET", path).text
    if raw.startswith(")]}'"):
        raw = raw[4:]
    return json.loads(raw)


def decode_gerrit_patch(raw: str) -> str:
    """Gerrit /patch 返回 base64；部分版本再套 gzip。"""
    text = raw.strip()
    if text.startswith(")]}'"):
        text = text[4:].lstrip()
    if text.startswith("diff ") or text.startswith("From ") or text.startswith("--- "):
        return text
    data = base64.b64decode(text)
    try:
        return gzip.decompress(data).decode("utf-8", errors="replace")
    except OSError:
        return data.decode("utf-8", errors="replace")


def fetch_change_meta(change_id: str) -> dict:
    detail = gerrit_json(f"/a/changes/{change_id}/detail?o=CURRENT_REVISION")
    revision = detail.get("current_revision") or "current"
    subject = (detail.get("subject") or "").strip()
    return {"revision": revision, "subject": subject, "raw": detail}


def fetch_change_files(change_id: str) -> list[str]:
    files = gerrit_json(f"/a/changes/{change_id}/revisions/current/files")
    return [p for p in files.keys() if p != "/COMMIT_MSG"]


def fetch_gerrit_diff(change_id: str) -> str:
    raw = gerrit_request("GET", f"/a/changes/{change_id}/revisions/current/patch").text
    diff = decode_gerrit_patch(raw)
    if len(diff) > DIFF_MAX_CHARS:
        half = DIFF_MAX_CHARS // 2
        diff = (
            diff[:half]
            + f"\n\n...(diff truncated, original length {len(diff)} chars)...\n\n"
            + diff[-half:]
        )
    return diff


def _extract_json_object(text: str) -> dict[str, Any]:
    text = text.strip()
    # Strip ```json ... ``` if model ignored instructions
    fence = re.search(r"```(?:json)?\s*([\s\S]*?)```", text)
    if fence:
        text = fence.group(1).strip()
    try:
        data = json.loads(text)
        if isinstance(data, dict):
            return data
    except json.JSONDecodeError:
        pass
    start = text.find("{")
    end = text.rfind("}")
    if start >= 0 and end > start:
        data = json.loads(text[start : end + 1])
        if isinstance(data, dict):
            return data
    raise ValueError("Cursor output is not valid JSON review object")


def resolve_file_path(path: str, known_files: list[str]) -> str | None:
    if not path:
        return None
    path = path.strip().lstrip("./")
    if path.startswith("a/") or path.startswith("b/"):
        path = path[2:]
    if path in known_files:
        return path
    # basename / suffix match
    matches = [f for f in known_files if f == path or f.endswith("/" + path) or f.endswith(path)]
    if len(matches) == 1:
        return matches[0]
    if len(matches) > 1:
        # prefer shortest (most specific suffix usually longer path - take exact endswith)
        exact = [f for f in matches if f.endswith("/" + path) or f.endswith(path)]
        return sorted(exact or matches, key=len)[0]
    return None


def parse_review_result(raw: str, known_files: list[str]) -> dict[str, Any]:
    data = _extract_json_object(raw)
    summary = str(data.get("summary") or "").strip()
    comments_in = data.get("comments") or []
    if not isinstance(comments_in, list):
        comments_in = []

    gerrit_comments: dict[str, list[dict[str, Any]]] = {}
    display_lines: list[str] = []
    skipped = 0

    for item in comments_in:
        if not isinstance(item, dict):
            skipped += 1
            continue
        path = resolve_file_path(str(item.get("path") or ""), known_files)
        try:
            line = int(item.get("line"))
        except (TypeError, ValueError):
            skipped += 1
            continue
        message = str(item.get("message") or "").strip()
        if not path or line <= 0 or not message:
            skipped += 1
            continue
        if len(message) > 4000:
            message = message[:3900] + "\n...(truncated)"
        gerrit_comments.setdefault(path, []).append(
            {
                "line": line,
                "message": message,
                "unresolved": True,
            }
        )
        display_lines.append(f"{path}\nLine {line}:\n{message}")

    if not summary:
        summary = "AI review completed. See inline comments."
    if skipped:
        summary += f"\n\n(Note: skipped {skipped} malformed inline comment(s).)"

    return {
        "summary": summary,
        "comments": gerrit_comments,
        "display": "\n\n".join(display_lines),
        "inline_count": sum(len(v) for v in gerrit_comments.values()),
    }


def submit_gerrit_review(
    change_id: str,
    summary: str,
    comments: dict[str, list[dict[str, Any]]] | None = None,
) -> None:
    if len(summary) > 14000:
        summary = summary[:13500] + "\n...(truncated)"
    payload: dict[str, Any] = {
        "message": f"{summary}\n\n{SEVERITY_LEGEND_EN}",
        "labels": {"Code-Review": 0},
        "tag": "autogenerated:cursor-ai-review",
        "omit_duplicate_comments": True,
    }
    if comments:
        payload["comments"] = comments
    gerrit_request(
        "POST",
        f"/a/changes/{change_id}/revisions/current/review",
        json=payload,
        headers={"Content-Type": "application/json; charset=UTF-8"},
    )


def cursor_review(diff_text: str, change_id: str, subject: str, known_files: list[str]) -> dict[str, Any]:
    if not CURSOR_API_KEY:
        raise RuntimeError("未配置 CURSOR_API_KEY，请到 Cursor Dashboard → Integrations 创建")

    use_workspace = bool(CURSOR_WORKSPACE and os.path.isdir(CURSOR_WORKSPACE))
    file_hint = "\n".join(f"- {p}" for p in known_files[:80])
    workspace_note = (
        f"Local workspace cwd: {CURSOR_WORKSPACE}\n"
        "Use read/grep/glob/ls against this tree for context.\n"
        if use_workspace
        else "No local workspace configured — review from Diff text only.\n"
    )
    prompt = (
        REVIEW_PROMPT_PREFIX
        + f"Gerrit Change: {change_id}\nSubject: {subject}\n"
        + workspace_note
        + "Files in this revision:\n"
        + file_hint
        + "\n\n"
        + diff_text
        + "\n===== Diff End =====\n"
    )

    def _run(cwd: str, *, with_tools: bool) -> str:
        options = AgentOptions(
            api_key=CURSOR_API_KEY,
            model=build_model_selection(),
            local=LocalAgentOptions(cwd=cwd),
            # tools=[] = 无工具；指定名单 = 仅这些只读工具
            tools=REVIEW_TOOLS if with_tools else [],
        )
        err_detail = ""
        # 用 create+stream，才能拿到地区限制等 ERROR message
        with Agent.create(options) as agent:
            run = agent.send(prompt)
            for event in run.stream():
                if getattr(event, "type", None) != "status":
                    continue
                if str(getattr(event, "status", "")).upper() != "ERROR":
                    continue
                err_detail = (getattr(event, "message", None) or "").strip() or err_detail
            result = run.wait()

        status = str(result.status or "").lower()
        if status in ("error", "failed"):
            detail = err_detail or f"run_id={getattr(result, 'id', None)}"
            raise RuntimeError(f"Cursor run 失败: {detail}")
        text = (result.result or "").strip()
        if not text:
            raise RuntimeError(
                f"Cursor 返回空评审结果 (status={result.status}, run_id={getattr(result, 'id', None)})"
            )
        return text

    try:
        if use_workspace:
            print(f"🔎 结合本地仓评审: {CURSOR_WORKSPACE} model={CURSOR_MODEL}")
            raw = _run(CURSOR_WORKSPACE, with_tools=True)
        else:
            print(f"⚠️ CURSOR_WORKSPACE 无效，纯 Diff 评审 model={CURSOR_MODEL}")
            with tempfile.TemporaryDirectory(prefix="gerrit-review-") as tmp:
                raw = _run(tmp, with_tools=False)
    except CursorAgentError as err:
        raise RuntimeError(
            f"Cursor 启动失败: {err.message} (retryable={err.is_retryable})"
        ) from err

    return parse_review_result(raw, known_files)


def process_publish(task: dict) -> None:
    preview_id = task.get("preview_message_id") or ""
    # 优先用入队时带上的 pending，避免与 feishu_bot 抢删 Redis key
    pending = task.get("pending")
    if not isinstance(pending, dict):
        pending = take_pending_review(r, preview_id) if preview_id else None
    elif preview_id:
        # 清理残留，防止重复点 ✅ 再次入队
        take_pending_review(r, preview_id)

    chat_id = task.get("chat_id") or ""
    root_message_id = task.get("message_id")

    if not pending:
        # 重复/过期确认：只打日志，不再打扰用户
        print(f"⏭ publish 跳过：pending 不存在 preview_id={preview_id}")
        return

    change_id = pending["change_id"]
    subject = pending.get("subject") or ""
    revision = pending.get("revision") or "current"
    review = pending["review"]
    root_message_id = pending.get("root_message_id") or root_message_id
    chat_id = pending.get("chat_id") or chat_id

    submit_gerrit_review(change_id, review["summary"], review["comments"])
    mark_reviewed(r, change_id, revision)

    try:
        add_feishu_reaction(root_message_id, os.getenv("FEISHU_DONE_EMOJI", "DONE"))
    except Exception as exc:  # noqa: BLE001
        print(f"⚠️ 完成表情失败: {exc}")

    reply_feishu_text(
        root_message_id,
        (
            f"✅ 已发布到 Gerrit\n"
            f"Change: {change_id}\n"
            f"Subject: {subject}\n"
            f"链接: {GERRIT_HOST}/#/c/{change_id}/\n"
            f"行内评论数: {review.get('inline_count', 0)}"
        ),
        chat_id=chat_id,
        reply_in_thread=True,
    )
    print(f"✅ 已发布 Gerrit {change_id}")


def process_review(task: dict) -> None:
    change_id = task["change_id"]
    chat_id = task.get("chat_id") or ""
    message_id = task.get("message_id")
    requester = task.get("sender_open_id")

    print(f"\n📥 收到评审任务 Gerrit change {change_id}")
    meta = fetch_change_meta(change_id)
    revision = meta["revision"]
    subject = meta["subject"]
    known_files = fetch_change_files(change_id)

    diff = fetch_gerrit_diff(change_id)
    review = cursor_review(diff, change_id, subject, known_files)

    body = review["display"] or "(no inline comments)"
    at = mention_tag(requester)
    notice = (
        f"{at}AI 评审已完成 —— 尚未发布到 Gerrit。\n"
        f"Change: {change_id}\n"
        f"Subject: {subject}\n"
        f"链接: {GERRIT_HOST}/#/c/{change_id}/\n"
        f"行内评论数: {review['inline_count']}\n"
        f"{SEVERITY_LEGEND_ZH}\n\n"
        f"请在本条预览消息上自己点表情确认（不要点机器人已贴的表情）：\n"
        f"· 自己点 {EMOJI_PUBLISH} ✅ → 发布到 Gerrit\n"
        f"· {EMOJI_DISCARD} ❌ → 不发布\n"
        f"· {EMOJI_REREVIEW} 🔄 → 重新 Review\n\n"
        f"Summary:\n{review['summary'][:800]}\n\n"
        f"{body[:1600]}"
    )

    preview_id = reply_feishu_text(
        message_id, notice, chat_id=chat_id, reply_in_thread=True
    )
    if not preview_id:
        # 无法拿到预览 message_id 时无法用表情确认，降级直接发布
        print("⚠️ 未拿到预览 message_id，降级直接发布到 Gerrit")
        submit_gerrit_review(change_id, review["summary"], review["comments"])
        mark_reviewed(r, change_id, revision)
        return

    save_pending_review(
        r,
        preview_id,
        {
            "change_id": change_id,
            "subject": subject,
            "revision": revision,
            "chat_id": chat_id,
            "root_message_id": message_id,
            "requester_open_id": requester,
            "preview_message_id": preview_id,
            "review": {
                "summary": review["summary"],
                "comments": review["comments"],
                "inline_count": review["inline_count"],
                "display": review["display"],
            },
        },
    )
    seed_confirm_reactions(preview_id)
    print(f"✅ 预览已发送，等待确认 preview_id={preview_id} inline={review['inline_count']}")


def _notify_download(task: dict, text: str) -> None:
    message_id = task.get("message_id") or ""
    chat_id = task.get("chat_id") or ""
    if not message_id and not chat_id:
        print(text)
        return
    reply_feishu_text(message_id, text, chat_id=chat_id, reply_in_thread=True)


def process_download(task: dict) -> None:
    message_id = task.get("message_id")
    requester = task.get("sender_open_id")
    url = task.get("project_url") or ""
    simple_name = task.get("simple_name") or ""
    type_key = task.get("work_item_type_key") or ""
    work_item_id = task.get("work_item_id") or ""

    print(f"\n📥 收到下载任务 {simple_name}/{type_key}/{work_item_id}")
    client = FeishuProjectClient()
    project_key = client.resolve_project_key(simple_name)
    item = client.get_work_item(project_key, type_key, work_item_id)
    comments = client.list_comments(project_key, type_key, work_item_id)
    names = extract_data_names(item, comments, task.get("raw_text") or "")
    wanted = task.get("data_names")
    if isinstance(wanted, list) and wanted:
        allow = {str(name) for name in wanted}
        names = [name for name in names if name in allow]
    title = str(item.get("name") or "").strip() or work_item_id
    at = mention_tag(requester)

    if not names:
        raise RuntimeError(
            f"工作项「{title}」里没有找到 log_part_lidar_pcap_cut 数据名。\n链接: {url}"
        )

    pending = [name for name in names if not clip_already_downloaded(name)]
    skipped = [name for name in names if name not in pending]
    for name in skipped:
        print(f"⏭ 已下载，跳过 {name}")
        if task.get("claimed"):
            release_download_name(r, name)

    if not pending:
        _notify_download(
            task,
            f"{at}工作项「{title}」里的 {len(names)} 条数据都已在本地，跳过。\n链接: {url}",
        )
        return

    _notify_download(
        task,
        (
            f"{at}已打开工作项「{title}」，找到 {len(names)} 条数据"
            f"（已下载跳过 {len(skipped)}），开始下载 {len(pending)} 条：\n"
            + "\n".join(f"· {name}" for name in pending)
        ),
    )

    lines: list[str] = [f"⏭ {name} 已在本地" for name in skipped]
    failed = 0
    for name in pending:
        try:
            ok, tail = run_medata_download(name)
        finally:
            if task.get("claimed"):
                release_download_name(r, name)
        if ok:
            lines.append(f"✅ {name}")
            print(f"✅ 下载完成 {name}")
        else:
            failed += 1
            lines.append(f"❌ {name}\n{tail}")
            print(f"❌ 下载失败 {name}\n{tail}")

    try:
        add_feishu_reaction(
            message_id,
            os.getenv("FEISHU_ERROR_EMOJI", "ERROR")
            if failed
            else os.getenv("FEISHU_DONE_EMOJI", "DONE"),
        )
    except Exception as exc:  # noqa: BLE001
        print(f"⚠️ 完成表情失败: {exc}")

    _notify_download(
        task,
        (
            f"{at}下载结束：成功 {len(pending) - failed}，失败 {failed}，跳过 {len(skipped)}\n"
            f"工作项: {title}\n"
            f"链接: {url}\n\n"
            + "\n".join(lines)
        ),
    )


def process_task(task: dict) -> None:
    action = task.get("action") or "review"
    if action == "publish":
        process_publish(task)
    elif action == "download":
        process_download(task)
    elif action == "rereview":
        # 重新评审：沿用原 change / 会话
        process_review(
            {
                "change_id": task["change_id"],
                "chat_id": task.get("chat_id"),
                "message_id": task.get("message_id"),
                "sender_open_id": task.get("sender_open_id"),
            }
        )
    else:
        process_review(task)


def main_loop() -> None:
    if not CURSOR_API_KEY:
        print("⚠️ 警告: CURSOR_API_KEY 为空，收到任务后会失败")
    print("🟢 Local Agent 启动，等待任务…")
    script = os.getenv(
        "MEDATA_DOWNLOAD_SH",
        "/home/uisee/docs/tools/medata/data_download.sh",
    )
    ws = CURSOR_WORKSPACE if CURSOR_WORKSPACE and os.path.isdir(CURSOR_WORKSPACE) else "(none)"
    print(
        f"   queue={REDIS_QUEUE_KEY} model={CURSOR_MODEL} "
        f"effort={CURSOR_MODEL_EFFORT} thinking={CURSOR_MODEL_THINKING} "
        f"workspace={ws} gerrit={GERRIT_HOST}"
    )
    print(f"   download={script}")
    while True:
        try:
            task_data = r.brpop(REDIS_QUEUE_KEY, timeout=5)
        except (redis.exceptions.TimeoutError, TimeoutError):
            continue
        except redis.exceptions.ConnectionError as exc:
            print(f"⚠️ Redis 连接异常，5s 后重试: {exc}")
            time.sleep(5)
            continue
        if not task_data:
            continue
        _, task_str = task_data
        try:
            task = json.loads(task_str)
        except json.JSONDecodeError as exc:
            print(f"❌ 非法任务 JSON: {exc} raw={task_str[:200]}")
            continue

        chat_id = task.get("chat_id") or ""
        change_id = task.get("change_id", "?")
        action = task.get("action") or "review"
        try:
            process_task(task)
        except Exception as exc:  # noqa: BLE001
            if action == "download":
                err_msg = f"❌ 数据下载失败：{exc}"
            else:
                err_msg = f"❌ Gerrit {change_id} AI 评审失败：{exc}"
            print(err_msg)
            try:
                add_feishu_reaction(
                    task.get("message_id"),
                    os.getenv("FEISHU_ERROR_EMOJI", "ERROR"),
                )
            except Exception:  # noqa: BLE001
                pass
            if chat_id or task.get("message_id"):
                try:
                    reply_feishu_text(
                        task.get("message_id"),
                        err_msg,
                        chat_id=chat_id,
                        reply_in_thread=True,
                    )
                except Exception as send_exc:  # noqa: BLE001
                    print(f"飞书错误通知也失败: {send_exc}")


if __name__ == "__main__":
    main_loop()
