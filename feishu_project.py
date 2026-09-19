"""飞书项目 Open API：读工作项，抽出 lidar pcap 数据名，调用 MeData 下载。"""

from __future__ import annotations

import json
import os
import re
import subprocess
import time
from datetime import datetime
from typing import Any

import requests

# 与 MeData data_download.py 的校验一致，避免把无关片段送去下载
DATA_NAME_RE = re.compile(
    r"[A-Za-z0-9][A-Za-z0-9._-]*\.car\d+[A-Za-z]*_log_part_lidar_pcap_cut_\d{14}_\d{14}"
)

_token_cache: dict[str, Any] = {"token": None, "expire_at": 0.0}
_space_cache: dict[str, str] = {}
_bb_save_path: str | None = None

# 负责人 / 经办人 / 当前操作人 / 角色负责人。不含报告人。
ASSIGN_FIELD_KEYS = ("owner", "issue_operator", "current_status_operator", "role_owners")
DOWNLOAD_CLAIM_PREFIX = "lux_dl"


def extract_data_names(*blobs: Any) -> list[str]:
    """从任意 JSON / 文本里按出现顺序抽出数据名，去重。"""
    parts: list[str] = []
    for blob in blobs:
        if blob is None:
            continue
        if isinstance(blob, str):
            parts.append(blob)
        else:
            parts.append(json.dumps(blob, ensure_ascii=False))
    seen: set[str] = set()
    names: list[str] = []
    for name in DATA_NAME_RE.findall("\n".join(parts)):
        if name not in seen:
            seen.add(name)
            names.append(name)
    return names


def _contains_user(value: Any, user_key: str) -> bool:
    if value is None or not user_key:
        return False
    if isinstance(value, str):
        return value == user_key or user_key in value
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return str(int(value)) == user_key
    if isinstance(value, list):
        return any(_contains_user(item, user_key) for item in value)
    if isinstance(value, dict):
        return any(_contains_user(item, user_key) for item in value.values())
    return False


def user_is_assignee(item: dict, user_key: str) -> bool:
    fields = item.get("fields") or []
    picked: dict[str, Any] = {}
    if isinstance(fields, list):
        for field in fields:
            if isinstance(field, dict) and field.get("field_key") in ASSIGN_FIELD_KEYS:
                picked[str(field.get("field_key"))] = field.get("field_value")
    return _contains_user(picked, user_key)


def comment_mentions_user(comment: dict, user_key: str) -> bool:
    """评论正文里出现 user_key 才算 @。评论作者本人不算。"""
    if not isinstance(comment, dict) or not user_key:
        return False
    payload = {
        "content": comment.get("content"),
        "doc_rich_text": comment.get("doc_rich_text"),
    }
    return _contains_user(payload, user_key)


def bb_save_path() -> str:
    global _bb_save_path
    if _bb_save_path:
        return _bb_save_path
    override = os.getenv("MEDATA_BB_SAVE_PATH", "").strip()
    if override:
        _bb_save_path = override
        return override
    script = os.getenv(
        "MEDATA_DOWNLOAD_SH",
        "/home/uisee/docs/tools/medata/data_download.sh",
    ).strip()
    cfg = os.path.join(os.path.dirname(script), "config", "default_settings.json")
    with open(cfg, encoding="utf-8") as handle:
        _bb_save_path = json.load(handle)["common_param"]["bb_save_path"]
    return _bb_save_path


def clip_local_paths(data_name: str) -> tuple[str, str]:
    """与 MeData parse_data_name 对齐：pcap 目录和 part_info。"""
    vehicle = data_name.split("_log_part_lidar_pcap_cut_")[0]
    s_time = data_name.split("_")[-2]
    e_time = data_name.split("_")[-1]
    day = datetime.strptime(s_time, "%Y%m%d%H%M%S").strftime("%Y/%m/%d")
    day_path = f"{bb_save_path()}/{vehicle}_bb_data/{vehicle}/{day}"
    part_time = f"{s_time[:-2]}-{s_time[-2:]}000_{e_time[:-2]}-{e_time[-2:]}000"
    pcap_path = f"{day_path}/pcap_cut/lidar_{part_time}"
    return pcap_path, f"{pcap_path}/part_info"


def clip_already_downloaded(data_name: str) -> bool:
    """part_info 已写出，或 pcap 目录里已经有文件，都视为下过。"""
    pcap_path, part_info = clip_local_paths(data_name)
    if os.path.isfile(part_info) and os.path.getsize(part_info) > 0:
        return True
    if not os.path.isdir(pcap_path):
        return False
    try:
        return any(True for _ in os.scandir(pcap_path))
    except OSError:
        return False


def claim_download_name(redis_client: Any, data_name: str) -> bool:
    ttl = int(os.getenv("MEDATA_DOWNLOAD_TIMEOUT", "3600")) + 600
    return bool(
        redis_client.set(f"{DOWNLOAD_CLAIM_PREFIX}:{data_name}", "1", nx=True, ex=ttl)
    )


def release_download_name(redis_client: Any, data_name: str) -> None:
    try:
        redis_client.delete(f"{DOWNLOAD_CLAIM_PREFIX}:{data_name}")
    except Exception as exc:  # noqa: BLE001
        print(f"⚠️ 释放下载占位失败 {data_name}: {exc}")


def _api_error(data: Any, action: str) -> None:
    if not isinstance(data, dict):
        raise RuntimeError(f"{action} 响应不是 JSON 对象")
    err = data.get("error")
    if isinstance(err, dict) and err.get("code") not in (None, 0, "0"):
        raise RuntimeError(f"{action} 失败: {err.get('msg') or err}")
    err_code = data.get("err_code")
    if err_code not in (None, 0, "0"):
        raise RuntimeError(f"{action} 失败: {data.get('err_msg') or err_code}")


class FeishuProjectClient:
    def __init__(self) -> None:
        self.base = os.getenv("FEISHU_PROJECT_BASE", "https://project.feishu.cn").rstrip("/")
        self.plugin_id = os.getenv("FEISHU_PROJECT_PLUGIN_ID", "").strip()
        self.plugin_secret = os.getenv("FEISHU_PROJECT_PLUGIN_SECRET", "").strip()
        self.user_key = os.getenv("FEISHU_PROJECT_USER_KEY", "").strip()
        missing = [
            name
            for name, value in (
                ("FEISHU_PROJECT_PLUGIN_ID", self.plugin_id),
                ("FEISHU_PROJECT_PLUGIN_SECRET", self.plugin_secret),
                ("FEISHU_PROJECT_USER_KEY", self.user_key),
            )
            if not value
        ]
        if missing:
            raise RuntimeError(
                "飞书项目凭证未配全，请在 .env 填写: " + ", ".join(missing)
            )

    def _token(self) -> str:
        now = time.time()
        if _token_cache["token"] and now < _token_cache["expire_at"] - 60:
            return _token_cache["token"]
        resp = requests.post(
            f"{self.base}/open_api/authen/plugin_token",
            json={
                "plugin_id": self.plugin_id,
                "plugin_secret": self.plugin_secret,
                "type": 0,
            },
            timeout=20,
        )
        resp.raise_for_status()
        data = resp.json()
        _api_error(data, "获取插件 token")
        token = ((data.get("data") or {}).get("token")) or ""
        if not token:
            raise RuntimeError(f"获取插件 token 失败: {data}")
        expire = int((data.get("data") or {}).get("expire_time") or 7200)
        _token_cache["token"] = token
        _token_cache["expire_at"] = now + expire
        return token

    def _headers(self) -> dict[str, str]:
        return {
            "Content-Type": "application/json",
            "X-PLUGIN-TOKEN": self._token(),
            "X-USER-KEY": self.user_key,
        }

    def _post(self, path: str, body: dict) -> dict:
        resp = requests.post(
            f"{self.base}{path}",
            headers=self._headers(),
            json=body,
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        _api_error(data, path)
        return data

    def _get(self, path: str, params: dict | None = None) -> dict:
        resp = requests.get(
            f"{self.base}{path}",
            headers=self._headers(),
            params=params,
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        _api_error(data, path)
        return data

    def resolve_project_key(self, name: str) -> str:
        cached = _space_cache.get(name)
        if cached:
            return cached
        last_error = ""
        for body in (
            {"user_key": self.user_key, "simple_names": [name]},
            {"user_key": self.user_key, "project_keys": [name]},
        ):
            try:
                data = self._post("/open_api/projects/detail", body)
            except Exception as exc:  # noqa: BLE001
                last_error = str(exc)
                continue
            project_key = _pick_project_key(data.get("data"), name)
            if project_key:
                _space_cache[name] = project_key
                return project_key
        if last_error:
            raise RuntimeError(
                f"无法解析空间 {name}。请确认插件已发布并安装到该空间。{last_error}"
            )
        raise RuntimeError(
            f"空间 {name} 没有返回 project_key。插件可能还没安装到这个空间。"
        )

    def get_work_item(self, project_key: str, type_key: str, work_item_id: str) -> dict:
        data = self._post(
            f"/open_api/{project_key}/work_item/{type_key}/query",
            {
                "work_item_ids": [int(work_item_id)],
                "expand": {
                    "need_multi_text": True,
                    "relation_fields_detail": True,
                },
            },
        )
        items = data.get("data") or []
        if isinstance(items, dict):
            items = items.get("work_item_list") or items.get("list") or []
        if not items:
            raise RuntimeError(
                f"没有读到工作项 {type_key}/{work_item_id}。"
                "常见原因：插件未安装到该空间、未发布版本，或 user_key 看不到这条工作项。"
            )
        item = items[0]
        if not isinstance(item, dict):
            raise RuntimeError(f"工作项响应格式异常: {type(item)}")
        return item

    def list_project_keys(self) -> list[str]:
        extra = os.getenv("FEISHU_PROJECT_SPACES", "").strip()
        if extra:
            return [part.strip() for part in extra.split(",") if part.strip()]
        data = self._post("/open_api/projects", {"user_key": self.user_key})
        raw = data.get("data") or []
        keys: list[str] = []
        if isinstance(raw, dict):
            keys = [str(key) for key in raw.keys() if key]
        elif isinstance(raw, list):
            for item in raw:
                if isinstance(item, str) and item:
                    keys.append(item)
                elif isinstance(item, dict):
                    key = str(item.get("project_key") or "")
                    if key:
                        keys.append(key)
        return keys

    def search_issues(self, project_key: str, param_key: str, value: Any, operator: str = "=") -> list[dict]:
        page_size = 50
        max_pages = int(os.getenv("LUX_SCAN_MAX_PAGES", "40"))
        found: list[dict] = []
        page = 1
        while page <= max_pages:
            data = self._post(
                f"/open_api/{project_key}/work_item/issue/search/params",
                {
                    "page_num": page,
                    "page_size": page_size,
                    "search_group": {
                        "conjunction": "AND",
                        "search_params": [
                            {"param_key": param_key, "value": value, "operator": operator}
                        ],
                        "search_groups": [],
                    },
                },
            )
            items = _as_item_list(data.get("data"))
            found.extend(items)
            total = int((data.get("pagination") or {}).get("total") or 0)
            if not items or page * page_size >= total:
                break
            page += 1
        return found

    def list_updated_issues(self, project_key: str, start_ms: int, end_ms: int) -> list[dict]:
        page_size = 50
        max_pages = int(os.getenv("LUX_SCAN_MAX_PAGES", "40"))
        found: list[dict] = []
        page = 1
        while page <= max_pages:
            data = self._post(
                f"/open_api/{project_key}/work_item/filter",
                {
                    "work_item_type_keys": ["issue"],
                    "updated_at": {"start": int(start_ms), "end": int(end_ms)},
                    "page_num": page,
                    "page_size": page_size,
                },
            )
            items = _as_item_list(data.get("data"))
            found.extend(items)
            total = int((data.get("pagination") or {}).get("total") or 0)
            if not items or len(items) < page_size or page * page_size >= total:
                break
            page += 1
        return found

    def list_issues(self, project_key: str) -> list[dict]:
        page_size = 50
        max_pages = int(os.getenv("LUX_SCAN_MAX_PAGES", "40"))
        found: list[dict] = []
        page = 1
        while page <= max_pages:
            data = self._post(
                f"/open_api/{project_key}/work_item/filter",
                {
                    "work_item_type_keys": ["issue"],
                    "page_num": page,
                    "page_size": page_size,
                },
            )
            items = _as_item_list(data.get("data"))
            found.extend(items)
            total = int((data.get("pagination") or {}).get("total") or 0)
            if not items or len(items) < page_size or (total and page * page_size >= total):
                break
            page += 1
        return found

    def list_comments(self, project_key: str, type_key: str, work_item_id: str) -> list:
        comments: list = []
        page = 1
        while page <= 10:
            try:
                data = self._get(
                    f"/open_api/{project_key}/work_item/{type_key}/{work_item_id}/comments",
                    params={"page_num": page, "page_size": 200},
                )
            except Exception as exc:  # noqa: BLE001
                print(f"⚠️ 评论拉取失败，仅使用工作项字段: {exc}")
                return comments
            batch = _as_item_list(data.get("data"))
            if not batch and isinstance(data.get("data"), dict):
                raw = data["data"]
                nested = raw.get("comments") or raw.get("list") or []
                batch = nested if isinstance(nested, list) else []
            comments.extend(item for item in batch if isinstance(item, dict))
            total = int((data.get("pagination") or {}).get("total") or 0)
            if len(batch) < 200 or (total and len(comments) >= total):
                break
            page += 1
        return comments


def _as_item_list(raw: Any) -> list[dict]:
    if isinstance(raw, list):
        return [item for item in raw if isinstance(item, dict)]
    if isinstance(raw, dict):
        for key in ("work_item_list", "list", "comments"):
            nested = raw.get(key)
            if isinstance(nested, list):
                return [item for item in nested if isinstance(item, dict)]
    return []


def _pick_project_key(data: Any, name: str) -> str | None:
    items: list[dict] = []
    if isinstance(data, dict):
        if "project_key" in data or "simple_name" in data:
            items = [data]
        else:
            for key, value in data.items():
                if isinstance(value, dict):
                    info = dict(value)
                    info.setdefault("project_key", key)
                    items.append(info)
                elif isinstance(value, str):
                    items.append({"project_key": key, "simple_name": value})
    elif isinstance(data, list):
        items = [item for item in data if isinstance(item, dict)]
    for info in items:
        project_key = str(info.get("project_key") or "")
        simple_name = str(info.get("simple_name") or "")
        if simple_name == name or project_key == name:
            return project_key or name
    if len(items) == 1:
        return str(items[0].get("project_key") or "") or None
    return None


def _download_env(script: str) -> dict[str, str]:
    """让 data_download.sh 使用 MeData 的 venv_web，而不是 lux 自己的 .venv。

    系统 python3 只有 PyYAML / PyQt5，没有 json5、opencv、onnxruntime。
    MeData 的 venv_web 已装齐 requirements_web.txt。
    """
    env = os.environ.copy()
    drop_bins = set()
    venv = env.pop("VIRTUAL_ENV", "")
    if venv:
        drop_bins.add(os.path.join(venv, "bin"))
    drop_bins.add(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".venv", "bin"))
    parts = [part for part in env.get("PATH", "").split(os.pathsep) if part and part not in drop_bins]
    medata_bin = os.getenv("MEDATA_PYTHON_BIN", "").strip()
    if not medata_bin:
        medata_bin = os.path.join(os.path.dirname(os.path.abspath(script)), "venv_web", "bin")
    python3 = os.path.join(medata_bin, "python3")
    if os.path.isfile(python3):
        parts.insert(0, medata_bin)
        env["VIRTUAL_ENV"] = os.path.dirname(medata_bin)
    else:
        print(f"⚠️ 未找到 MeData venv: {python3}，退回系统 python3")
    env["PATH"] = os.pathsep.join(parts)
    return env


def run_medata_download(data_name: str) -> tuple[bool, str]:
    script = os.getenv(
        "MEDATA_DOWNLOAD_SH",
        "/home/uisee/docs/tools/medata/data_download.sh",
    ).strip()
    mode = os.getenv("MEDATA_DELETE_MODE", "no_delete").strip() or "no_delete"
    if mode not in ("no_delete", "delete_partial", "delete_all"):
        mode = "no_delete"
    timeout = int(os.getenv("MEDATA_DOWNLOAD_TIMEOUT", "3600"))
    if not os.path.isfile(script):
        raise RuntimeError(f"找不到下载脚本: {script}")
    print(f"⬇ 调用下载 {data_name} mode={mode}")
    env = _download_env(script)
    py_bin = env.get("PATH", "").split(os.pathsep)[0]
    print(f"   python bin={py_bin}")
    try:
        proc = subprocess.run(
            ["bash", script, data_name, mode],
            cwd=os.path.dirname(script) or None,
            capture_output=True,
            text=True,
            timeout=timeout,
            env=env,
        )
    except subprocess.TimeoutExpired as exc:
        raise RuntimeError(f"下载超时（>{timeout}s）: {data_name}") from exc
    output = ((proc.stdout or "") + "\n" + (proc.stderr or "")).strip()
    tail = output[-800:] if output else "(无输出)"
    return proc.returncode == 0, tail
