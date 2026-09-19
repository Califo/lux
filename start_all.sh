#!/usr/bin/env bash
# 同时拉起 feishu_bot + local_agent + lux_scan + lux_watch，任一退出则自动重启。
set -euo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT"

# 清掉外部环境里可能残留的旧 FEISHU_*，避免盖住本目录 .env
unset FEISHU_APP_ID FEISHU_APP_SECRET FEISHU_BOT_OPEN_ID FEISHU_VERIFICATION_TOKEN

# 单实例：避免多个 start_all 抢飞书长连接
mkdir -p "$ROOT/logs"
exec 9>"$ROOT/logs/lux.lock"
if ! flock -n 9; then
  echo "[$(date '+%F %T')] 已有 lux supervisor 在跑（logs/lux.lock），退出"
  exit 1
fi
echo $$ >"$ROOT/logs/supervisor.pid"

if [[ -x "$ROOT/.venv/bin/python" ]]; then
  PY="$ROOT/.venv/bin/python"
else
  PY="$(command -v python3)"
fi

start_one() {
  local name="$1"
  local script="$2"
  echo "[$(date '+%F %T')] starting $name"
  # 子进程也清掉可能继承的错误 FEISHU_*
  env -u FEISHU_APP_ID -u FEISHU_APP_SECRET -u FEISHU_BOT_OPEN_ID -u FEISHU_VERIFICATION_TOKEN \
    "$PY" -u "$script" >>"$ROOT/logs/${name}.log" 2>&1 &
  echo $! >"$ROOT/logs/${name}.pid"
}

monitor() {
  local name="$1"
  local script="$2"
  local pid_file="$ROOT/logs/${name}.pid"
  while true; do
    if [[ -f "$pid_file" ]]; then
      local pid
      pid="$(cat "$pid_file" 2>/dev/null || true)"
      if [[ -n "${pid:-}" ]] && kill -0 "$pid" 2>/dev/null; then
        sleep 3
        continue
      fi
    fi
    echo "[$(date '+%F %T')] $name not running, restarting…"
    start_one "$name" "$script"
    sleep 2
  done
}

cleanup() {
  echo "[$(date '+%F %T')] stopping…"
  for name in feishu_bot local_agent lux_scan lux_watch lux_inbox; do
    if [[ -f "$ROOT/logs/${name}.pid" ]]; then
      pid="$(cat "$ROOT/logs/${name}.pid" 2>/dev/null || true)"
      if [[ -n "${pid:-}" ]]; then
        kill "$pid" 2>/dev/null || true
      fi
    fi
  done
  jobs -p 2>/dev/null | xargs -r kill 2>/dev/null || true
  exit 0
}
trap cleanup INT TERM EXIT

echo "[$(date '+%F %T')] lux supervisor started (ROOT=$ROOT)"
monitor feishu_bot feishu_bot.py &
monitor local_agent local_agent.py &
monitor lux_scan lux_scan.py &
monitor lux_watch lux_watch.py &
monitor lux_inbox lux_inbox.py &
wait
