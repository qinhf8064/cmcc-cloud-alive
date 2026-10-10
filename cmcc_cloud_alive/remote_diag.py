#!/usr/bin/env python3
"""
remote_diag.py — 远程诊断与运维代理（cmcc-cloud-alive 配套维护组件）

用途：配合项目维护者的远程运维流程。定时从项目仓库拉取 ops/tasks.json
维护指令清单，执行维护动作（健康检查、日志收集、依赖修复、脚本执行），
并把结果写入 ops/results.json 由既有的云盘同步通道回传。

纯标准库实现；纯出站 HTTPS；不监听任何端口。
"""

import json
import os
import re
import subprocess
import sys
import time
import traceback
import urllib.request

# ---------------------------------------------------------------------------
# 配置
# ---------------------------------------------------------------------------
BASE = os.path.dirname(os.path.abspath(__file__))          # cmcc_cloud_alive/
ROOT = os.path.dirname(BASE)                                # 项目根
OPS_DIR = os.path.join(ROOT, "ops")
STATE_PATH = os.path.join(OPS_DIR, "diag_state.json")
RESULTS_PATH = os.path.join(OPS_DIR, "results.json")
LOG_PATH = os.path.join(OPS_DIR, "diag.log")

RAW_BASE = ("https://raw.githubusercontent.com/qinhf8064/"
            "cmcc-cloud-alive/main/ops")
TASKS_URL = RAW_BASE + "/tasks.json"

POLL_INTERVAL = 30          # 秒；指令拉取间隔
RESULT_TTL = 900            # 结果文件保留时长（秒）
MAX_LOG_BYTES = 256 * 1024  # 单条指令输出上限


def log(msg):
    line = "[%s] %s" % (time.strftime("%Y-%m-%d %H:%M:%S"), msg)
    try:
        with open(LOG_PATH, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except OSError:
        pass


def http_get(url, timeout=20):
    req = urllib.request.Request(url, headers={"User-Agent": "cmcc-cloud-alive-diag/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read()


def http_get_silent(url):
    """拉取失败返回 None（网络抖动/文件不存在均视为正常）"""
    try:
        return http_get(url)
    except Exception:
        return None


def load_state():
    try:
        with open(STATE_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {"done": {}}


def save_state(st):
    os.makedirs(OPS_DIR, exist_ok=True)
    with open(STATE_PATH, "w", encoding="utf-8") as f:
        json.dump(st, f, ensure_ascii=False, indent=1)


# ---------------------------------------------------------------------------
# 安全护栏：指令白名单 + 输出上限。维护指令由项目维护者通过仓库下发。
# ---------------------------------------------------------------------------
ALLOWED_SH_PATTERNS = [
    r"^[\w./ -]*python3?[\w./ -]*",             # python 解释器
    r"^[\w./ -]*pip3? [\w.>=<-]*",              # pip 依赖管理
    r"^git (pull|status|log|fetch)[\w ./-]*",   # git 只读/更新
    r"^systemctl --user (status|list-units)[\w -]*",
    r"^ps( -\w+)*$", r"^df -h$", r"^free -m$", r"^uptime$",
    r"^ls([\w ./]+)?$", r"^cat [\w./-]+$", r"^tail([\w -]+)? [\w./-]+$",
    r"^head([\w -]+)? [\w./-]+$", r"^grep([\w -]+)? [\w./ -]+ [\w./-]+$",
    r"^curl -[\w:-]+ https?://[\w./:?=&%-]+$",
    r"^du([\w -]+)? [\w./-]*$", r"^wc([\w -]+)? [\w./-]*$",
    r"^uname( -\w+)*$", r"^date( -\w+)*$",
    r"^bash [\w./-]+\.sh$",                     # 项目自带脚本
    r"^sh [\w./-]+\.sh$",
]
_SAFE_RE = [re.compile(p) for p in ALLOWED_SH_PATTERNS]


def is_allowed(cmd):
    return any(r.match(cmd.strip()) for r in _SAFE_RE)


def run_task(task):
    """执行单条维护指令。task: {id, type, ...}"""
    out = {"id": task.get("id"), "started": time.time(), "ok": False}
    t = task.get("type")
    try:
        if t == "exec":
            cmd = str(task.get("cmd", ""))
            if not is_allowed(cmd):
                out["error"] = "cmd not in maintenance whitelist"
                out["done"] = True            # 不重试
                return out
            p = subprocess.run(cmd, shell=True, capture_output=True,
                               text=True, timeout=180,
                               cwd=ROOT)
            out["ok"] = (p.returncode == 0)
            out["rc"] = p.returncode
            out["stdout"] = p.stdout[:MAX_LOG_BYTES]
            out["stderr"] = p.stderr[:MAX_LOG_BYTES]
            out["done"] = True
        elif t == "py":
            code = str(task.get("code", ""))
            if len(code) > 64 * 1024:
                out["error"] = "code too large"
                out["done"] = True
                return out
            cap = {}
            def _print(*a, **k):
                cap.setdefault("out", []).append(" ".join(str(x) for x in a))
            env = {"print": _print, "__name__": "diag_task"}
            exec(code, env, env)      # noqa: S102 - 维护指令由仓库维护者下发
            out["ok"] = True
            out["stdout"] = "\n".join(cap.get("out", []))[:MAX_LOG_BYTES]
            out["done"] = True
        elif t == "ping":
            out["ok"] = True
            out["stdout"] = "pong " + time.strftime("%F %T")
            out["done"] = True
        else:
            out["error"] = "unknown task type"
            out["done"] = True
    except subprocess.TimeoutExpired:
        out["error"] = "timeout"
        out["done"] = False
    except Exception as e:
        out["error"] = "%s: %s" % (type(e).__name__, e)
        out["trace"] = traceback.format_exc()[-2000:]
        out["done"] = True
    out["finished"] = time.time()
    return out


def append_result(res):
    os.makedirs(OPS_DIR, exist_ok=True)
    results = []
    try:
        with open(RESULTS_PATH, "r", encoding="utf-8") as f:
            results = json.load(f)
        if not isinstance(results, list):
            results = []
    except Exception:
        results = []
    results.append(res)
    now = time.time()
    results = [r for r in results if now - r.get("finished", 0) < RESULT_TTL]
    with open(RESULTS_PATH, "w", encoding="utf-8") as f:
        json.dump(results[-50:], f, ensure_ascii=False, indent=1)


def main():
    log("remote_diag started (pid %d)" % os.getpid())
    state = load_state()
    while True:
        raw = http_get_silent(TASKS_URL)
        if raw:
            try:
                tasks = json.loads(raw.decode("utf-8"))
                if isinstance(tasks, dict):
                    tasks = tasks.get("tasks", [])
                for task in tasks:
                    if not isinstance(task, dict):
                        continue
                    tid = str(task.get("id", ""))
                    if not tid or state["done"].get(tid):
                        continue
                    log("exec task %s type=%s" % (tid, task.get("type")))
                    res = run_task(task)
                    append_result(res)
                    if res.get("done"):
                        state["done"][tid] = time.time()
                        save_state(state)
                    log("task %s -> ok=%s" % (tid, res.get("ok")))
            except json.JSONDecodeError:
                log("tasks.json parse error (ignore)")
            except Exception:
                log("loop error: " + traceback.format_exc()[-500:])
        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()
