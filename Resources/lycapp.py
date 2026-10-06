#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
可移植文件搜索器 — 带前端界面的本地应用
==========================================
启动后自动打开浏览器 (http://127.0.0.1:8765), 在搜索框输入关键词即可
毫秒级模糊搜索整盘文件; 支持在系统文件管理器中定位 / 用默认应用打开。

依赖: 仅 Python 3 标准库, 无需安装任何第三方包。
"""
import os
import sys
import json
import time
import socket
import select
import shutil
import sqlite3
import threading
import subprocess
import webbrowser
import http.server
import urllib.parse
import html
import secrets
import queue
import hashlib
from collections import OrderedDict

# 导入同目录的 lycsearch 模块(复制进 app 内, 自包含)
APP_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, APP_DIR)
import lycsearch

VOLUME = lycsearch.VOLUME
MASTER_DB_PATH = lycsearch.DEFAULT_DB

# 本地服务鉴权令牌：由原生壳生成并经环境变量传入；浏览器/命令行模式自生成。
TOKEN = os.environ.get("LYCSEARCH_TOKEN") or secrets.token_urlsafe(32)


def local_fast_db_for(volume, master_db):
    """可写移动盘使用本机 SSD 镜像；只读盘原本已落到本机目录。"""
    if sys.platform == "darwin" and os.path.realpath(volume) == os.sep:
        return master_db
    try:
        master_on_volume = os.path.commonpath(
            [os.path.realpath(master_db), os.path.realpath(volume)]) == \
            os.path.realpath(volume)
    except ValueError:
        master_on_volume = False
    if not master_on_volume:
        return master_db
    if sys.platform == "darwin":
        root = os.path.expanduser("~/Library/Caches/文件搜索器")
    elif os.name == "nt":
        root = os.path.join(
            os.environ.get("LOCALAPPDATA", os.path.expanduser("~")),
            "FileSearcher", "Cache")
    else:
        root = os.path.join(
            os.environ.get("XDG_CACHE_HOME", os.path.expanduser("~/.cache")),
            "file-searcher")
    key = hashlib.sha256(
        os.path.realpath(volume).encode("utf-8")).hexdigest()[:16]
    return os.path.join(root, key, os.path.basename(master_db))


DB_PATH = local_fast_db_for(VOLUME, MASTER_DB_PATH)
INDEX_DIR = os.path.dirname(DB_PATH)
LOCK = os.path.join(INDEX_DIR, ".refresh.lock")
BASE_PORT = 8765
APP_VERSION = "4.0"
VOLUME_NAME = ("全局" if sys.platform == "darwin" and
               os.path.realpath(VOLUME) == os.sep else
               (os.path.basename(VOLUME.rstrip(os.sep)) or VOLUME))
SEARCH_POOL_SIZE = 8
CACHE_LIMIT = 2048

_refreshing = {
    "pid": None, "error": None, "syncing": False,
    "started_at": None, "last_progress": None,
}
_clients = {}
_clients_lock = threading.Lock()
_client_generation = 0
_ever_had_client = False
_search_pool = queue.LifoQueue(maxsize=SEARCH_POOL_SIZE)
_pool_lock = threading.Lock()
_pool_created = 0
_result_cache = OrderedDict()
_cache_lock = threading.Lock()
_warm_lock = threading.Lock()
_mirror_lock = threading.Lock()


def search_locations():
    """返回可筛选的已挂载位置；空值表示全局。"""
    locations = [("", "全局（默认）")]
    home = os.path.expanduser("~")
    locations.append((home, "本机 · " + os.path.basename(home)))
    if sys.platform == "darwin":
        try:
            for name in sorted(os.listdir("/Volumes")):
                path = os.path.join("/Volumes", name)
                if (not name.startswith(".") and os.path.isdir(path) and
                        os.path.realpath(path) != os.sep):
                    locations.append((path, "外置硬盘 · " + name))
        except OSError:
            pass
    return locations


def validated_location(value):
    allowed = {path for path, _ in search_locations()}
    return value if value in allowed else ""


def index_revision(path):
    if not os.path.exists(path):
        return 0
    try:
        conn = sqlite3.connect(path, timeout=3)
        row = conn.execute(
            "SELECT value FROM meta WHERE key='revision'").fetchone()
        conn.close()
        if row:
            return int(row[0])
    except (sqlite3.Error, ValueError, TypeError):
        pass
    try:
        return os.stat(path).st_mtime_ns
    except OSError:
        return 0


def copy_index_atomic(source, destination, progress=None):
    """优先顺序复制已收敛的索引；存在 WAL 时回退 SQLite 在线备份。"""
    os.makedirs(os.path.dirname(destination), exist_ok=True)
    temporary = destination + ".syncing"
    try:
        os.remove(temporary)
    except OSError:
        pass
    wal_path = source + "-wal"
    wal_size = os.path.getsize(wal_path) if os.path.exists(wal_path) else 0
    if wal_size <= 4096:
        total = max(1, os.path.getsize(source))
        copied = 0
        with open(source, "rb", buffering=0) as src, \
                open(temporary, "wb", buffering=0) as dst:
            while True:
                block = src.read(64 * 1024 * 1024)
                if not block:
                    break
                dst.write(block)
                copied += len(block)
                if progress:
                    progress(copied, total)
            os.fsync(dst.fileno())
    else:
        source_conn = sqlite3.connect(source, timeout=30)
        destination_conn = sqlite3.connect(temporary, timeout=30)
        try:
            source_conn.backup(destination_conn, pages=32768, sleep=0.01)
        finally:
            destination_conn.close()
            source_conn.close()
    os.replace(temporary, destination)


def recent_change_count():
    try:
        with open(lycsearch._progress_file(DB_PATH), encoding="utf-8") as handle:
            progress = json.load(handle)
        return sum(int(progress.get(key, 0) or 0)
                   for key in ("added", "changed", "deleted"))
    except (OSError, ValueError, TypeError):
        return 0


def index_total(path):
    if not os.path.exists(path):
        return 0
    try:
        conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=3)
        row = conn.execute(
            "SELECT value FROM meta WHERE key='total'").fetchone()
        conn.close()
        return int(row[0]) if row else 0
    except (sqlite3.Error, ValueError, TypeError):
        return 0


def master_copy_progress(copied, total):
    percent = min(99, int(copied * 100 / max(1, total)))
    _refreshing["last_progress"] = {
        "stage": f"顺序同步主索引（{copied / 1048576:.0f} / "
                 f"{total / 1048576:.0f} MB）",
        "scope": "移动盘主索引", "percent": percent,
        "overall_percent": min(99, 70 + round(percent * .29)),
        "scanned": 0, "estimated": 0, "added": 0,
        "changed": 0, "deleted": 0,
        "elapsed": round(max(
            0, time.time() - (_refreshing.get("started_at") or time.time())), 1),
    }


def prepare_fast_index():
    """首次复制移动盘主索引；之后直接复用本机 SSD 高速镜像。"""
    os.makedirs(INDEX_DIR, exist_ok=True)
    if os.path.realpath(DB_PATH) == os.path.realpath(MASTER_DB_PATH):
        return
    master_revision = index_revision(MASTER_DB_PATH)
    local_revision = index_revision(DB_PATH)
    if master_revision and master_revision > local_revision:
        copy_index_atomic(MASTER_DB_PATH, DB_PATH)


def sync_master_index(track=False):
    """让移动盘主索引独立消费变更日志，避免反复复制 1.6GB。"""
    if (os.path.realpath(DB_PATH) == os.path.realpath(MASTER_DB_PATH) or
            not os.path.exists(DB_PATH) or
            not _mirror_lock.acquire(blocking=False)):
        if track:
            _refreshing["syncing"] = False
        return
    sync_error = None
    try:
        local_revision = index_revision(DB_PATH)
        if local_revision <= index_revision(MASTER_DB_PATH):
            return
        # 大批量删除/移动时，在移动盘 SQLite 上逐条维护多个索引比顺序复制
        # 更慢。超过阈值直接以 64MB 大块连续写入，充分利用 USB 带宽。
        local_total = index_total(DB_PATH)
        master_total = index_total(MASTER_DB_PATH)
        if (recent_change_count() >= 50000 or
                abs(local_total - master_total) >= 50000):
            copy_index_atomic(
                DB_PATH, MASTER_DB_PATH,
                master_copy_progress if track else None)
            return
        if os.path.exists(MASTER_DB_PATH):
            mirror_env = os.environ.copy()
            mirror_env["LYCSEARCH_VOLUME"] = VOLUME
            mirror_env["LYCSEARCH_DB"] = MASTER_DB_PATH
            mirror_env["LYCSEARCH_TURBO"] = "1"
            result = subprocess.run(
                [sys.executable, os.path.join(APP_DIR, "lycsearch.py"), "refresh"],
                env=mirror_env, stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL, timeout=600)
            if result.returncode == 0:
                conn = sqlite3.connect(MASTER_DB_PATH, timeout=30)
                conn.execute(
                    "INSERT OR REPLACE INTO meta(key,value) VALUES('revision',?)",
                    (str(local_revision),))
                conn.commit()
                conn.close()
                return
        copy_index_atomic(
            DB_PATH, MASTER_DB_PATH,
            master_copy_progress if track else None)
    except (OSError, sqlite3.Error, subprocess.SubprocessError) as exc:
        sync_error = str(exc)
    finally:
        _mirror_lock.release()
        if track:
            _refreshing["syncing"] = False
            elapsed = max(0, time.time() - (_refreshing.get("started_at") or time.time()))
            previous = _refreshing.get("last_progress") or {}
            _refreshing["last_progress"] = {
                **previous,
                "stage": "完成" if not sync_error else "同步失败",
                "percent": 100 if not sync_error else previous.get("percent", 0),
                "elapsed": round(elapsed, 1),
                "scope": "全部索引",
            }
            if sync_error:
                _refreshing["error"] = "主索引同步失败：" + sync_error


def start_master_sync(track=False):
    if track:
        try:
            os.remove(lycsearch._progress_file(MASTER_DB_PATH))
        except OSError:
            pass
        # 在线程启动前切换阶段，避免状态接口短暂报告“已完成”。
        _refreshing["syncing"] = True
    threading.Thread(
        target=sync_master_index, args=(track,), daemon=True,
        name="index-mirror").start()


def touch_client(client_id):
    global _client_generation, _ever_had_client
    if not client_id:
        return
    with _clients_lock:
        _clients[client_id] = time.monotonic()
        _ever_had_client = True
        _client_generation += 1


def close_client(client_id):
    global _client_generation
    if not client_id:
        return
    with _clients_lock:
        # 忽略上一次服务遗留页面的迟到关闭事件，避免误杀新进程。
        if client_id not in _clients:
            return
        _clients.pop(client_id, None)
        _client_generation += 1
        generation = _client_generation
    # 给页面刷新留 3 秒重连窗口；最后一个页面关闭后自动退出。
    threading.Timer(3, _quit_if_no_clients, args=(generation,)).start()


def _quit_if_no_clients(generation):
    if os.environ.get("LYCSEARCH_KEEP_ALIVE"):
        return
    with _clients_lock:
        should_quit = generation == _client_generation and not _clients
    if should_quit:
        shutdown_app()


def client_watchdog():
    """浏览器异常退出时清理失联页面，避免后台服务永久残留。"""
    global _client_generation
    while True:
        time.sleep(30)
        if os.environ.get("LYCSEARCH_KEEP_ALIVE"):
            continue
        cutoff = time.monotonic() - 90
        with _clients_lock:
            stale = [client_id for client_id, seen in _clients.items()
                     if seen < cutoff]
            for client_id in stale:
                _clients.pop(client_id, None)
            if stale:
                _client_generation += 1
            should_quit = _ever_had_client and not _clients
        if should_quit:
            shutdown_app()


def shutdown_app():
    """退出服务时同时终止由本 App 启动的刷新子进程。"""
    pid = _refreshing.get("pid")
    if pid:
        try:
            os.kill(pid, 15)
        except OSError:
            pass
    os._exit(0)


# ---------------------------------------------------------------- 工具

def get_free_port():
    for port in range(BASE_PORT, BASE_PORT + 20):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                s.bind(("127.0.0.1", port))
                return port
            except OSError:
                continue
    return BASE_PORT


_WARMED = threading.Event()


def warm_up():
    """后台只预热高频元数据并预建只读查询连接；不再整库读入内存。

    冷启动不被全库顺序读阻塞：首搜直接执行、连接按需创建。此函数仅把
    stats/状态轮询依赖的 meta 头部页读入系统缓存, 并预建连接池
    (创建连接本身不读数据, 毫秒级)。"""
    global _pool_created
    if not _warm_lock.acquire(blocking=False):
        return
    _WARMED.clear()
    try:
        # 预热高频元数据(只读 meta, 毫秒); 不再整库读入内存。
        try:
            c = sqlite3.connect(DB_PATH, timeout=3)
            c.execute("SELECT key, value FROM meta").fetchall()
            c.close()
        except sqlite3.Error:
            pass
        while True:
            with _pool_lock:
                if _pool_created >= SEARCH_POOL_SIZE:
                    break
                _pool_created += 1
            try:
                _search_pool.put_nowait(
                    lycsearch._connect_ro(DB_PATH, cross_thread=True))
            except Exception:
                with _pool_lock:
                    _pool_created -= 1
                break
    except Exception:
        pass
    finally:
        _WARMED.set()
        _warm_lock.release()


def start_warm_up():
    threading.Thread(target=warm_up, daemon=True, name="index-warmup").start()


def acquire_search_conn():
    """最多创建 64 个可跨请求复用的只读连接。"""
    global _pool_created
    try:
        return _search_pool.get_nowait()
    except queue.Empty:
        with _pool_lock:
            if _pool_created < SEARCH_POOL_SIZE:
                _pool_created += 1
                create = True
            else:
                create = False
        if create:
            try:
                return lycsearch._connect_ro(DB_PATH, cross_thread=True)
            except Exception:
                with _pool_lock:
                    _pool_created -= 1
                raise
        return _search_pool.get(timeout=15)


def release_search_conn(conn):
    conn.set_progress_handler(None, 0)
    try:
        _search_pool.put_nowait(conn)
    except queue.Full:
        conn.close()


def clear_result_cache():
    with _cache_lock:
        _result_cache.clear()


def get_cached_result(key):
    with _cache_lock:
        value = _result_cache.get(key)
        if value is not None:
            _result_cache.move_to_end(key)
            return dict(value)
    return None


def put_cached_result(key, value):
    with _cache_lock:
        _result_cache[key] = dict(value)
        _result_cache.move_to_end(key)
        while len(_result_cache) > CACHE_LIMIT:
            _result_cache.popitem(last=False)


def existing_only(response):
    """复核真实文件系统, 区分"已展示条数 / 总命中数 / 精确性"。

    索引会保留已拔出卷的历史记录, 用于卷重新接入后立即搜索, 但离线时
    不应显示。卷层面的离线记录已由 lycsearch 按已挂载卷过滤并从 count
    剔除(在线卷候选总数); 此处逐条确认页面内路径。若仍存在尚未刷新
    导致的失效路径, 返回 count_exact=False 交由界面提示"已显示 N 项,
    索引总命中待刷新", 不再把 count 伪报为页面幸存条数——否则会掩盖
    后续大量结果。truncated 保持分页语义, 不被清零。
    """
    checked = dict(response)
    hits = checked.get("hits", [])
    live = []
    for hit in hits:
        if os.path.exists(hit.get("path", "")):
            live.append(hit)
    checked["hits"] = live
    checked["count_exact"] = (len(live) == len(hits))
    return checked


def wait_warm():
    """首次请求等待预热完成(一次性开销), 之后即时。"""
    if not _WARMED.is_set():
        _WARMED.wait(timeout=90)


def db_stats():
    if not os.path.exists(DB_PATH):
        return {"ok": False, "msg": "索引不存在"}
    try:
        # 独立短连接: 预热线程用主连接扫描时, stats 不排队(只读 meta, 毫秒)
        c = sqlite3.connect(DB_PATH, timeout=3)
        c.execute("PRAGMA busy_timeout=3000")
        # meta 表(构建时写入) → 毫秒; 无 meta(旧库) 才回退全表 COUNT
        row = c.execute(
            "SELECT value FROM meta WHERE key='total'").fetchone()
        if row:
            n = int(row[0])
            nf = int(c.execute(
                "SELECT value FROM meta WHERE key='files'").fetchone()[0])
            updated = c.execute(
                "SELECT value FROM meta WHERE key='updated'").fetchone()[0]
        else:
            n = c.execute("SELECT COUNT(*) FROM files").fetchone()[0]
            nf = c.execute("SELECT COUNT(*) FROM files WHERE is_dir=0").fetchone()[0]
            updated = time.strftime("%Y-%m-%d %H:%M",
                                    time.localtime(os.path.getmtime(DB_PATH)))
        uncovered = []
        try:
            urow = c.execute(
                "SELECT value FROM meta WHERE key='uncovered'").fetchone()
            if urow:
                uncovered = json.loads(urow[0])
        except (sqlite3.Error, ValueError):
            uncovered = []
        c.close()
    except sqlite3.OperationalError as e:
        return {"ok": False, "msg": f"索引读取失败({e})"}
    du = shutil.disk_usage(VOLUME)
    local_refreshing = refresh_running()
    syncing = bool(_refreshing.get("syncing"))
    progress = None
    if local_refreshing:
        try:
            with open(lycsearch._progress_file(DB_PATH), encoding="utf-8") as handle:
                progress = json.load(handle)
        except (OSError, ValueError):
            pass
        if not progress:
            progress = dict(_refreshing.get("last_progress") or {})
        if progress:
            progress["scope"] = "本机高速索引"
            progress["overall_percent"] = min(69, round(progress.get("percent", 0) * .7))
    elif syncing:
        try:
            with open(lycsearch._progress_file(MASTER_DB_PATH), encoding="utf-8") as handle:
                progress = json.load(handle)
        except (OSError, ValueError):
            pass
        if progress:
            progress["scope"] = "移动盘主索引"
            progress["overall_percent"] = min(
                99, 70 + round(progress.get("percent", 0) * .29))
        else:
            progress = dict(_refreshing.get("last_progress") or {}) or {
                "stage": "同步移动盘主索引", "scope": "移动盘主索引",
                "percent": 0, "overall_percent": 70,
                "scanned": 0, "estimated": 0,
                "added": 0, "changed": 0, "deleted": 0,
                "elapsed": round(max(
                    0, time.time() - (_refreshing.get("started_at") or time.time())), 1),
            }
            progress["scope"] = "移动盘主索引"
            progress["overall_percent"] = min(
                99, 70 + round(progress.get("percent", 0) * .29))
    elif _refreshing.get("last_progress"):
        progress = dict(_refreshing["last_progress"])
        progress.setdefault("overall_percent", progress.get("percent", 0))
    running = local_refreshing or syncing
    return {
        "ok": True, "app": "lyc-filesearch", "version": APP_VERSION,
        "volume": os.path.realpath(VOLUME), "volume_name": VOLUME_NAME,
        "fast_cache": os.path.realpath(DB_PATH) != os.path.realpath(MASTER_DB_PATH),
        "entries": n, "files": nf, "dirs": n - nf,
        "updated": updated,
        "db_mb": round(os.path.getsize(DB_PATH) / 1048576, 1),
        "volume_total_gb": round(du.total / (1 << 30), 1),
        "volume_used_gb": round(du.used / (1 << 30), 1),
        "volume_free_gb": round(du.free / (1 << 30), 1),
        "refreshing": running,
        "refresh_phase": ("local" if local_refreshing else
                          "sync" if syncing else "done" if progress else "idle"),
        "refresh_started_at": _refreshing.get("started_at"),
        "refresh_error": _refreshing.get("error"),
        "refresh_progress": progress,
        "warmed": _WARMED.is_set(),
        "uncovered": uncovered,
    }


def refresh_running():
    # 1) 本 App 触发的子进程
    pid = _refreshing.get("pid")
    if pid:
        try:
            os.kill(pid, 0)
            return True
        except OSError:
            _refreshing["pid"] = None
    # 2) 其他进程(命令行 build/refresh)留下的锁文件
    lock = os.path.join(INDEX_DIR, ".building.lock")
    if os.path.exists(lock):
        try:
            with open(lock) as f:
                lpid = int(f.read().strip() or "0")
        except (OSError, ValueError):
            lpid = 0
        if lpid:
            try:
                os.kill(lpid, 0)
                return True
            except OSError:
                pass  # 锁文件残留, 视为可清理
        try:
            os.remove(lock)
        except OSError:
            pass
    return False


def start_refresh():
    if refresh_running() or _refreshing.get("syncing"):
        return False, "索引正在刷新中, 请稍候"
    _refreshing["error"] = None
    _refreshing["started_at"] = time.time()
    _refreshing["last_progress"] = {
        "stage": "准备刷新", "scope": "本机高速索引",
        "percent": 0, "overall_percent": 0,
        "scanned": 0, "estimated": 0,
        "added": 0, "changed": 0, "deleted": 0, "elapsed": 0,
    }
    clear_result_cache()
    try:
        os.remove(lycsearch._progress_file(DB_PATH))
    except OSError:
        pass
    command = [sys.executable, os.path.join(APP_DIR, "lycsearch.py"), "refresh"]
    # 刷新进程不显示终端，但保持正常 I/O 优先级。ExFAT 下使用
    # taskpolicy -b 会将大量目录读取严重限速，反而使刷新慢数倍。
    refresh_env = os.environ.copy()
    refresh_env["LYCSEARCH_VOLUME"] = VOLUME
    refresh_env["LYCSEARCH_DB"] = DB_PATH
    # 本机高速镜像可从主索引重建：刷新时换取最高写入吞吐。
    refresh_env["LYCSEARCH_TURBO"] = "1"
    p = subprocess.Popen(
        command, env=refresh_env,
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    _refreshing["pid"] = p.pid
    threading.Thread(target=_wait_refresh, args=(p,), daemon=True).start()
    return True, "开始刷新索引"


def _wait_refresh(p):
    code = p.wait()
    # 本机刷新成功后立即占住“同步中”状态，避免接口短暂显示 100% 后
    # 又跳回 70%，也防止用户在这个缝隙重复提交刷新。
    if not code:
        _refreshing["syncing"] = True
    try:
        with open(lycsearch._progress_file(DB_PATH), encoding="utf-8") as handle:
            _refreshing["last_progress"] = json.load(handle)
    except (OSError, ValueError):
        pass
    _refreshing["pid"] = None
    if code:
        _refreshing["error"] = f"索引刷新失败（退出码 {code}）"
        if _refreshing.get("last_progress"):
            _refreshing["last_progress"]["stage"] = "刷新失败"
    else:
        clear_result_cache()
        start_warm_up()
        start_master_sync(track=True)


def path_exists_in_index(path):
    try:
        c = sqlite3.connect(DB_PATH, timeout=3)
        r = c.execute("SELECT 1 FROM files WHERE path=?", (path,)).fetchone()
        c.close()
        return r is not None
    except sqlite3.OperationalError:
        return False


def reveal_in_finder(path):
    if sys.platform == "darwin":
        command = ["open", "-R", path]
    elif os.name == "nt":
        command = ["explorer", "/select,", os.path.normpath(path)]
    else:
        command = ["xdg-open", os.path.dirname(path)]
    return subprocess.Popen(command, stdout=subprocess.DEVNULL,
                            stderr=subprocess.DEVNULL)


def open_with_default(path):
    if os.name == "nt":
        os.startfile(path)
        return None
    command = ["open", path] if sys.platform == "darwin" else ["xdg-open", path]
    return subprocess.Popen(command, stdout=subprocess.DEVNULL,
                            stderr=subprocess.DEVNULL)


def preview_with_quicklook(path):
    """用 macOS 原生 Quick Look 预览；同时只保留一个预览窗口。"""
    if sys.platform == "darwin":
        subprocess.run(["/usr/bin/pkill", "-x", "qlmanage"],
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        return subprocess.Popen(["/usr/bin/qlmanage", "-p", path],
                                stdout=subprocess.DEVNULL,
                                stderr=subprocess.DEVNULL)
    return open_with_default(path)


# ---------------------------------------------------------------- 路由

class Handler(http.server.BaseHTTPRequestHandler):
    server_version = "LYCSearch/" + APP_VERSION

    def log_message(self, *a):
        pass

    def _client_gone(self):
        """供 SQLite progress handler 调用；浏览器取消旧搜索后终止 SQL。"""
        try:
            readable, _, _ = select.select([self.connection], [], [], 0)
            return bool(readable and
                        self.connection.recv(1, socket.MSG_PEEK) == b"")
        except (OSError, ValueError):
            return True

    def _send(self, code, body, ctype="application/json; charset=utf-8"):
        if isinstance(body, (dict, list)):
            body = json.dumps(body, ensure_ascii=False).encode("utf-8")
        elif isinstance(body, str):
            body = body.encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        try:
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError):
            # 用户继续输入时前一次搜索会被浏览器取消。
            pass

    def _api_authorized(self):
        """所有 /api/* 请求都必须携带有效令牌，防止本机其他进程/网页调用。"""
        return secrets.compare_digest(
            self.headers.get("X-Lyc-Token", ""), TOKEN)

    def _origin_allowed(self):
        """POST 副作用请求校验来源：浏览器必须来自本服务自身；
        无 Origin 的非浏览器客户端（如命令行）依赖令牌即可。"""
        origin = self.headers.get("Origin")
        if not origin:
            return True
        try:
            p = urllib.parse.urlparse(origin)
            host = p.hostname
            port = p.port
        except (ValueError, AttributeError):
            return False
        if port is None:
            port = 443 if p.scheme == "https" else 80
        return host in ("127.0.0.1", "localhost", "::1") and \
            port == self.server.server_port

    def do_GET(self):
        u = urllib.parse.urlparse(self.path)
        q = urllib.parse.parse_qs(u.query)
        if u.path == "/":
            return self._send(200, INDEX_HTML, "text/html; charset=utf-8")
        if u.path == "/refresh":
            return self._send(200, REFRESH_HTML, "text/html; charset=utf-8")
        if u.path == "/api/stats":
            # 只读统计（供启动探测与状态轮询）不校验令牌；其余 API 一律校验。
            return self._send(200, db_stats())
        # 打开/定位/预览已改为 POST；GET 一律拒绝。
        if u.path in ("/api/reveal", "/api/open", "/api/preview"):
            return self._send(405, {"error": "method not allowed, use POST"})
        if u.path.startswith("/api/"):
            if not self._api_authorized():
                return self._send(403, {"error": "unauthorized"})
            if u.path == "/api/search":
                return self.api_search(q)
            return self._send(404, {"error": "not found"})
        return self._send(404, {"error": "not found"})

    def do_POST(self):
        u = urllib.parse.urlparse(self.path)
        q = urllib.parse.parse_qs(u.query)
        if u.path.startswith("/api/"):
            if not self._api_authorized():
                return self._send(403, {"error": "unauthorized"})
            if not self._origin_allowed():
                return self._send(403, {"error": "origin rejected"})
        if u.path == "/api/heartbeat":
            touch_client(q.get("id", [""])[0])
            return self._send(200, {"ok": True})
        if u.path == "/api/client-close":
            close_client(q.get("id", [""])[0])
            return self._send(200, {"ok": True})
        if u.path == "/api/refresh":
            ok, msg = start_refresh()
            return self._send(200, {"ok": ok, "msg": msg})
        if u.path == "/api/quit":
            threading.Timer(0.3, shutdown_app).start()
            return self._send(200, {"ok": True})
        if u.path in ("/api/reveal", "/api/open", "/api/preview"):
            return self.api_action(q, u.path[len("/api/"):])
        return self._send(404, {"error": "not found"})

    # ---- API 实现 ----

    def api_search(self, q):
        kws = [w for w in q.get("q", [""])[0].split() if w]
        if not kws:
            return self._send(200, {"hits": [], "count": 0, "elapsed_ms": 0})
        kind = q.get("kind", ["all"])[0]
        if kind not in ("all", "file", "dir"):
            kind = "all"
        raw_types = q.get("types", q.get("type", [""]))
        type_filter = tuple(sorted({name for value in raw_types
                                    for name in value.split(",")
                                    if name in lycsearch.TYPE_EXTS}))
        try:
            limit = max(1, min(int(q.get("limit", ["100"])[0]), 500))
        except (TypeError, ValueError):
            limit = 100
        sort = q.get("sort", ["relevance"])[0]
        if sort not in ("relevance", "name", "size", "time", "path"):
            sort = "relevance"
        full = q.get("full", [""])[0] == "1"
        location = validated_location(q.get("location", [""])[0])
        # 具体类型天然只命中文件；目录范围下保留但忽略类型条件。
        if type_filter and kind != "dir":
            kind = "file"
        active_types = () if kind == "dir" else type_filter
        file_only = kind == "file"
        dir_only = kind == "dir"

        # 首搜直接执行, 不等待整库预热; 连接按需创建/复用连接池。
        cache_key = (tuple(kws), kind, active_types, limit, sort, full, location)
        cached = get_cached_result(cache_key)
        if cached is not None:
            cached = existing_only(cached)
            cached["elapsed_ms"] = 0.1
            cached["cached"] = True
            return self._send(200, cached)

        t0 = time.time()
        # 64 路连接池允许并行读；mmap 页由操作系统跨连接共享。
        # 进度回调会在前端 AbortController 取消时终止旧 SQL。
        search_conn = acquire_search_conn()
        search_conn.set_progress_handler(self._client_gone, 10000)
        try:
            if full:
                # 完整路径: 文件名与路径命中已由 search 内部 UNION 去重，
                # 返回准确总数（不再用样本去重比例估算）。
                rows, _, total = lycsearch.search(
                    DB_PATH, kws, file_only=file_only, dir_only=dir_only,
                    type_filter=active_types, limit=limit, sort_by=sort,
                    conn=search_conn, return_total=True,
                    path_prefix=location or None)
            else:
                rows, _, total = lycsearch.search(
                    DB_PATH, kws, file_only=file_only, dir_only=dir_only,
                    type_filter=active_types, limit=limit, sort_by=sort,
                    conn=search_conn, return_total=True,
                    path_prefix=location or None)
        except Exception as e:
            release_search_conn(search_conn)
            msg = str(e)
            if "interrupted" in msg.lower():
                return
            if "locked" in msg or "busy" in msg.lower():
                return self._send(200, {"error": "索引正在更新中, 请稍后重试"})
            return self._send(200, {"error": msg})
        release_search_conn(search_conn)
        hits = []
        for path, is_dir, size, mtime in rows:
            hits.append({
                "path": path,
                "name": os.path.basename(path),
                "is_dir": bool(is_dir),
                "size": size,
                "size_h": lycsearch.human_size(size),
                "mtime": mtime,
                "time_h": time.strftime("%Y-%m-%d %H:%M", time.localtime(mtime)) if mtime else "-",
                "ext": (os.path.splitext(path)[1] or "").lstrip(".").lower(),
            })
        response = {
            "hits": hits,
            "count": int(total),
            "elapsed_ms": round((time.time() - t0) * 1000, 1),
            "truncated": len(hits) < total,
        }
        put_cached_result(cache_key, response)
        return self._send(200, existing_only(response))

    def api_action(self, q, action):
        path = q.get("path", [""])[0]
        if not path or not path_exists_in_index(path) or not os.path.exists(path):
            return self._send(404, {"error": "文件当前不存在或所在硬盘未连接"})
        try:
            if action == "reveal":
                reveal_in_finder(path)
            elif action == "preview":
                preview_with_quicklook(path)
            else:
                open_with_default(path)
            return self._send(200, {"ok": True})
        except Exception as e:
            return self._send(500, {"error": str(e)})


class HighConcurrencyHTTPServer(http.server.ThreadingHTTPServer):
    """本机交互服务；有界队列避免过期查询争抢搜索资源。"""
    request_queue_size = 32
    daemon_threads = True
    block_on_close = False


# ---------------------------------------------------------------- 前端

INDEX_HTML = r"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>__VOLUME_NAME__ 文件搜索器</title>
<style>
:root{color-scheme:dark light;
--bg:#f5f5f7;
--glass:rgba(255,255,255,.72);--glass2:rgba(255,255,255,.86);--glass3:rgba(118,118,128,.08);
--hair:rgba(0,0,0,.09);--hair2:rgba(0,0,0,.14);
--text:#1d1d1f;--sub:#6e6e73;--muted:#aeaeb2;
--accent:#007aff;--accent-deep:#0064d2;--accent-tint:rgba(0,122,255,.10);
--ok:#34c759;--danger:#ff3b30;--mark:rgba(0,122,255,.18);
--ring:0 0 0 4px rgba(0,122,255,.22);
--row-hover:rgba(0,0,0,.045);
--shadow:0 12px 40px rgba(0,0,0,.10),0 1px 2px rgba(0,0,0,.05);
--font:-apple-system,BlinkMacSystemFont,"SF Pro Text","SF Pro Display","PingFang SC","Helvetica Neue",sans-serif}
@media(prefers-color-scheme:dark){:root{
--bg:#1a1a1c;
--glass:rgba(40,40,44,.55);--glass2:rgba(48,48,52,.66);--glass3:rgba(44,44,48,.42);
--hair:rgba(255,255,255,.10);--hair2:rgba(255,255,255,.16);
--text:#f5f5f7;--sub:#98989d;--muted:#636366;
--accent:#0a84ff;--accent-deep:#3395ff;--accent-tint:rgba(10,132,255,.16);
--ok:#30d158;--danger:#ff453a;--mark:rgba(10,132,255,.30);
--row-hover:rgba(255,255,255,.06);
--shadow:0 12px 40px rgba(0,0,0,.42),0 1px 2px rgba(0,0,0,.3)}}
*{box-sizing:border-box}
body{margin:0;min-height:100vh;background:var(--bg);color:var(--text);
font-family:var(--font);font-size:13px;line-height:1.5;-webkit-font-smoothing:antialiased}
body::before{content:"";position:fixed;inset:0;z-index:-1;pointer-events:none;background:var(--bg)}
button,input,select{font:inherit;color:inherit}
::-webkit-scrollbar{width:9px;height:9px}
::-webkit-scrollbar-thumb{background:var(--hair2);border-radius:6px;border:2px solid transparent;background-clip:content-box}
::-webkit-scrollbar-thumb:hover{background:var(--muted);background-clip:content-box}
::-webkit-scrollbar-track{background:transparent}
::selection{background:var(--mark)}
.wrap{max-width:980px;margin:auto;padding:0 28px 52px}

/* ---------- 顶栏：macOS 工具栏 ---------- */
header{position:sticky;top:0;z-index:20;display:flex;align-items:center;gap:12px;
min-height:58px;margin:0 -28px 4px;padding:8px 28px;
background:rgba(246,246,246,.82);backdrop-filter:blur(30px) saturate(180%);-webkit-backdrop-filter:blur(30px) saturate(180%);
border-bottom:1px solid var(--hair)}
@media(prefers-color-scheme:dark){header{background:rgba(30,30,32,.82)}}
.appmark{width:30px;height:30px;display:grid;place-items:center;border-radius:8px;
color:#fff;background:linear-gradient(180deg,#3b9cff,#087bf5);box-shadow:inset 0 0 0 .5px rgba(255,255,255,.35),0 1px 3px rgba(0,82,180,.3)}
.appmark svg{width:17px;height:17px}
.brand{font-size:15px;font-weight:650;letter-spacing:-.01em;white-space:nowrap;display:flex;align-items:baseline;gap:8px}
.brand small{font-weight:500;color:var(--muted);font-size:11.5px;letter-spacing:0}
.window-drag-zone{align-self:stretch;flex:1;min-width:36px;cursor:grab}
.window-drag-zone:active{cursor:grabbing}
.head-right{margin-left:0;display:flex;gap:8px;align-items:center;min-width:0}
.badge{display:flex;align-items:center;gap:7px;max-width:300px;overflow:hidden;text-overflow:ellipsis;
font-size:12px;color:var(--sub);background:var(--glass2);backdrop-filter:blur(20px) saturate(180%);-webkit-backdrop-filter:blur(20px) saturate(180%);
border:1px solid var(--hair);padding:5px 12px;border-radius:99px;white-space:nowrap;
box-shadow:0 1px 3px rgba(0,0,0,.05)}
.badge .dot{width:7px;height:7px;border-radius:50%;background:var(--ok);flex:none}
.badge .dot.err{background:var(--danger)}
.iconbtn{display:grid;place-items:center;width:29px;height:29px;border-radius:8px;border:1px solid var(--hair);
background:var(--glass2);backdrop-filter:blur(20px) saturate(180%);-webkit-backdrop-filter:blur(20px) saturate(180%);
color:var(--sub);cursor:pointer;transition:.15s;flex:none}
.iconbtn:hover{color:var(--text);border-color:var(--hair2);transform:translateY(-1px)}
.iconbtn:active{transform:scale(.95)}
.iconbtn svg{width:14.5px;height:14.5px}
.iconbtn.quit:hover{color:var(--danger);border-color:rgba(255,59,48,.4)}

/* ---------- 搜索区 ---------- */
.search-panel{padding:24px 0 4px}
.searchbox{display:flex;align-items:center;gap:2px;padding:5px 10px 5px 14px;height:52px;
background:var(--glass2);border:1px solid var(--hair2);border-radius:12px;box-shadow:0 1px 3px rgba(0,0,0,.06);
transition:border-color .16s,box-shadow .16s}
.searchbox:focus-within{border-color:var(--accent);box-shadow:var(--ring),var(--shadow)}
.search-icon{display:grid;place-items:center;color:var(--muted);flex:none;margin-right:6px}
.search-icon svg{width:19px;height:19px}
.searchbox input{flex:1;min-width:0;background:transparent;border:0;outline:0;
padding:8px 6px;font-size:17px;letter-spacing:-.01em;color:var(--text)}
.searchbox input::placeholder{color:var(--muted)}
.clear{display:none;border:0;background:transparent;color:var(--muted);cursor:pointer;
padding:6px;border-radius:50%;line-height:0;transition:.14s}
.clear:hover{color:var(--text);background:var(--row-hover)}
.clear.show{display:block}
.clear svg{width:14px;height:14px}
.shortcut{align-self:center;margin:0 8px 0 4px;color:var(--muted);font-size:11px;
border:1px solid var(--hair);border-bottom-width:2px;border-radius:6px;padding:2px 8px;
user-select:none;background:var(--glass3)}

/* ---------- 工具栏 ---------- */
.toolbar{display:flex;align-items:center;gap:9px;flex-wrap:wrap;margin-top:12px}
.tb-group{display:flex;align-items:center;gap:7px}
.tb-label{font-size:12px;color:var(--muted)}
select{appearance:none;-webkit-appearance:none;color:var(--text);
background:var(--glass2);backdrop-filter:blur(20px) saturate(180%);-webkit-backdrop-filter:blur(20px) saturate(180%);
border:1px solid var(--hair);border-radius:8px;padding:5px 26px 5px 11px;font-size:12.5px;outline:0;cursor:pointer;
background-image:url("data:image/svg+xml;charset=utf-8,%3Csvg xmlns='http://www.w3.org/2000/svg' width='10' height='6' viewBox='0 0 10 6'%3E%3Cpath d='M1 1l4 4 4-4' stroke='%23999' stroke-width='1.5' fill='none' stroke-linecap='round'/%3E%3C/svg%3E");
background-repeat:no-repeat;background-position:right 9px center;transition:.15s;
box-shadow:0 1px 2px rgba(0,0,0,.04)}
select:hover{border-color:var(--hair2)}
select:focus{border-color:var(--accent);box-shadow:var(--ring)}
select.compact{max-width:210px}
.vline{width:1px;height:17px;background:var(--hair2);margin:0 3px}
.chips{display:flex;gap:2px;padding:2px;border-radius:9px;
background:var(--glass3);border:1px solid var(--hair);transition:opacity .18s,filter .18s}
.chip{font-size:12px;color:var(--sub);background:transparent;border:0;padding:4.5px 11px;
border-radius:7px;cursor:pointer;transition:color .14s,background .14s,box-shadow .14s,transform .08s;user-select:none;white-space:nowrap}
.chip:hover{color:var(--text);background:var(--row-hover)}
.chip:active{transform:scale(.96);background:var(--hair)}
.chip.on{color:#fff;background:var(--accent);font-weight:600;
box-shadow:0 1px 4px rgba(0,122,255,.35)}
.chip.on:hover{background:var(--accent-deep)}
.chips.disabled{opacity:.38;filter:saturate(.35);pointer-events:none}
.chips.disabled .chip{cursor:default}
.chip.toggle{border:0;padding:4.5px 11px}
.chip.toggle.on{color:#fff;background:var(--accent)}

/* ---------- 状态栏 ---------- */
.statusbar{display:flex;align-items:center;gap:10px;min-height:36px;padding:9px 4px 5px}
.status{font-size:12.5px;color:var(--sub);flex:1;min-width:0;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.status b{color:var(--accent);font-weight:650}
.status .spin{margin-right:2px}
.result-tools{display:none;gap:7px}
.result-tools.show{display:flex}
.mini{display:flex;align-items:center;gap:5px;font-size:11.5px;color:var(--sub);
background:var(--glass2);backdrop-filter:blur(20px);-webkit-backdrop-filter:blur(20px);
border:1px solid var(--hair);border-radius:7px;padding:4px 10px;cursor:pointer;transition:.14s}
.mini:hover{color:var(--text);border-color:var(--hair2)}
.mini svg{width:12px;height:12px}

/* ---------- 结果列表：原生列表卡 ---------- */
.list{display:flex;flex-direction:column;background:var(--glass2);
border:1px solid var(--hair);border-radius:12px;overflow:hidden;box-shadow:0 1px 3px rgba(0,0,0,.05)}
.list:empty{display:none}
.row{display:grid;grid-template-columns:36px minmax(0,1fr) auto auto;align-items:center;gap:12px;
padding:9px 14px 9px 12px;cursor:default;transition:background .1s;position:relative}
.row + .row::before{content:"";position:absolute;left:58px;right:0;top:0;height:1px;background:var(--hair);
transform:scaleY(.6)}
.row:hover{background:var(--row-hover)}
.row:focus-visible{outline:2px solid var(--accent);outline-offset:-2px;z-index:1}
.row:hover + .row::before,.row:hover::before{opacity:0}
.row.sel{background:var(--accent-tint)}
.row.sel:hover{background:var(--accent-tint)}
.ic{width:36px;height:36px;display:grid;place-items:center;color:var(--sub);flex:none}
.ic svg{width:22px;height:22px}
.ic.dir{color:var(--accent)}
.body{min-width:0}
.name{font-size:13.5px;font-weight:600;letter-spacing:-.005em;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.name mark{background:var(--mark);color:inherit;border-radius:3.5px;padding:0 1.5px;font-weight:700}
.path{font-size:11.5px;color:var(--muted);white-space:nowrap;overflow:hidden;text-overflow:ellipsis;
margin-top:1.5px;direction:rtl;text-align:left}
.meta{text-align:right;font-size:11px;color:var(--muted);line-height:1.5;font-variant-numeric:tabular-nums;flex:none}
.meta .sz{display:block;color:var(--sub);font-size:11.5px;font-weight:500}
.acts{display:flex;gap:3px;opacity:0;transition:opacity .12s}
.row:hover .acts,.row.sel .acts{opacity:1}
.acts .a{display:grid;place-items:center;width:27px;height:27px;border-radius:7.5px;border:1px solid transparent;
background:transparent;color:var(--muted);cursor:pointer;transition:.13s}
.acts .a:hover{color:var(--accent);background:var(--glass2);border-color:var(--hair);
box-shadow:0 1px 3px rgba(0,0,0,.08)}
.acts .a:active{transform:scale(.92)}
.acts .a svg{width:14.5px;height:14.5px}

/* ---------- 空状态 / 骨架 ---------- */
.empty{margin:13vh 0 8vh;padding:24px;text-align:center;color:var(--muted);font-size:13.5px;
background:transparent;border:0;box-shadow:none;animation:fade .2s ease}
.empty svg{width:46px;height:46px;color:var(--hair2);margin-bottom:14px}
.empty strong{display:block;color:var(--sub);font-size:15.5px;font-weight:700;margin-bottom:6px;letter-spacing:-.01em}
.empty small{display:block;margin-top:5px;color:var(--muted);font-size:12.5px;line-height:1.8}
.empty kbd{border:1px solid var(--hair);border-bottom-width:2px;border-radius:5px;
padding:1px 6px;font-size:11px;font-family:var(--font);margin:0 1px;background:var(--glass3)}
.skeleton{height:50px;border-radius:0;margin:0;position:relative;overflow:hidden}
.skeleton + .skeleton{border-top:1px solid var(--hair)}
.skeleton:after{content:"";position:absolute;inset:0;
background:linear-gradient(100deg,transparent 30%,var(--row-hover) 50%,transparent 70%);
background-size:220% 100%;animation:shimmer 1.2s infinite}

/* ---------- 浮层 / 页脚 ---------- */
.toast{position:fixed;left:50%;top:20px;z-index:40;transform:translate(-50%,-14px);opacity:0;
background:var(--glass2);backdrop-filter:blur(28px) saturate(180%);-webkit-backdrop-filter:blur(28px) saturate(180%);
border:1px solid var(--hair2);box-shadow:var(--shadow);
padding:9px 18px;border-radius:12px;font-size:13px;font-weight:500;transition:.22s;pointer-events:none}
.toast.show{opacity:1;transform:translate(-50%,0)}
footer{margin-top:22px;padding-top:13px;border-top:1px solid var(--hair);font-size:11px;color:var(--muted);
display:flex;justify-content:space-between;gap:12px;line-height:1.7;font-variant-numeric:tabular-nums}
footer b{color:var(--sub);font-weight:600}
kbd{background:var(--glass3);border:1px solid var(--hair);border-bottom-width:2px;border-radius:5px;
padding:1px 5px;font-size:10px;font-family:var(--font)}
.spin{display:inline-block;width:12px;height:12px;border:2px solid var(--hair2);
border-top-color:var(--accent);border-radius:50%;animation:rot .75s linear infinite;vertical-align:-2px}
@keyframes rot{to{transform:rotate(360deg)}}
@keyframes fade{from{opacity:0;transform:translateY(3px)}to{opacity:1}}
@keyframes shimmer{to{background-position:-220% 0}}
/* ---------- Liquid Glass window composition ---------- */
html,body{height:100%;background:transparent;overflow:hidden}
body::before{background:rgba(246,246,248,.94)}
.wrap{width:100%;max-width:none;height:100vh;min-height:0;margin:0;padding:0 22px 24px;display:flex;flex-direction:column}
header{min-height:52px;margin:0 -22px;padding:7px 22px 7px 86px;background:rgba(246,246,248,.88);
backdrop-filter:blur(22px) saturate(135%);-webkit-backdrop-filter:blur(22px) saturate(135%);
border-bottom:.5px solid var(--hair);box-shadow:0 .5px 0 rgba(255,255,255,.45)}
.appmark{width:28px;height:28px;border-radius:7px;background:var(--accent);box-shadow:inset 0 0 0 .5px rgba(255,255,255,.32),0 1px 2px rgba(0,0,0,.14)}
.brand{font-size:14px}.badge,.iconbtn,.mini{background:rgba(255,255,255,.82);box-shadow:inset 0 .5px 0 rgba(255,255,255,.72)}
.search-panel{padding:18px 4px 14px}
.searchbox{max-width:980px;height:56px;margin:auto;padding:5px 12px 5px 17px;border-radius:16px;
background:rgba(255,255,255,.90);border:.5px solid rgba(255,255,255,.98);
backdrop-filter:blur(30px) saturate(145%);-webkit-backdrop-filter:blur(30px) saturate(145%);
box-shadow:0 12px 28px rgba(0,0,0,.08),0 1px 3px rgba(0,0,0,.08),inset 0 .5px 0 rgba(255,255,255,.9)}
.searchbox:focus-within{border-color:color-mix(in srgb,var(--accent) 70%,white);box-shadow:var(--ring),0 16px 34px rgba(0,0,0,.10),inset 0 .5px 0 rgba(255,255,255,.9)}
.searchbox input{font-size:18px;font-weight:450}.shortcut{background:rgba(118,118,128,.08)}
.workspace{display:grid;grid-template-columns:218px minmax(0,1fr);gap:14px;align-items:stretch;width:100%;max-width:1380px;flex:1;min-height:0;margin:0 auto}
.sidebar{min-height:0;padding:10px;overflow:auto;border-radius:16px;background:rgba(249,249,251,.88);
border:.5px solid rgba(255,255,255,.56);backdrop-filter:blur(24px) saturate(130%);-webkit-backdrop-filter:blur(24px) saturate(130%);
box-shadow:inset 0 .5px 0 rgba(255,255,255,.62),0 1px 3px rgba(0,0,0,.05)}
.sidebar .toolbar{display:block;margin:0}.sidebar .tb-group{display:block;margin:0 0 12px}
.sidebar .tb-label{display:block;margin:0 7px 5px;font-size:10.5px;font-weight:650;letter-spacing:.025em;text-transform:uppercase}
.sidebar select{width:100%;max-width:none!important;background-color:rgba(255,255,255,.76);border-color:transparent;box-shadow:none;padding-top:6px;padding-bottom:6px}
.sidebar select:hover{background-color:rgba(255,255,255,.96)}
.sidebar .vline{display:none}
.sidebar-label{margin:5px 7px 6px;color:var(--muted);font-size:10.5px;font-weight:650;letter-spacing:.025em;text-transform:uppercase}
.sidebar #typeChips{display:block;padding:2px;background:transparent;border:0}
.sidebar #typeChips .chip{display:flex;width:100%;align-items:center;padding:6px 9px;margin:1px 0;border-radius:7px;font-size:12.5px}
.sidebar #typeChips .chip::before{content:"";width:6px;height:6px;border-radius:50%;margin-right:9px;background:var(--hair2)}
.sidebar #typeChips .chip.on{color:var(--text);background:var(--accent-tint);box-shadow:none;font-weight:600}
.sidebar #typeChips .chip.on::before{background:var(--accent)}
.sidebar #typeChips .chip[data-v="all"]{margin-bottom:4px}
.sidebar #fullChip{display:flex;width:100%;margin-top:7px;padding:6px 9px;border-radius:7px;color:var(--sub);font-size:12.5px}
.sidebar #fullChip.on{color:var(--accent);background:var(--accent-tint);box-shadow:none}
.content{min-width:0;min-height:0;display:flex;flex-direction:column}
.content-toolbar{display:flex;align-items:center;gap:12px;min-height:45px;padding:4px 4px 9px}
.content-toolbar #kindChips{flex:none;padding:2px;border:.5px solid var(--hair);border-radius:8px;background:rgba(118,118,128,.10);box-shadow:inset 0 .5px 1px rgba(0,0,0,.05)}
.content-toolbar #kindChips .chip{min-width:58px;padding:4px 12px;border-radius:6px}
.content-toolbar #kindChips .chip.on{color:var(--text);background:rgba(255,255,255,.78);box-shadow:0 1px 3px rgba(0,0,0,.16),inset 0 .5px 0 rgba(255,255,255,.8)}
.content-toolbar .statusbar{flex:1;min-width:0;padding:0;min-height:32px}.content-toolbar .status{text-align:right}
.content-surface{min-height:0;flex:1;display:flex;flex-direction:column;background:rgba(255,255,255,.97);border:.5px solid rgba(0,0,0,.08);border-radius:16px;overflow:hidden;
box-shadow:0 5px 20px rgba(0,0,0,.055),inset 0 .5px 0 rgba(255,255,255,.85)}
.list{border:0;border-radius:0;background:transparent;box-shadow:none;flex:1;min-height:0;overflow:auto}.row{padding:9px 13px}.row + .row::before{height:.5px}
.empty{margin:11vh 0 8vh}.content-surface footer{margin:0 14px;padding:12px 0;border-top:.5px solid var(--hair)}
.toast{top:62px;background:rgba(255,255,255,.72);border:.5px solid rgba(255,255,255,.8);backdrop-filter:blur(30px) saturate(140%);-webkit-backdrop-filter:blur(30px) saturate(140%)}
@media(prefers-color-scheme:dark){
body::before{background:rgba(25,25,28,.95)}
header{background:rgba(31,31,34,.90);box-shadow:0 .5px 0 rgba(255,255,255,.08)}
.searchbox{background:rgba(48,48,52,.93);border-color:rgba(255,255,255,.15);box-shadow:0 14px 30px rgba(0,0,0,.25),inset 0 .5px 0 rgba(255,255,255,.12)}
.sidebar{background:rgba(38,38,42,.92);border-color:rgba(255,255,255,.09);box-shadow:inset 0 .5px 0 rgba(255,255,255,.08)}
.sidebar select,.badge,.iconbtn,.mini{background-color:rgba(66,66,71,.82)}
.sidebar select:hover{background-color:rgba(82,82,87,.94)}
.content-toolbar #kindChips .chip.on{background:rgba(105,105,112,.58);box-shadow:0 1px 3px rgba(0,0,0,.36),inset 0 .5px 0 rgba(255,255,255,.12)}
.content-surface{background:rgba(31,31,34,.97);border-color:rgba(255,255,255,.11);box-shadow:0 8px 24px rgba(0,0,0,.24),inset 0 .5px 0 rgba(255,255,255,.07)}
.toast{background:rgba(48,48,52,.76);border-color:rgba(255,255,255,.12)}}
@media(prefers-reduced-transparency:reduce){body::before{background:var(--bg)}header,.searchbox,.sidebar,.content-surface,.badge,.iconbtn,.mini,.toast{background:var(--bg);backdrop-filter:none;-webkit-backdrop-filter:none}.content-surface{background:var(--glass2)}}
@media(prefers-contrast:more){header,.searchbox,.sidebar,.content-surface,.chips,select,.iconbtn,.badge{border-width:1px;border-color:var(--hair2)}.muted,.tb-label{color:var(--sub)}}
@media(max-width:900px){.workspace{grid-template-columns:180px minmax(0,1fr)}.brand small,.badge{display:none}.content-toolbar .status{display:none}}
@media(max-width:760px){
.wrap{height:auto;min-height:100vh;display:block;padding:0 12px 18px}html,body{height:auto;overflow:auto}header{margin:0 -12px;padding-left:78px;padding-right:14px}.brand small,.shortcut,.vline{display:none}
.search-panel{padding:12px 0}.searchbox{height:49px;border-radius:13px}.workspace{display:block;min-height:0}.sidebar{height:auto;margin-bottom:9px;padding:8px}.content-surface{min-height:360px}
.sidebar .toolbar{display:flex;align-items:end;gap:7px;overflow-x:auto}.sidebar .tb-group{min-width:140px;margin:0}.sidebar-label{display:none}
.sidebar #typeChips{display:flex;min-width:max-content}.sidebar #typeChips .chip{width:auto;padding:5px 9px}.sidebar #typeChips .chip::before{display:none}
.sidebar #typeChips .chip[data-v="all"]{margin:1px}.sidebar #fullChip{width:auto;min-width:max-content;margin:1px 0;padding:5px 9px}
.content-toolbar{padding-left:1px;padding-right:1px}.content-toolbar #kindChips{width:100%;display:grid;grid-template-columns:repeat(3,1fr)}
.row{grid-template-columns:30px minmax(0,1fr) auto;gap:9px;padding:8px 11px}.row + .row::before{left:50px}.ic{width:30px;height:30px}.ic svg{width:18px;height:18px}
.meta{display:none}.acts{grid-column:3;flex-direction:column;opacity:1}.path{direction:ltr}.list{overflow:visible}.content-surface footer{display:block}}
@media(prefers-reduced-motion:reduce){*{animation:none!important;transition:none!important}}
</style>
</head>
<body>
<div class="wrap">
  <header>
    <div class="appmark" aria-hidden="true"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round"><circle cx="10.5" cy="10.5" r="6.5"/><path d="m15.5 15.5 4 4"/></svg></div>
    <div class="brand">文件搜索器<small>__VOLUME_NAME__索引</small></div>
    <div class="window-drag-zone" aria-hidden="true" title="拖动窗口"></div>
    <div class="head-right">
      <span class="badge" id="stBadge"><i class="dot"></i><span>正在连接…</span></span>
      <button class="iconbtn" id="btnAuth" style="display:none" title="为未覆盖的文件夹授权并纳入索引"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="4" y="11" width="16" height="9" rx="2"/><path d="M8 11V7a4 4 0 0 1 8 0v4"/></svg></button>
      <button class="iconbtn" id="btnRefresh" title="索引管理与刷新"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21 12a9 9 0 1 1-2.6-6.3"/><path d="M21 3v6h-6"/></svg></button>
      <button class="iconbtn quit" id="btnQuit" title="退出文件搜索器"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"><path d="M18 6 6 18M6 6l12 12"/></svg></button>
    </div>
  </header>

  <section class="search-panel">
    <div class="searchbox">
      <span class="search-icon"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"><circle cx="11" cy="11" r="7"/><path d="m20 20-3.5-3.5"/></svg></span>
      <input id="q" type="text" autocomplete="off" spellcheck="false" aria-label="搜索文件"
             placeholder="搜索所有文件 — 多关键词用空格分隔，支持 * ? 通配符">
      <button class="clear" id="btnClear" title="清空 (Esc)"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"><path d="M18 6 6 18M6 6l12 12"/></svg></button>
      <span class="shortcut">/</span>
    </div>
  </section>

  <div class="workspace">
    <aside class="sidebar" aria-label="搜索筛选">
      <div class="toolbar">
        <div class="tb-group"><span class="tb-label">位置</span>
          <select id="location" class="compact" aria-label="检索位置">__LOCATION_OPTIONS__</select></div>
        <div class="tb-group"><span class="tb-label">排序</span>
          <select id="sortSel" aria-label="排序方式">
            <option value="relevance">相关度</option><option value="time">最新优先</option>
            <option value="name">按名称</option><option value="size">按大小</option>
          </select></div>
        <div class="sidebar-label">文件类型</div>
        <div class="chips" id="typeChips" role="group" aria-label="文件类型">
          <span class="chip on" role="checkbox" aria-checked="true" data-k="type" data-v="all">全部类型</span><span class="chip" role="checkbox" aria-checked="false" data-k="type" data-v="图片">图片</span><span class="chip" role="checkbox" aria-checked="false" data-k="type" data-v="视频">视频</span><span class="chip" role="checkbox" aria-checked="false" data-k="type" data-v="音频">音频</span><span class="chip" role="checkbox" aria-checked="false" data-k="type" data-v="文档">文档</span><span class="chip" role="checkbox" aria-checked="false" data-k="type" data-v="压缩">压缩包</span><span class="chip" role="checkbox" aria-checked="false" data-k="type" data-v="代码">代码</span>
        </div>
        <span class="chip toggle" id="fullChip" data-k="full" data-v="1" title="文件夹路径也参与匹配">搜索完整路径</span>
      </div>
    </aside>

    <main class="content">
      <div class="content-toolbar">
        <div class="chips" id="kindChips" role="radiogroup" aria-label="对象范围">
          <span class="chip on" role="radio" aria-checked="true" data-k="kind" data-v="all">全部</span><span class="chip" role="radio" aria-checked="false" data-k="kind" data-v="file">文件</span><span class="chip" role="radio" aria-checked="false" data-k="kind" data-v="dir">文件夹</span>
        </div>
        <div class="statusbar">
          <div class="status" id="status" aria-live="polite"></div>
          <div class="result-tools" id="resultTools">
            <button class="mini" id="copyFirst"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"><rect x="9" y="9" width="12" height="12" rx="2.5"/><path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"/></svg>复制路径</button>
          </div>
        </div>
      </div>
      <div class="content-surface">
        <div class="list" id="list"></div>
        <footer>
          <div id="ftStats">正在读取索引信息…</div>
          <div><kbd>↑</kbd><kbd>↓</kbd> 选择 · <kbd>⏎</kbd> 打开 · <kbd>空格</kbd> 预览 · <kbd>单击</kbd> 定位 · <kbd>双击</kbd> 打开</div>
        </footer>
      </div>
    </main>
  </div>
</div>
<div class="toast" id="toast"></div>

<script>
window.__LYC_TOKEN__ = "__LYC_TOKEN__";
const $=s=>document.querySelector(s);
const clientId=(crypto.randomUUID?crypto.randomUUID():Date.now()+"-"+Math.random());
function apiHeaders(){return {"X-Lyc-Token": window.__LYC_TOKEN__||""};}
function heartbeat(){
  fetch("/api/heartbeat?id="+encodeURIComponent(clientId),
        {method:"POST",keepalive:true,headers:apiHeaders()}).catch(()=>{});
}
heartbeat();setInterval(heartbeat,15000);
window.addEventListener("pagehide",e=>{
  if(!e.persisted)fetch("/api/client-close?id="+encodeURIComponent(clientId),
        {method:"POST",keepalive:true,headers:apiHeaders()}).catch(()=>{});
});

/* ---- SF Symbols 风格线性图标 ---- */
const P={
folder:'<path d="M3.5 6.5A2 2 0 0 1 5.5 4.5h4l2 2.5h7a2 2 0 0 1 2 2v8a2 2 0 0 1-2 2h-13a2 2 0 0 1-2-2z"/>',
doc:'<path d="M6 3.5h8l4 4V19a1.5 1.5 0 0 1-1.5 1.5h-10A1.5 1.5 0 0 1 5 19V5A1.5 1.5 0 0 1 6.5 3.5z"/><path d="M14 3.5V8h4"/><path d="M8.5 13h7M8.5 16.5h5"/>',
image:'<rect x="3.5" y="5" width="17" height="14" rx="2"/><circle cx="9" cy="10" r="1.6"/><path d="m6 19 5.5-6 4 4.2 2.5-2.7L20.5 17"/>',
video:'<rect x="3.5" y="5.5" width="17" height="13" rx="2.5"/><path d="m10.5 9.5 4.5 2.5-4.5 2.5z"/>',
music:'<path d="M9 18.5V6l10-2v12.5"/><circle cx="6.8" cy="18.5" r="2.3"/><circle cx="16.8" cy="16.5" r="2.3"/>',
sheet:'<rect x="4" y="4" width="16" height="16" rx="2"/><path d="M4 9.5h16M4 15h16M9.5 4v16M15 4v16" stroke-width="1.2"/>',
archive:'<rect x="3.5" y="4.5" width="17" height="5" rx="1.5"/><path d="M5.5 9.5V18a1.8 1.8 0 0 0 1.8 1.8h9.4A1.8 1.8 0 0 0 18.5 18V9.5"/><path d="M10 13h4"/>',
code:'<path d="m8.5 8-4.5 4 4.5 4M15.5 8l4.5 4-4.5 4"/>',
disc:'<circle cx="12" cy="12" r="8.5"/><circle cx="12" cy="12" r="2.6"/>'};
const CAT={jpg:"image",jpeg:"image",png:"image",gif:"image",webp:"image",heic:"image",heif:"image",bmp:"image",svg:"image",raw:"image",cr2:"image",nef:"image",tif:"image",tiff:"image",psd:"image",
mp4:"video",mov:"video",mkv:"video",avi:"video",wmv:"video",flv:"video",m4v:"video",webm:"video",mpg:"video",mpeg:"video",rmvb:"video","3gp":"video","m2ts":"video",mts:"video",
mp3:"music",wav:"music",flac:"music",aac:"music",m4a:"music",ogg:"music",wma:"music",ape:"music",opus:"music",
xls:"sheet",xlsx:"sheet",csv:"sheet",numbers:"sheet",
zip:"archive",rar:"archive","7z":"archive",tar:"archive",gz:"archive",bz2:"archive",xz:"archive",dmg:"archive",iso:"archive",
py:"code",js:"code",ts:"code",java:"code",c:"code",cpp:"code",h:"code",html:"code",css:"code",json:"code",xml:"code",sql:"code",sh:"code",swift:"code",go:"code",rs:"code",rb:"code"};
function svgIcon(kind){return '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round">'+(P[kind]||P.doc)+'</svg>';}
function iconFor(h){
  if(h.is_dir)return svgIcon("folder");
  const c=CAT[h.ext];
  if(c)return svgIcon(c);
  return svgIcon("doc");
}

class SearchFilter{
  constructor({range="all",types=[],full=false,sort="relevance",location=""}={}){
    this.range=["all","file","dir"].includes(range)?range:"all";
    const validTypes=new Set(["图片","视频","音频","文档","压缩","代码"]);
    this.types=new Set((Array.isArray(types)?types:(typeof types==="string"&&types?[types]:[])).filter(type=>validTypes.has(type)));
    if(this.types.size&&this.range!=="dir")this.range="file";
    this.full=!!full;this.sort=sort;this.location=location;
  }
  setRange(range){if(["all","file","dir"].includes(range))this.range=range;}
  toggleType(type){
    if(this.range==="dir")return;
    if(type==="all"){this.types.clear();return;}
    this.types.has(type)?this.types.delete(type):this.types.add(type);
    if(this.types.size)this.range="file";
  }
  activeTypes(){return this.range==="dir"?[]:[...this.types].sort();}
  params(){return {kind:this.range,types:this.activeTypes().join(","),full:this.full?"1":"",sort:this.sort,location:this.location};}
  json(){return {range:this.range,types:[...this.types],full:this.full,sort:this.sort,location:this.location};}
}
const searchFilter=new SearchFilter();
let state={q:"",sel:-1,hits:[],total:0,elapsed:0,timer:null,loading:false,kws:[]};
let searchSeq=0, searchCtl=null, sawRefresh=false;

function el(html){const d=document.createElement("div");d.innerHTML=html.trim();return d.firstChild;}
function esc(s){return String(s??"").replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;").replace(/"/g,"&quot;");}
function hl(name,kws){
  let out=esc(name);
  for(const kw of kws){
    const k=kw.replace(/[*?]/g,"").toLowerCase(); if(!k) continue;
    const re=new RegExp("("+k.replace(/[.*+?^${}()|[\]\\]/g,"\\$&")+")","gi");
    out=out.replace(re,"<mark>$1</mark>");
  }
  return out;
}
function parentOf(h){return h.path.length>h.name.length+1?h.path.slice(0,h.path.length-h.name.length-1):"−";}

const SVG_PREVIEW='<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M2.5 12S6 5.8 12 5.8 21.5 12 21.5 12 18 18.2 12 18.2 2.5 12 2.5 12z"/><circle cx="12" cy="12" r="3"/></svg>';
const SVG_REVEAL='<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="m3.5 8 4-4h9l4 4"/><path d="M3.5 8v11a1.5 1.5 0 0 0 1.5 1.5h14a1.5 1.5 0 0 0 1.5-1.5V8"/><path d="m9.5 13.5 2.5 2.5 4.5-5"/></svg>';
const SVG_COPY='<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round"><rect x="9" y="9" width="12" height="12" rx="2.5"/><path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"/></svg>';
const SVG_OPEN='<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M14 4h6v6"/><path d="M20 4 11 13"/><path d="M19 14v5a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V7a2 2 0 0 1 2-2h5"/></svg>';

function render(){
  const list=$("#list");
  $("#resultTools").classList.toggle("show",!!state.hits.length&&!state.loading);
  if(!state.q){
    list.innerHTML='<div class="empty"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.4" stroke-linecap="round"><circle cx="11" cy="11" r="7"/><path d="m20 20-3.5-3.5"/><path d="M8.2 11h5.6M11 8.2v5.6" opacity=".55"/></svg><strong>随时搜索你的所有文件</strong><small>支持多关键词、<kbd>*</kbd><kbd>?</kbd> 通配符与路径匹配 · 输入即搜<br>单击结果在 Finder 中定位 · 双击打开 · 空格键快速预览</small></div>';
    return;
  }
  if(state.loading){list.innerHTML=Array.from({length:7},()=>'<div class="skeleton"></div>').join("");return;}
  if(!state.hits.length){
    list.innerHTML='<div class="empty"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.4" stroke-linecap="round"><circle cx="11" cy="11" r="7"/><path d="m20 20-3.5-3.5"/><path d="M8.5 11h5"/></svg><strong>没有找到“'+esc(state.q)+'”</strong><small>试试减少关键词 · 切换到全局位置 · 开启“搜路径”</small></div>';
    return;
  }
  const frag=document.createDocumentFragment();
  state.hits.forEach((h,i)=>{
    const r=el('<div class="row '+(i===state.sel?"sel":"")+'" data-i="'+i+'" tabindex="0" title="'+esc(h.path)+'">'+
      '<div class="ic '+(h.is_dir?"dir":"")+'">'+iconFor(h)+'</div>'+
      '<div class="body"><div class="name">'+hl(h.name,state.kws)+'</div>'+
      '<div class="path">'+esc(parentOf(h))+'</div></div>'+
      '<div class="meta"><span class="sz">'+(h.is_dir?"文件夹":h.size_h)+'</span>'+esc(h.time_h)+'</div>'+
      '<div class="acts">'+
        '<button class="a" data-a="preview" title="快速预览 (空格)">'+SVG_PREVIEW+'</button>'+
        '<button class="a" data-a="reveal" title="在 Finder 中显示 (单击)">'+SVG_REVEAL+'</button>'+
        '<button class="a" data-a="copy" title="复制路径">'+SVG_COPY+'</button>'+
        '<button class="a" data-a="open" title="打开 (双击)">'+SVG_OPEN+'</button>'+
      '</div></div>');
    let clickTimer=null;
    r.addEventListener("click",e=>{
      const a=e.target.closest("[data-a]");
      if(a){e.stopPropagation();
        if(clickTimer){clearTimeout(clickTimer);clickTimer=null;}
        if(a.dataset.a==="copy")copyPath(h.path);
        else act(h.path,a.dataset.a);
        return;}
      /* 单击 = 在 Finder 中定位；双击的第一段 click 会取消本次定时 */
      if(clickTimer){clearTimeout(clickTimer);clickTimer=null;return;}
      clickTimer=setTimeout(()=>{clickTimer=null;act(h.path,"reveal");},240);
    });
    r.addEventListener("dblclick",e=>{
      if(e.target.closest("[data-a]"))return;
      if(clickTimer){clearTimeout(clickTimer);clickTimer=null;}
      act(h.path,"open");
    });
    r.addEventListener("focus",()=>{state.sel=i;});
    r.addEventListener("keydown",e=>{
      if(e.key==="Enter"){e.preventDefault();act(h.path,"open");}
      else if(e.key===" "){e.preventDefault();act(h.path,"preview");}
    });
    frag.appendChild(r);
  });
  list.innerHTML="";list.appendChild(frag);
}

async function act(path,kind){
  try{
    const r=await fetch("/api/"+kind+"?path="+encodeURIComponent(path),
      {method:"POST",headers:apiHeaders()});
    const j=await r.json();
    if(!r.ok&&j.error)flash(j.error);
    else if(kind==="preview")flash("已打开快速预览");
    else if(kind==="reveal")flash("已在 Finder 中定位");
  }catch(e){flash("操作失败: "+e);}
}
async function copyPath(p){
  try{await navigator.clipboard.writeText(p);flash("已复制路径");}
  catch(e){flash("复制失败");}
}

async function doSearch(){
  const seq=++searchSeq;
  if(searchCtl)searchCtl.abort();
  searchCtl=new AbortController();
  state.q=$("#q").value.trim();
  $("#btnClear").classList.toggle("show",!!state.q);
  if(!state.q){state.hits=[];state.total=0;state.sel=-1;state.loading=false;render();setStatus("");syncUrl();return;}
  state.loading=true;render();
  const params=new URLSearchParams({q:state.q,...searchFilter.params(),limit:200});
  try{
    const r=await fetch("/api/search?"+params,{signal:searchCtl.signal,headers:apiHeaders()});
    const j=await r.json();
    if(seq!==searchSeq)return;
    if(j.error){state.hits=[];setStatus("⚠ "+j.error);state.loading=false;render();return;}
    state.hits=j.hits||[];state.total=j.count||0;state.elapsed=j.elapsed_ms||0;
    state.kws=state.q.split(/\s+/).filter(Boolean);
    state.sel=-1;
    const shown=state.hits.length, scope=$("#location").selectedOptions[0]?.textContent||"全局";
    let countText;
    if(j.count_exact===false){
      countText="已显示 "+shown+" 项 · 索引总命中待刷新";
    }else{
      countText="<b>"+state.total.toLocaleString()+"</b> 项"+(j.truncated?" · 显示前 "+shown+" 项":"");
    }
    setStatus(countText+" · "+esc(scope)+" · "+state.elapsed+" ms"+(j.cached?" · 缓存":""));
    syncUrl();savePrefs();
  }catch(e){
    if(e.name==="AbortError"||seq!==searchSeq)return;
    state.hits=[];setStatus("搜索出错: "+e);
  }
  if(seq!==searchSeq)return;
  state.loading=false;render();
}

function setStatus(t){$("#status").innerHTML=t;}
let toastTimer;
function flash(msg){const t=$("#toast");t.textContent=msg;t.classList.add("show");clearTimeout(toastTimer);toastTimer=setTimeout(()=>t.classList.remove("show"),1600);}

function syncUrl(){const u=new URL(location.href);if(state.q)u.searchParams.set("q",state.q);else u.searchParams.delete("q");history.replaceState(null,"",u);}
function savePrefs(){localStorage.setItem("lycsearch-prefs",JSON.stringify({
  full:searchFilter.full,sort:searchFilter.sort,location:searchFilter.location
}));}
function syncChips(){
  document.querySelectorAll("#kindChips .chip").forEach(c=>{
    const on=c.dataset.v===searchFilter.range;c.classList.toggle("on",on);c.setAttribute("aria-checked",String(on));
  });
  document.querySelectorAll("#typeChips .chip").forEach(c=>{
    const on=c.dataset.v==="all"?!searchFilter.types.size:searchFilter.types.has(c.dataset.v);
    c.classList.toggle("on",on);c.setAttribute("aria-checked",String(on));
  });
  const typesDisabled=searchFilter.range==="dir";
  $("#typeChips").classList.toggle("disabled",typesDisabled);
  $("#typeChips").setAttribute("aria-disabled",String(typesDisabled));
  $("#fullChip").classList.toggle("on",searchFilter.full);
  $("#sortSel").value=searchFilter.sort;
}
function moveSel(d){
  if(!state.hits.length)return;
  state.sel=(state.sel+d+state.hits.length)%state.hits.length;
  render();
  const row=$('.row[data-i="'+state.sel+'"]');
  if(row)row.scrollIntoView({block:"nearest"});
}

async function loadStats(){
  try{
    const j=await(await fetch("/api/stats",{headers:apiHeaders()})).json();
    if(!j.ok){$("#stBadge").innerHTML='<i class="dot err"></i><span>索引不可用</span>';return;}
    $("#btnAuth").style.display=((j.uncovered||[]).length)?"grid":"none";
    if(j.refreshing){
      sawRefresh=true;
      const p=j.refresh_progress;
      const detail=p?(p.stage+" "+p.percent+"%"):"准备中…";
      $("#stBadge").innerHTML='<span class="spin"></span><span>'+esc(detail)+'</span>';
      if(p)setStatus("后台刷新："+esc(detail));
    }
    else if(!j.warmed){$("#stBadge").innerHTML='<span class="spin"></span><span>正在预热高速索引</span>';}
    else{
      const _unc=(j.uncovered||[]).map(p=>p.split("/").filter(Boolean).pop()||p);
      if(_unc.length){
        $("#stBadge").innerHTML='<i class="dot err"></i><span>未覆盖：'+esc(_unc.join("、"))+' · 点击上方授权</span>';
      }else{
        $("#stBadge").innerHTML='<i class="dot"></i><span>'+j.entries.toLocaleString()+' 项已就绪</span>';
      }
      if(sawRefresh){
        sawRefresh=false;
        if(j.refresh_error)setStatus("⚠ "+j.refresh_error);
        else if(state.q)doSearch();
      }
    }
    $("#ftStats").innerHTML="索引 "+j.db_mb+" MB · <b>"+j.files.toLocaleString()+"</b> 个文件 · <b>"+j.dirs.toLocaleString()+"</b> 个文件夹 · 更新于 "+esc(j.updated);
    setTimeout(loadStats,j.refreshing||!j.warmed?1500:10000);
  }catch(e){
    $("#stBadge").innerHTML='<i class="dot err"></i><span>服务连接中断</span>';
    setTimeout(loadStats,2500);
  }
}

/* ---- 事件 ---- */
$("#q").addEventListener("input",()=>{
  clearTimeout(state.timer);
  state.timer=setTimeout(doSearch,120);
});
$("#btnClear").addEventListener("click",()=>{$("#q").value="";doSearch();$("#q").focus();});
$("#location").addEventListener("change",e=>{searchFilter.location=e.target.value;savePrefs();if(state.q)doSearch();});
$("#sortSel").addEventListener("change",e=>{searchFilter.sort=e.target.value;savePrefs();if(state.q)doSearch();});
document.querySelectorAll("#kindChips .chip, #typeChips .chip").forEach(ch=>{
  ch.addEventListener("click",()=>{
    if(ch.dataset.k==="kind")searchFilter.setRange(ch.dataset.v);else searchFilter.toggleType(ch.dataset.v);
    syncChips();savePrefs();
    if(state.q)doSearch();
  });
});
$("#fullChip").addEventListener("click",()=>{
  searchFilter.full=!searchFilter.full;syncChips();savePrefs();
  if(state.q)doSearch();
});
$("#copyFirst").addEventListener("click",()=>{
  const it=state.hits[state.sel>=0?state.sel:0];
  if(it)copyPath(it.path);
});
$("#q").addEventListener("keydown",e=>{
  if(e.key==="Enter"){
    if(state.hits.length)act(state.hits[state.sel>=0?state.sel:0].path,"open");
  }else if(e.key==="ArrowDown"){e.preventDefault();moveSel(1);}
  else if(e.key==="ArrowUp"){e.preventDefault();moveSel(-1);}
  else if(e.key==="Escape"){$("#q").value="";doSearch();}
});
$("#btnAuth").addEventListener("click",()=>{
  if(window.webkit&&window.webkit.messageHandlers&&window.webkit.messageHandlers.nativeApp){
    window.webkit.messageHandlers.nativeApp.postMessage("requestFolderAccess");
  }else{
    flash("当前为浏览器/命令行模式：请在系统设置 › 隐私与安全性 › 文件与文件夹中为“文件搜索器”授权，再手动刷新");
  }
});
$("#btnRefresh").addEventListener("click",()=>{location.href="/refresh";});
$("#btnQuit").addEventListener("click",async()=>{
  await fetch("/api/quit",{method:"POST",headers:apiHeaders()});
  document.body.innerHTML='<div style="padding:80px;text-align:center;color:#8a93a3;font-family:-apple-system,sans-serif">服务已停止，可以关闭本页</div>';
});
window.addEventListener("keydown",e=>{
  const inInput=document.activeElement===$("#q");
  if(e.key==="/"&&!inInput){e.preventDefault();$("#q").focus();}
  else if((e.metaKey||e.ctrlKey)&&e.key.toLowerCase()==="k"){e.preventDefault();$("#q").focus();$("#q").select();}
  else if(!inInput){
    if(e.key==="ArrowDown"){e.preventDefault();moveSel(1);}
    else if(e.key==="ArrowUp"){e.preventDefault();moveSel(-1);}
    else if(e.key==="Enter"&&state.hits.length){act(state.hits[state.sel>=0?state.sel:0].path,"open");}
    else if(e.key===" "&&state.hits.length){e.preventDefault();act(state.hits[state.sel>=0?state.sel:0].path,"preview");}
    else if((e.metaKey||e.ctrlKey)&&e.key.toLowerCase()==="c"&&state.hits.length){
      e.preventDefault();copyPath(state.hits[state.sel>=0?state.sel:0].path);}
  }
});
loadStats();
// 恢复偏好 & URL 预填
try{
  const p=JSON.parse(localStorage.getItem("lycsearch-prefs")||"{}");
  // 每次打开都从“对象全部 + 类型全部”开始；仅保留位置、排序和搜路径偏好。
  const restored=new SearchFilter({range:"all",types:[],full:p.full,sort:p.sort,location:p.location});
  Object.assign(searchFilter,restored);
  if([...$("#location").options].some(o=>o.value===searchFilter.location))$("#location").value=searchFilter.location;else searchFilter.location="";
  syncChips();savePrefs();
}catch(e){syncChips();}
const _up=new URLSearchParams(location.search);
if(_up.get("q")){$("#q").value=_up.get("q");$("#btnClear").classList.add("show");doSearch();}else render();
$("#q").focus();
</script>
</body>
</html>"""
INDEX_HTML = INDEX_HTML.replace(
    "__VOLUME_NAME__", html.escape(VOLUME_NAME, quote=True))
INDEX_HTML = INDEX_HTML.replace(
    '"__LYC_TOKEN__"', json.dumps(TOKEN))
INDEX_HTML = INDEX_HTML.replace(
    "__LOCATION_OPTIONS__", "".join(
        '<option value="{}">{}</option>'.format(
            html.escape(path, quote=True), html.escape(label))
        for path, label in search_locations()))


REFRESH_HTML = r"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>__VOLUME_NAME__ · 索引管理</title>
<style>
:root{color-scheme:dark light;
--bg:#f5f5f7;
--glass:rgba(255,255,255,.72);--glass2:rgba(255,255,255,.86);--glass3:rgba(118,118,128,.08);
--hair:rgba(0,0,0,.09);--hair2:rgba(0,0,0,.14);
--text:#1d1d1f;--sub:#6e6e73;--muted:#aeaeb2;
--accent:#007aff;--accent-deep:#0064d2;--accent-tint:rgba(0,122,255,.10);
--ok:#34c759;--warn:#ff9f0a;--danger:#ff3b30;
--shadow:0 12px 40px rgba(0,0,0,.10),0 1px 2px rgba(0,0,0,.05);
--font:-apple-system,BlinkMacSystemFont,"SF Pro Text","SF Pro Display","PingFang SC","Helvetica Neue",sans-serif;
--mono:ui-monospace,SFMono-Regular,Menlo,monospace}
@media(prefers-color-scheme:dark){:root{
--bg:#1a1a1c;
--glass:rgba(40,40,44,.55);--glass2:rgba(48,48,52,.66);--glass3:rgba(44,44,48,.42);
--hair:rgba(255,255,255,.10);--hair2:rgba(255,255,255,.16);
--text:#f5f5f7;--sub:#98989d;--muted:#636366;
--accent:#0a84ff;--accent-deep:#3395ff;--accent-tint:rgba(10,132,255,.16);
--ok:#30d158;--warn:#ffd60a;--danger:#ff453a;
--shadow:0 12px 40px rgba(0,0,0,.42),0 1px 2px rgba(0,0,0,.3)}}
*{box-sizing:border-box}
body{margin:0;background:var(--bg);color:var(--text);font-family:var(--font);
font-size:13px;line-height:1.55;-webkit-font-smoothing:antialiased}
body::before{content:"";position:fixed;inset:0;z-index:-1;pointer-events:none;background:var(--bg)}
::-webkit-scrollbar{width:9px;height:9px}
::-webkit-scrollbar-thumb{background:var(--hair2);border-radius:6px;border:2px solid transparent;background-clip:content-box}
::-webkit-scrollbar-track{background:transparent}
.wrap{max-width:900px;margin:auto;padding:0 22px 52px}

header{position:sticky;top:0;z-index:20;display:flex;align-items:center;gap:11px;
min-height:58px;margin:0 -22px 4px;padding:8px 24px;
background:rgba(246,246,246,.82);backdrop-filter:blur(30px) saturate(180%);-webkit-backdrop-filter:blur(30px) saturate(180%);
border-bottom:1px solid var(--hair)}
@media(prefers-color-scheme:dark){header{background:rgba(30,30,32,.82)}}
.appmark{width:30px;height:30px;display:grid;place-items:center;border-radius:8px;color:#fff;
background:linear-gradient(180deg,#3b9cff,#087bf5);box-shadow:inset 0 0 0 .5px rgba(255,255,255,.35),0 1px 3px rgba(0,82,180,.3)}
.appmark svg{width:17px;height:17px}
.brand{font-size:15px;font-weight:650;letter-spacing:-.01em;white-space:nowrap}
.brand small{font-weight:500;color:var(--muted);margin-left:8px;font-size:11.5px;letter-spacing:0}
.back{margin-left:auto;display:flex;align-items:center;gap:6px;color:var(--sub);text-decoration:none;
background:var(--glass2);backdrop-filter:blur(20px) saturate(180%);-webkit-backdrop-filter:blur(20px) saturate(180%);
border:1px solid var(--hair);padding:6px 13px;border-radius:8px;font-size:12.5px;transition:.15s}
.back:hover{color:var(--text);border-color:var(--hair2)}
.back:active{transform:scale(.97)}
.back svg{width:13px;height:13px}

.card{background:var(--glass2);border:1px solid var(--hair);border-radius:12px;padding:22px;margin-top:16px;box-shadow:0 1px 3px rgba(0,0,0,.05)}
.topline{display:flex;align-items:flex-start;gap:14px}
.stateIcon{width:44px;height:44px;border-radius:12px;background:var(--accent-tint);color:var(--accent);
display:grid;place-items:center;flex:none}
.stateIcon svg{width:21px;height:21px}
.stateIcon.ok{color:var(--ok);background:rgba(52,199,89,.12)}
.stateIcon.err{color:var(--danger);background:rgba(255,59,48,.12)}
.state{flex:1;min-width:0}
.state h2{font-size:16px;margin:0 0 3px;letter-spacing:-.01em;font-weight:700}
.state h2.error{color:var(--danger)}
.state p{font-size:12.5px;color:var(--sub);margin:0;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.start{border:0;border-radius:10px;padding:9px 18px;background:var(--accent);color:#fff;
font-weight:600;font-size:13px;cursor:pointer;transition:.15s;flex:none;
box-shadow:0 2px 8px rgba(0,122,255,.35)}
.start:hover{background:var(--accent-deep)}
.start:active{transform:scale(.97)}
.start:disabled{opacity:.45;cursor:not-allowed;box-shadow:none}

.progressHead{display:flex;justify-content:space-between;align-items:baseline;font-size:12.5px;margin-top:22px}
.progressHead span:first-child{color:var(--sub);min-width:0;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.percent{font-size:19px;font-weight:700;color:var(--accent);font-variant-numeric:tabular-nums}
.bar{height:6px;background:var(--glass3);border:1px solid var(--hair);border-radius:6px;overflow:hidden;margin:9px 0 2px}
.fill{height:100%;width:0;background:var(--accent);border-radius:6px;
transition:width .3s ease;position:relative}
.fill.running:after{content:"";position:absolute;inset:0;
background:linear-gradient(90deg,transparent,rgba(255,255,255,.35),transparent);
animation:sweep 1.25s linear infinite}
@keyframes sweep{from{transform:translateX(-100%)}to{transform:translateX(100%)}}

.metrics{display:grid;grid-template-columns:repeat(4,1fr);gap:10px;margin-top:18px}
.metric{background:var(--glass3);border:1px solid var(--hair);border-radius:11px;padding:12px 14px}
.metric label{display:block;color:var(--muted);font-size:11px;margin-bottom:5px}
.metric strong{font-size:16px;font-variant-numeric:tabular-nums;letter-spacing:-.01em;font-weight:650}
.metric small{font-size:11px;color:var(--muted);margin-left:4px}

.sectionTitle{font-size:13px;font-weight:650;margin:0 0 15px;color:var(--sub);letter-spacing:.01em}
.timeline{display:grid;grid-template-columns:repeat(6,1fr);position:relative}
.timeline:before{content:"";position:absolute;top:14px;left:9%;right:9%;height:2px;background:var(--hair2)}
.step{text-align:center;position:relative;z-index:1}
.step .dot{width:28px;height:28px;border-radius:50%;margin:0 auto 8px;background:var(--glass2);
border:1.5px solid var(--hair2);display:grid;place-items:center;color:var(--muted);
font-size:11px;font-weight:650;transition:.2s;font-variant-numeric:tabular-nums}
.step.done .dot{background:var(--ok);border-color:var(--ok);color:#fff}
.step.active .dot{background:var(--accent);border-color:var(--accent);color:#fff;
box-shadow:0 0 0 4px var(--accent-tint)}
.step b{font-size:12px;display:block;font-weight:600}
.step span{font-size:10.5px;color:var(--muted);display:block;margin-top:2px}

.details{display:grid;grid-template-columns:1fr 1fr;gap:0 16px}
.detailRows{display:grid;gap:9px}
.detail{display:flex;justify-content:space-between;gap:12px;border-bottom:1px solid var(--hair);
padding-bottom:8px;font-size:12.5px}
.detail:last-child{border-bottom:0;padding-bottom:0}
.detail span:first-child{color:var(--muted);flex:none}
.detail b{font-weight:600;text-align:right;min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.log{height:170px;overflow:auto;background:var(--glass3);border:1px solid var(--hair);
border-radius:11px;padding:11px 13px;font:11.5px/1.8 var(--mono);color:var(--sub)}
.log .t{color:var(--muted)}
.log .ok{color:var(--ok)}
.log .err{color:var(--danger)}
.note{font-size:12px;color:var(--muted);line-height:1.7;margin:13px 0 0}
html,body{min-height:100%;background:transparent}body::before{background:rgba(246,246,248,.94)}
.wrap{max-width:1040px;padding:0 24px 42px}
header{min-height:52px;margin:0 -24px 10px;padding:7px 24px 7px 88px;background:rgba(246,246,248,.88);
backdrop-filter:blur(22px) saturate(135%);-webkit-backdrop-filter:blur(22px) saturate(135%);border-bottom:.5px solid var(--hair);box-shadow:inset 0 .5px 0 rgba(255,255,255,.5)}
.appmark{width:28px;height:28px;border-radius:7px;background:var(--accent);box-shadow:inset 0 .5px 0 rgba(255,255,255,.35),0 1px 2px rgba(0,0,0,.12)}
.back{background:rgba(255,255,255,.82);border:.5px solid rgba(255,255,255,.92);box-shadow:inset 0 .5px 0 rgba(255,255,255,.7)}
.card{background:rgba(255,255,255,.97);border:.5px solid rgba(0,0,0,.08);border-radius:16px;box-shadow:0 5px 20px rgba(0,0,0,.055),inset 0 .5px 0 rgba(255,255,255,.85)}
.metric,.log{background:rgba(118,118,128,.065);border:.5px solid var(--hair)}
@media(prefers-color-scheme:dark){body::before{background:rgba(25,25,28,.95)}header{background:rgba(31,31,34,.90);box-shadow:inset 0 .5px 0 rgba(255,255,255,.07)}.back{background:rgba(66,66,71,.82);border-color:rgba(255,255,255,.12)}.card{background:rgba(31,31,34,.97);border-color:rgba(255,255,255,.11);box-shadow:0 8px 24px rgba(0,0,0,.24),inset 0 .5px 0 rgba(255,255,255,.07)}}
@media(prefers-reduced-transparency:reduce){body::before,header,.back,.card{background:var(--bg);backdrop-filter:none;-webkit-backdrop-filter:none}.card{background:var(--glass2)}}
@media(prefers-contrast:more){header,.back,.card,.metric,.log{border-width:1px;border-color:var(--hair2)}}
@media(max-width:700px){
.metrics{grid-template-columns:1fr 1fr}
.timeline{grid-template-columns:repeat(3,1fr);gap:18px 0}
.timeline:before{display:none}
.details{grid-template-columns:1fr}
.brand small{display:none}
.appmark{width:28px;height:28px}.wrap{padding:0 12px 28px}header{margin:0 -12px;padding-left:78px;padding-right:14px}.card{padding:17px}}
@media(prefers-reduced-motion:reduce){*{animation:none!important;transition:none!important}}
</style>
</head>
<body><div class="wrap">
<header>
  <div class="appmark" aria-hidden="true"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round"><circle cx="10.5" cy="10.5" r="6.5"/><path d="m15.5 15.5 4 4"/></svg></div>
  <div class="brand">索引管理<small>__VOLUME_NAME__ · 独立刷新控制台</small></div>
  <a class="back" href="/"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="m15 18-6-6 6-6"/></svg>返回搜索</a>
</header>

<section class="card">
  <div class="topline">
    <div class="stateIcon" id="stateIcon"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M20 6 9 17l-5-5"/></svg></div>
    <div class="state"><h2 id="stateTitle">正在读取索引状态</h2><p id="stateSub">请稍候…</p></div>
    <button class="start" id="startBtn">立即刷新索引</button>
  </div>
  <div class="progressHead"><span id="scope">等待开始</span><strong class="percent" id="percent">0%</strong></div>
  <div class="bar"><div class="fill" id="fill"></div></div>
  <div class="metrics">
    <div class="metric"><label>已处理 / 预计</label><strong id="scanned">—</strong><small>项</small></div>
    <div class="metric"><label>新增 / 更新 / 删除</label><strong id="changes">0 / 0 / 0</strong></div>
    <div class="metric"><label>本次用时</label><strong id="elapsed">0.0</strong><small>秒</small></div>
    <div class="metric"><label>索引总量</label><strong id="entries">—</strong><small>项</small></div>
  </div>
</section>

<section class="card"><h3 class="sectionTitle">刷新流程</h3><div class="timeline" id="timeline">
  <div class="step" data-i="0"><div class="dot">1</div><b>准备</b><span>锁定版本</span></div>
  <div class="step" data-i="1"><div class="dot">2</div><b>读取变更</b><span>磁盘日志</span></div>
  <div class="step" data-i="2"><div class="dot">3</div><b>更新索引</b><span>本机 SSD</span></div>
  <div class="step" data-i="3"><div class="dot">4</div><b>重载缓存</b><span>64 路连接</span></div>
  <div class="step" data-i="4"><div class="dot">5</div><b>同步主库</b><span>移动硬盘</span></div>
  <div class="step" data-i="5"><div class="dot">6</div><b>完成</b><span>可跨电脑</span></div>
</div></section>

<section class="details">
  <div class="card"><h3 class="sectionTitle">详细状态</h3><div class="detailRows">
    <div class="detail"><span>当前阶段</span><b id="stage">—</b></div>
    <div class="detail"><span>索引位置</span><b id="cacheMode">—</b></div>
    <div class="detail"><span>文件 / 目录</span><b id="types">—</b></div>
    <div class="detail"><span>索引大小</span><b id="dbSize">—</b></div>
    <div class="detail"><span>上次更新</span><b id="updated">—</b></div>
    <div class="detail"><span>预计剩余</span><b id="eta">—</b></div>
  </div><p class="note">刷新期间仍可正常搜索。流程为先更新本机高速镜像，再同步移动盘主索引；即使同步中断，原主索引也不会被提前替换。</p></div>
  <div class="card"><h3 class="sectionTitle">运行记录</h3><div class="log" id="log"></div></div>
</section>
</div>
<script>
window.__LYC_TOKEN__ = "__LYC_TOKEN__";
const $=s=>document.querySelector(s), fmt=n=>Number(n||0).toLocaleString();
const clientId=(crypto.randomUUID?crypto.randomUUID():Date.now()+"-"+Math.random());
function apiHeaders(){return {"X-Lyc-Token": window.__LYC_TOKEN__||""};}
function heartbeat(){fetch("/api/heartbeat?id="+encodeURIComponent(clientId),{method:"POST",keepalive:true,headers:apiHeaders()}).catch(()=>{});}
heartbeat();setInterval(heartbeat,15000);
window.addEventListener("pagehide",e=>{if(!e.persisted)fetch("/api/client-close?id="+encodeURIComponent(clientId),{method:"POST",keepalive:true,headers:apiHeaders()}).catch(()=>{});});
let lastLogKey="", pollTimer=null;
function addLog(text,kind=""){
  const d=new Date(), line=document.createElement("div");line.className=kind;
  line.innerHTML='<span class="t">'+d.toLocaleTimeString()+"</span>　"+String(text).replace(/</g,"&lt;");
  $("#log").appendChild(line);$("#log").scrollTop=$("#log").scrollHeight;
}
function activeStep(j,p){
  if(j.refresh_phase==="sync")return 4;
  if(!j.refreshing&&j.refresh_phase==="done"&&Number(p.percent)>=100)return 5;
  const s=p.stage||"";
  if(/准备/.test(s))return 0;
  if(/读取|分析|扫描/.test(s))return 1;
  if(/写入|应用|校验|压缩|完成/.test(s))return 2;
  return j.refreshing?0:-1;
}
function renderSteps(j,p){
  const active=activeStep(j,p), finalDone=!j.refreshing&&j.refresh_phase==="done"&&Number(p.percent)>=100;
  document.querySelectorAll(".step").forEach((el,i)=>{
    el.classList.toggle("active",!finalDone&&i===active);
    el.classList.toggle("done",finalDone||i<active||(i===3&&j.warmed&&active>=4));
    const dot=el.querySelector(".dot");dot.textContent=(finalDone||el.classList.contains("done"))?"✓":String(i+1);
  });
}
function setIcon(kind){
  const box=$("#stateIcon");
  box.className="stateIcon "+kind;
  box.innerHTML=kind==="err"
    ?'<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"><circle cx="12" cy="12" r="9"/><path d="M12 7.5V13M12 16.5h.01"/></svg>'
    :kind==="run"
    ?'<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21 12a9 9 0 1 1-2.6-6.3"/><path d="M21 3v6h-6"/></svg>'
    :'<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M20 6 9 17l-5-5"/></svg>';
}
function render(j){
  const p=j.refresh_progress||{}, running=!!j.refreshing;
  const overall=Math.max(0,Math.min(100,Number(p.overall_percent??p.percent??0)));
  $("#fill").style.width=overall+"%";$("#fill").classList.toggle("running",running);
  $("#percent").textContent=Math.round(overall)+"%";
  $("#scope").textContent=(p.scope||"索引就绪")+(p.stage?" · "+p.stage:"");
  $("#stage").textContent=p.stage||"等待刷新";
  $("#scanned").textContent=p.estimated?fmt(p.scanned)+" / "+fmt(p.estimated):"—";
  $("#changes").textContent=fmt(p.added)+" / "+fmt(p.changed)+" / "+fmt(p.deleted);
  $("#elapsed").textContent=Number(p.elapsed||0).toFixed(1);
  $("#entries").textContent=fmt(j.entries);
  $("#types").textContent=fmt(j.files)+" / "+fmt(j.dirs);
  $("#dbSize").textContent=(j.db_mb||0)+" MB";
  $("#updated").textContent=j.updated||"—";
  $("#cacheMode").textContent=j.fast_cache?"本机 SSD 高速镜像 + 移动盘主库":"当前索引库";
  const pct=Number(p.percent||0), elapsed=Number(p.elapsed||0);
  $("#eta").textContent=(running&&pct>1&&pct<100)?("约 "+Math.max(0,Math.round(elapsed*(100-pct)/pct))+" 秒"):running?"计算中":"—";
  $("#startBtn").disabled=running;$("#startBtn").textContent=running?"刷新进行中…":"立即刷新索引";
  if(j.refresh_error){setIcon("err");$("#stateTitle").textContent="刷新出现问题";
    $("#stateTitle").classList.add("error");$("#stateSub").textContent=j.refresh_error;
  }else if(running){setIcon("run");$("#stateTitle").textContent=p.stage||"正在刷新索引";
    $("#stateTitle").classList.remove("error");$("#stateSub").textContent=(p.scope||"索引")+"正在处理，搜索功能仍然可用";
  }else{setIcon("ok");$("#stateTitle").textContent=j.refresh_phase==="done"?"索引刷新完成":"索引已就绪";
    $("#stateTitle").classList.remove("error");$("#stateSub").textContent="共 "+fmt(j.entries)+" 项，上次更新 "+j.updated;}
  renderSteps(j,p);
  const key=[j.refresh_phase,p.scope,p.stage,p.percent,p.scanned,p.added,p.changed,p.deleted,j.refresh_error].join("|");
  if(key!==lastLogKey){lastLogKey=key;
    const tail=(p.percent??0)+"% · 已处理 "+fmt(p.scanned)+" · +"+fmt(p.added)+" ~"+fmt(p.changed)+" -"+fmt(p.deleted);
    addLog(j.refresh_error?j.refresh_error:((p.scope||"索引")+" / "+(p.stage||"就绪")+" / "+tail),j.refresh_error?"err":(!running&&p.percent===100?"ok":""));}
}
async function load(){clearTimeout(pollTimer);try{const j=await(await fetch("/api/stats",{cache:"no-store",headers:apiHeaders()})).json();
  if(!j.ok)throw new Error(j.msg||"索引不可用");render(j);pollTimer=setTimeout(load,j.refreshing?250:1500);
  }catch(e){addLog("状态读取失败："+e,"err");pollTimer=setTimeout(load,2000);}}
$("#startBtn").addEventListener("click",async()=>{$("#startBtn").disabled=true;addLog("已提交刷新请求");
  try{const j=await(await fetch("/api/refresh",{method:"POST",headers:apiHeaders()})).json();addLog(j.msg,j.ok?"ok":"err");await load();}
  catch(e){addLog("刷新启动失败："+e,"err");$("#startBtn").disabled=false;}});
addLog("索引管理页面已连接");load();
</script></body></html>"""
REFRESH_HTML = REFRESH_HTML.replace(
    "__VOLUME_NAME__", html.escape(VOLUME_NAME, quote=True))
REFRESH_HTML = REFRESH_HTML.replace(
    '"__LYC_TOKEN__"', json.dumps(TOKEN))


# ---------------------------------------------------------------- 入口

def main():
    prepare_fast_index()
    port = get_free_port()
    # 检查整个候选端口段，并用应用标识避免误认其他本地服务。
    import urllib.request
    for candidate in range(BASE_PORT, BASE_PORT + 20):
        try:
            with urllib.request.urlopen(
                    f"http://127.0.0.1:{candidate}/api/stats", timeout=1.5) as r:
                info = json.loads(r.read().decode("utf-8"))
                if (info.get("app") == "lyc-filesearch" and
                        info.get("version") == APP_VERSION and
                        os.path.realpath(info.get("volume", "")) ==
                        os.path.realpath(VOLUME)):
                    if (not os.environ.get("LYCSEARCH_NO_BROWSER") and
                            not os.environ.get("LYCSEARCH_LOADING_PAGE")):
                        webbrowser.open(f"http://127.0.0.1:{candidate}")
                    return
        except Exception:
            continue

    if not os.path.exists(DB_PATH):
        print("首次运行, 正在建立索引(约几分钟)…")
        lycsearch.rebuild_index_atomic(DB_PATH, quiet=False)
    else:
        if lycsearch.bind_index_to_volume(DB_PATH, VOLUME):
            print(f"已将索引路径迁移到新硬盘位置: {VOLUME}")
    start_master_sync()

    # 资源换速度：后台把 1.6GB 索引整体装入系统页缓存，并预建 64 路连接。
    start_warm_up()

    # 并发服务：512 请求监听队列，慢请求不阻塞状态、退出与其他搜索。
    srv = HighConcurrencyHTTPServer(("127.0.0.1", port), Handler)
    url = f"http://127.0.0.1:{port}"
    print(f"LYCSEARCH_READY {url}", flush=True)
    print(f"{VOLUME_NAME} 文件搜索器已启动: {url}")
    print("在浏览器中搜索文件；关闭搜索页面即停止服务。")
    threading.Thread(target=client_watchdog, daemon=True,
                     name="browser-watchdog").start()
    if (not os.environ.get("LYCSEARCH_NO_BROWSER") and
            not os.environ.get("LYCSEARCH_LOADING_PAGE")):
        def open_ready_ui():
            _WARMED.wait(timeout=120)
            webbrowser.open(url)
        threading.Thread(target=open_ready_ui, daemon=True).start()
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        srv.server_close()


if __name__ == "__main__":
    main()
