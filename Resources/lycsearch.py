#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
可移植超快速模糊文件搜索器
========================
原理: 先把整盘文件路径建成 SQLite 索引(含 FTS5 trigram 子串索引),
      之后所有搜索都在内存/磁盘索引上完成 —— 毫秒级返回,
      不需要再遍历磁盘。

用法:
  python3 lycsearch.py build                 # 全量扫描, 建立索引(首次使用)
  python3 lycsearch.py refresh               # 增量更新索引(新增/删除/改名后)
  python3 lycsearch.py <关键词...> [选项]     # 模糊搜索(支持多个词, AND 关系)
  python3 lycsearch.py                       # 交互模式(连续模糊搜索, 回车退出)

搜索示例:
  python3 lycsearch.py 三亚
  python3 lycsearch.py 周杰伦 演唱会
  python3 lycsearch.py 照片 2024 --dir
  python3 lycsearch.py *.mp4 --file          # 通配符
  python3 lycsearch.py 毕业 --type 文档 --limit 50

选项:
  -f, --file        只搜文件
  -d, --dir         只搜目录
  --full            同时匹配完整路径(默认只匹配文件名/父目录名, 更快)
  -t, --type 类别   按类型过滤: 图片|视频|音频|文档|压缩|代码
  --limit N         最多显示 N 条 (默认 30, 0 = 全部)
  -c, --count       只输出命中数量
  --case            区分大小写(默认不区分)
  --sort 字段       排序: name | size | time | path (默认按匹配度)
  --db 路径         指定索引文件位置 (默认为程序所在硬盘的 .lycsearch/index.db)
  -h, --help        显示帮助
"""

import os
import re
import json
import sys
import time
import hashlib
import argparse
import threading
import concurrent.futures
import subprocess
import sqlite3
import heapq
import unicodedata

def discover_volume(script_path=None):
    """自动识别程序所在的硬盘/挂载点，也可用环境变量覆盖。"""
    override = os.environ.get("LYCSEARCH_VOLUME")
    if override:
        return os.path.abspath(os.path.expanduser(override))
    # 本机 App 默认建立“全局”索引：当前用户目录 + 所有已挂载外置卷。
    # 环境变量仍可用于精确指定单个目录（测试/高级用法）。
    if sys.platform == "darwin":
        return os.sep
    path = os.path.realpath(script_path or __file__)
    if sys.platform == "darwin":
        prefix = os.path.join(os.sep, "Volumes") + os.sep
        if path.startswith(prefix):
            rest = path[len(prefix):].split(os.sep, 1)[0]
            if rest:
                return os.path.join(os.sep, "Volumes", rest)
    if os.name == "nt":
        drive = os.path.splitdrive(path)[0]
        if drive:
            return drive + os.sep
    current = os.path.dirname(path)
    while current and current != os.path.dirname(current):
        if os.path.ismount(current):
            return current
        current = os.path.dirname(current)
    # 程序在系统盘时默认搜索当前用户，避免扫描整个系统根目录。
    return os.path.expanduser("~")


def default_db_for(volume):
    override = os.environ.get("LYCSEARCH_DB")
    if override:
        return os.path.abspath(os.path.expanduser(override))
    if sys.platform == "darwin" and os.path.realpath(volume) == os.sep:
        db_name = "index-global.db"
    elif sys.platform == "darwin":
        db_name = "index.db"  # 兼容现有 macOS 索引
    elif os.name == "nt":
        db_name = "index-windows.db"
    else:
        db_name = "index-linux.db"
    # macOS 独立版的索引与高速缓存全部放在 App 包内，
    # 移动 App 时索引会随程序一起迁移，不再写入用户库目录。
    if sys.platform == "darwin":
        bundle_root = os.path.dirname(os.path.dirname(
            os.path.dirname(os.path.abspath(__file__))))
        data_root = os.path.join(
            bundle_root, "Contents", "SharedSupport", "IndexCache.nosync")
        key = hashlib.sha256(
            os.path.realpath(volume).encode("utf-8")).hexdigest()[:16]
        return os.path.join(data_root, key, db_name)

    preferred_dir = os.path.join(volume, ".lycsearch")
    if ((os.path.isdir(preferred_dir) and os.access(preferred_dir, os.W_OK)) or
            (not os.path.exists(preferred_dir) and os.access(volume, os.W_OK))):
        return os.path.join(preferred_dir, db_name)
    if os.name == "nt":
        data_root = os.path.join(
            os.environ.get("LOCALAPPDATA", os.path.expanduser("~")),
            "FileSearcher", "indexes")
    else:
        data_root = os.path.join(
            os.environ.get("XDG_DATA_HOME", os.path.expanduser("~/.local/share")),
            "file-searcher", "indexes")
    key = hashlib.sha256(os.path.realpath(volume).encode("utf-8")).hexdigest()[:16]
    return os.path.join(data_root, key, db_name)


def _root_from_indexed_path(path):
    """从旧索引的绝对路径推断原硬盘根目录。"""
    if not path:
        return None
    match = re.match(r"^(/Volumes/[^/]+)(?:/|$)", path)
    if match:
        return match.group(1)
    drive = os.path.splitdrive(path)[0]
    if drive:
        return drive + os.sep
    return None


def bind_index_to_volume(db_path, volume):
    """硬盘改名或换电脑后，将已有索引的路径前缀安全迁移到新挂载点。"""
    if not os.path.exists(db_path):
        return False
    current = os.path.abspath(volume).rstrip(os.sep) or os.sep
    conn = _connect(db_path)
    create_schema(conn)
    row = conn.execute(
        "SELECT value FROM meta WHERE key='volume_root'").fetchone()
    old = row[0] if row else None
    if not old:
        sample = conn.execute("SELECT path FROM files LIMIT 1").fetchone()
        old = _root_from_indexed_path(sample[0] if sample else None)
    migrated = bool(old and os.path.normcase(old) != os.path.normcase(current))
    if migrated:
        conn.execute("BEGIN")
        try:
            prefix = old.rstrip(os.sep) + os.sep
            conn.execute(
                "UPDATE files SET path=? || substr(path, ?), "
                "parent=? || substr(parent, ?) "
                "WHERE path=? OR (path>=? AND path<?)",
                (current, len(old) + 1, current, len(old) + 1,
                 old, prefix, prefix + "\U0010ffff"))
            # FSEvents 游标与原硬盘绑定，换盘后必须重建基线。
            conn.execute(
                "DELETE FROM meta WHERE key IN ('fsevent_id','fsevent_roots')")
            conn.commit()
        except Exception:
            conn.rollback()
            conn.close()
            raise
    conn.execute(
        "INSERT OR REPLACE INTO meta(key,value) VALUES('volume_root',?)",
        (current,))
    if migrated:
        conn.execute(
            "INSERT OR REPLACE INTO meta(key,value) VALUES('revision',?)",
            (str(time.time_ns()),))
    conn.commit()
    conn.close()
    return migrated


VOLUME = discover_volume()
DEFAULT_DB = default_db_for(VOLUME)

# ExFAT 卷上的系统目录, 索引时跳过(无搜索价值, 还能大幅提速)
SKIP_DIRS = {
    ".Spotlight-V100", ".fseventsd", ".Trashes", ".TemporaryItems",
    "$RECYCLE.BIN", "System Volume Information", ".lycsearch", ".Trash-1000",
}
# 跳过的文件名前缀/固定名 (AppleDouble / Finder 垃圾)
SKIP_NAME_PREFIXES = ("._", ".trash_purge_tmp_")
SKIP_NAMES = {".DS_Store", ".localized", ".fseventsd"}
# macOS 会把这些目录视为受 TCC 保护的系统媒体资料库。
# 并发扫描它们会反复弹出 Apple Music / 照片授权；跳过资料库包，
# 但不影响 Music 和 Pictures 中的其他普通文件。
PROTECTED_MEDIA_PATHS = {
    # 仅排除确需跳过的资料库包与系统库目录：
    # - ~/Library 内含照片/联系人/日历/邮件及各 App 容器，递归会触发大量 TCC 弹窗；
    # - Apple Music / iTunes / Photos 资料库包为特殊媒体库，同样跳过。
    # 桌面、文稿、下载、影片、音乐、图片等常用顶层目录不再跳过，
    # 纳入普通索引；未获授权访问时会被标记为“未覆盖”。
    # ~/.Trash 为系统废纸篓（TCC 保护且无搜索价值），主动排除。
    os.path.realpath(os.path.expanduser("~/Library")),
    os.path.realpath(os.path.expanduser("~/.Trash")),
    os.path.realpath(os.path.expanduser("~/Music/Music")),
    os.path.realpath(os.path.expanduser("~/Music/iTunes")),
    os.path.realpath(os.path.expanduser("~/Pictures/Photos Library.photoslibrary")),
}


def _is_protected_path(path):
    """不触碰 macOS TCC 保护目录及其子项。"""
    candidate = os.path.abspath(os.path.expanduser(path)).rstrip(os.sep)
    return any(candidate == root or candidate.startswith(root + os.sep)
               for root in PROTECTED_MEDIA_PATHS)

# 因 TCC/权限被拒而未能扫描的目录；扫描结束时写入 meta["uncovered"]，
# 供 stats 展示“未覆盖”并在授权后强制重新纳入。
UNCOVERED = set()
_uncovered_lock = threading.Lock()


def _mark_uncovered(path):
    real = os.path.realpath(path)
    with _uncovered_lock:
        UNCOVERED.add(real)


def _uncovered_snapshot():
    with _uncovered_lock:
        return sorted(UNCOVERED)


def _persist_uncovered(conn):
    """把未覆盖目录快照写入索引 meta，供 stats 与后续刷新读取。"""
    conn.execute(
        "INSERT OR REPLACE INTO meta(key,value) VALUES('uncovered',?)",
        (json.dumps(_uncovered_snapshot(), ensure_ascii=False),))
    conn.commit()


def _user_dirs_indexed(conn):
    """常用目录是否已纳入索引（未纳入则强制一次全盘增量）。"""
    try:
        row = conn.execute(
            "SELECT value FROM meta WHERE key='user_dirs_indexed'").fetchone()
        return bool(row and row[0] == "1")
    except sqlite3.Error:
        return False


def _has_uncovered(conn):
    """索引中是否仍有未覆盖目录；存在则下次刷新强制全盘尝试。"""
    try:
        row = conn.execute(
            "SELECT value FROM meta WHERE key='uncovered'").fetchone()
        return bool(row and json.loads(row[0]))
    except (sqlite3.Error, ValueError):
        return False


def _force_full_incremental(conn):
    """尚未完成常用目录覆盖或存在未覆盖目录时，跳过 FSEvents 快速路径，
    强制一次全盘增量扫描，确保授权后能重新纳入这些目录。"""
    return (not _user_dirs_indexed(conn)) or _has_uncovered(conn)


# 因权限拒绝/磁盘拔出/瞬时 I/O 错误而无法完整枚举的目录子树。
# 增量刷新时，这些子树下的旧索引记录一律禁止删除，防止把
# “目录临时不可读”误判为“大量文件被删除”。
FAILED_SUBTREES = set()
_failed_subtrees_lock = threading.Lock()


def _mark_failed_subtree(path):
    real = os.path.realpath(path)
    with _failed_subtrees_lock:
        FAILED_SUBTREES.add(real)


def _failed_subtree_snapshot():
    with _failed_subtrees_lock:
        return set(FAILED_SUBTREES)


def _under_failed_subtree(path, failed=None):
    """判断 path 是否位于任一失败子树之下（或就是其根）。"""
    if failed is None:
        failed = _failed_subtree_snapshot()
    if not failed:
        return False
    p = os.path.realpath(path).rstrip(os.sep)
    for root in failed:
        root = root.rstrip(os.sep)
        if p == root or p.startswith(root + os.sep):
            return True
    return False
SCAN_WORKERS = 64
_RESOURCE_DIR = os.path.dirname(os.path.abspath(__file__))
_BUNDLE_CANDIDATE = os.path.dirname(os.path.dirname(_RESOURCE_DIR))
SELF_BUNDLE_ROOT = (_BUNDLE_CANDIDATE
                    if _BUNDLE_CANDIDATE.endswith(".app") else "")

TYPE_EXTS = {
    "图片":  {"jpg", "jpeg", "png", "gif", "webp", "heic", "heif", "bmp", "tiff",
              "tif", "raw", "cr2", "nef", "arw", "svg", "ico", "psd", "ai"},
    "视频":  {"mp4", "mov", "mkv", "avi", "wmv", "flv", "m4v", "webm", "mpg",
              "mpeg", "ts", "rmvb", "3gp", "m2ts", "mts"},
    "音频":  {"mp3", "wav", "flac", "aac", "m4a", "ogg", "wma", "ape", "aiff", "opus"},
    "文档":  {"pdf", "doc", "docx", "xls", "xlsx", "ppt", "pptx", "txt", "md",
              "csv", "rtf", "pages", "numbers", "key", "odt", "ods", "odp",
              "epub", "mobi", "org"},
    "压缩":  {"zip", "rar", "7z", "tar", "gz", "bz2", "xz", "dmg", "iso", "tgz", "zst"},
    "代码":  {"py", "js", "ts", "java", "c", "cpp", "h", "hpp", "html", "css",
              "json", "xml", "yaml", "yml", "sh", "zsh", "bash", "swift", "go",
              "rs", "rb", "php", "sql", "vue", "jsx", "tsx", "kt", "lua"},
}


def normalize_search_key(value):
    """生成跨大小写、全半角、空格和常见标点差异的稳定检索键。"""
    value = unicodedata.normalize("NFKC", str(value or "")).casefold()
    value = re.sub(r"[‐‑‒–—―−]", "-", value)
    value = re.sub(r"\s+", " ", value).strip()
    return value


def type_extensions(type_filter):
    """把单个或多个文件类型统一展开为扩展名集合。"""
    if not type_filter:
        return set()
    names = [type_filter] if isinstance(type_filter, str) else type_filter
    return set().union(*(TYPE_EXTS.get(name, set()) for name in names))

FTS_OK = False  # 运行期检测 trigram 支持


def _detect_fts(conn):
    """检测现有索引是否包含 FTS5 表。

    GUI 进程只打开已有数据库，不会调用 create_schema()；
    因此不能依赖进程内的 FTS_OK 初始值，否则 GUI 会退化成全表 LIKE。
    """
    global FTS_OK
    try:
        table_ok = conn.execute(
            "SELECT 1 FROM sqlite_master "
            "WHERE type='table' AND name='files_fts'"
        ).fetchone() is not None
        path_ok = conn.execute(
            "SELECT 1 FROM sqlite_master "
            "WHERE type='table' AND name='path_fts'"
        ).fetchone() is not None
        ready = conn.execute(
            "SELECT value FROM meta WHERE key='fts_ready'").fetchone()
        FTS_OK = bool(table_ok and path_ok and ready is not None
                      and ready[0] == "1")
    except sqlite3.Error:
        FTS_OK = False
    return FTS_OK


def _glob_to_like(value):
    """将 shell 风格的 * / ? 安全转换为 SQLite LIKE 模式。"""
    out = []
    for ch in value:
        if ch == "*":
            out.append("%")
        elif ch == "?":
            out.append("_")
        elif ch in ("\\", "%", "_"):
            out.append("\\" + ch)
        else:
            out.append(ch)
    return "".join(out)


def _is_cjk(ch):
    code = ord(ch)
    return (0x3400 <= code <= 0x4DBF or 0x4E00 <= code <= 0x9FFF or
            0xF900 <= code <= 0xFAFF)


def _cjk_short_grams(name):
    """生成中文单字/双字索引键，解决 FTS5 trigram 无法搜索
    少于 3 个字符的限制。"""
    grams = set()
    previous = None
    for ch in name:
        if _is_cjk(ch):
            grams.add(ch)
            if previous is not None:
                grams.add(previous + ch)
            previous = ch
        else:
            previous = None
    return grams


def _short_index_ready(conn):
    try:
        row = conn.execute(
            "SELECT value FROM meta WHERE key='short_grams_ready'").fetchone()
        return bool(row and row[0] == "1")
    except sqlite3.Error:
        return False


# ---------------------------------------------------------------- 索引层

def human_size(n):
    if n is None:
        return "-"
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024 or unit == "TB":
            return f"{n:.1f}{unit}" if unit != "B" else f"{int(n)}B"
        n /= 1024
    return f"{n:.1f}TB"


def _connect(db_path):
    conn = sqlite3.connect(db_path)
    conn.isolation_level = None  # 显式事务管理, 由 flush() 自行 BEGIN/COMMIT
    rebuild = os.environ.get("LYCSEARCH_REBUILD") == "1"
    # WAL: 写读并发安全, 崩溃不损坏; 支持返回 wal 即生效
    if rebuild:
        conn.execute("PRAGMA journal_mode=OFF")
        conn.execute("PRAGMA locking_mode=EXCLUSIVE")
    else:
        try:
            conn.execute("PRAGMA journal_mode=WAL")
        except sqlite3.OperationalError:
            conn.execute("PRAGMA journal_mode=OFF")
    turbo = os.environ.get("LYCSEARCH_TURBO") == "1"
    conn.execute("PRAGMA synchronous=" + ("OFF" if turbo else "NORMAL"))
    conn.execute("PRAGMA temp_store=MEMORY")
    conn.execute(f"PRAGMA cache_size={-262144 if turbo else -20000}")
    conn.execute(f"PRAGMA wal_autocheckpoint={0 if turbo else 2000}")
    if turbo:
        conn.execute("PRAGMA mmap_size=8589934592")
    return conn


def _connect_ro(db_path, cross_thread=False):
    """高并发只读连接：大内存映射共享系统页缓存，避免重复磁盘 I/O。"""
    conn = sqlite3.connect(
        db_path, timeout=10, check_same_thread=not cross_thread)
    conn.execute("PRAGMA busy_timeout=3000")
    conn.execute("PRAGMA query_only=ON")
    conn.execute("PRAGMA temp_store=MEMORY")
    # 每个连接 16 MiB 私有页缓存(8 路池共 128 MiB); 8 GiB mmap 的物理页由所有连接共享。
    conn.execute("PRAGMA cache_size=-16384")
    try:
        conn.execute("PRAGMA mmap_size=8589934592")
    except sqlite3.Error:
        pass
    return conn


def create_schema(conn):
    conn.execute(
        "CREATE TABLE IF NOT EXISTS files("
        "rowid INTEGER PRIMARY KEY,"
        "path TEXT UNIQUE NOT NULL,"
        "name TEXT NOT NULL,"
        "parent TEXT NOT NULL,"
        "is_dir INTEGER NOT NULL,"
        "size INTEGER,"
        "mtime REAL)"    )
    # meta: 统计信息(构建后写入, stats 毫秒读取, 免全表 COUNT)
    conn.execute(
        "CREATE TABLE IF NOT EXISTS meta("
        "key TEXT PRIMARY KEY, value TEXT NOT NULL)"
    )
    # fname: 紧凑列名表(rowid 与 files 对齐), 短词 LIKE 只扫它, 避免全库扫描
    # ext: 扩展名(小写), 类型过滤走它可保持 fname 驱动; is_dir: 范围过滤
    conn.execute(
        "CREATE TABLE IF NOT EXISTS fname("
        "rowid INTEGER PRIMARY KEY, name TEXT NOT NULL, ext TEXT NOT NULL DEFAULT '',"
        "is_dir INTEGER NOT NULL DEFAULT 0)"
    )
    # fpbase: 父目录名表, "含路径"搜索匹配 name + 一级父目录名(9MB, 快)
    conn.execute(
        "CREATE TABLE IF NOT EXISTS fpbase("
        "rowid INTEGER PRIMARY KEY, pbase TEXT NOT NULL, ext TEXT NOT NULL DEFAULT '',"
        "is_dir INTEGER NOT NULL DEFAULT 0)"
    )
    conn.execute(
        "CREATE TABLE IF NOT EXISTS short_grams("
        "gram TEXT NOT NULL, file_rowid INTEGER NOT NULL,"
        "PRIMARY KEY(gram, file_rowid)) WITHOUT ROWID"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_short_grams_rowid ON short_grams(file_rowid)"
    )
    # 旧结构(无 is_dir) -> 重建
    for tbl in ("fname", "fpbase"):
        cols = [r[1] for r in conn.execute(f"PRAGMA table_info({tbl})").fetchall()]
        if "is_dir" not in cols:
            conn.execute(f"DROP TABLE {tbl}")
    conn.execute(
        "CREATE TABLE IF NOT EXISTS fname("
        "rowid INTEGER PRIMARY KEY, name TEXT NOT NULL, ext TEXT NOT NULL DEFAULT '',"
        "is_dir INTEGER NOT NULL DEFAULT 0)"
    )
    conn.execute(
        "CREATE TABLE IF NOT EXISTS fpbase("
        "rowid INTEGER PRIMARY KEY, pbase TEXT NOT NULL, ext TEXT NOT NULL DEFAULT '',"
        "is_dir INTEGER NOT NULL DEFAULT 0)"
    )
    # ext 索引: 通配符 *.mp4 之类直接走等值索引, 毫秒级
    conn.execute("CREATE INDEX IF NOT EXISTS idx_fname_ext ON fname(ext)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_fpbase_ext ON fpbase(ext)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_parent ON files(parent)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_dir ON files(is_dir)")
    # FTS5 trigram: 专为任意子串搜索设计(中文/英文均可), 毫秒级
    # 只索引 name(文件名), 保证搜索聚焦且极快; 父路径匹配由 --full 另行处理
    global FTS_OK
    try:
        conn.execute("CREATE VIRTUAL TABLE IF NOT EXISTS files_fts "
                     "USING fts5(name, tokenize='trigram')")
        cols = [r[1] for r in conn.execute("PRAGMA table_info(files_fts)").fetchall()]
        if cols != ["name"]:
            # 旧版(含 parent 列)结构 -> 重建
            conn.execute("DROP TABLE files_fts")
            conn.execute("CREATE VIRTUAL TABLE files_fts "
                         "USING fts5(name, tokenize='trigram')")
        # path_fts: 完整路径 trigram, 供 --full 覆盖任意层级路径命中。
        conn.execute("CREATE VIRTUAL TABLE IF NOT EXISTS path_fts "
                     "USING fts5(path, tokenize='trigram')")
        pcols = [r[1] for r in conn.execute("PRAGMA table_info(path_fts)").fetchall()]
        if pcols != ["path"]:
            conn.execute("DROP TABLE path_fts")
            conn.execute("CREATE VIRTUAL TABLE path_fts "
                         "USING fts5(path, tokenize='trigram')")
        FTS_OK = True
    except sqlite3.OperationalError:
        FTS_OK = False
    conn.commit()


def drop_query_indexes(conn):
    for name in ("idx_fname_ext", "idx_fpbase_ext", "idx_parent", "idx_dir"):
        conn.execute(f"DROP INDEX IF EXISTS {name}")
    conn.commit()


def create_query_indexes(conn):
    conn.execute("CREATE INDEX IF NOT EXISTS idx_fname_ext ON fname(ext)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_fpbase_ext ON fpbase(ext)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_parent ON files(parent)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_dir ON files(is_dir)")
    conn.commit()


def maintain_fts(conn, optimize=False):
    """合并 FTS segment；全量构建后压到最快查询形态。"""
    if not FTS_OK:
        return
    try:
        if optimize:
            conn.execute("INSERT INTO files_fts(files_fts) VALUES('optimize')")
            conn.execute("INSERT INTO path_fts(path_fts) VALUES('optimize')")
        else:
            conn.execute(
                "INSERT INTO files_fts(files_fts,rank) VALUES('merge',1000)")
            conn.execute(
                "INSERT INTO path_fts(path_fts,rank) VALUES('merge',1000)")
        conn.commit()
    except sqlite3.Error:
        pass


def _scan_directory(d, include_metadata, known_paths, known_dirs):
    """扫描单个目录。独立函数便于增量刷新并发隐藏 ExFAT 的 I/O 延迟。"""
    entries = []
    children = []
    if _is_protected_path(d):
        return entries, children
    try:
        with os.scandir(d) as it:
            for e in it:
                if e.name in SKIP_NAMES or e.name.startswith(SKIP_NAME_PREFIXES):
                    continue
                p = e.path
                try:
                    if SELF_BUNDLE_ROOT and (p == SELF_BUNDLE_ROOT or
                            p.startswith(SELF_BUNDLE_ROOT + os.sep)):
                        continue
                    if known_paths is not None and p in known_paths:
                        is_dir = p in known_dirs
                    else:
                        is_dir = e.is_dir(follow_symlinks=False)
                    if is_dir:
                        if e.name in SKIP_DIRS or _is_protected_path(p):
                            continue
                        entries.append((p, e.name, d, 1, None, None))
                        children.append(p)
                    elif include_metadata:
                        try:
                            st = e.stat(follow_symlinks=False)
                            entries.append(
                                (p, e.name, d, 0, st.st_size, st.st_mtime))
                        except OSError:
                            entries.append((p, e.name, d, 0, None, None))
                    else:
                        entries.append((p, e.name, d, 0, None, None))
                except OSError:
                    continue
    except PermissionError:
        _mark_uncovered(d)
        _mark_failed_subtree(d)
    except OSError:
        _mark_failed_subtree(d)
    return entries, children


def _scan_roots(volume):
    roots = [volume]
    if sys.platform == "darwin" and os.path.realpath(volume) == os.sep:
        roots = [os.path.expanduser("~")]
        try:
            roots.extend(
                os.path.join("/Volumes", name)
                for name in sorted(os.listdir("/Volumes"))
                if not name.startswith(".") and
                os.path.isdir(os.path.join("/Volumes", name)) and
                os.path.realpath(os.path.join("/Volumes", name)) != os.sep)
        except OSError:
            pass
    return roots


def benchmark_scan_workers(volume):
    """在当前卷抽样 4/8/16/32/64 路目录吞吐，选择最快并发度。"""
    override = os.environ.get("LYCSEARCH_SCAN_WORKERS")
    if override:
        try:
            return max(1, min(128, int(override)))
        except ValueError:
            pass
    samples = []
    queue = list(_scan_roots(volume))
    while queue and len(samples) < 128:
        directory = queue.pop(0)
        samples.append(directory)
        try:
            with os.scandir(directory) as entries:
                for entry in entries:
                    if len(queue) + len(samples) >= 256:
                        break
                    if (entry.name not in SKIP_DIRS and
                            not _is_protected_path(entry.path) and
                            entry.is_dir(follow_symlinks=False)):
                        queue.append(entry.path)
        except OSError:
            pass
    if len(samples) < 8:
        return min(8, SCAN_WORKERS)
    # 先预热目录页，避免第一个候选独自承担冷缓存成本。
    for directory in samples[:16]:
        _scan_directory(directory, False, None, None)
    best_workers, best_rate = 4, 0.0
    for workers in (4, 8, 16, 32, 64):
        started = time.perf_counter()
        entries = 0
        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
            for rows, _ in pool.map(
                    lambda d: _scan_directory(d, False, None, None), samples):
                entries += len(rows)
        elapsed = max(.0001, time.perf_counter() - started)
        rate = entries / elapsed
        if rate > best_rate:
            best_workers, best_rate = workers, rate
    return best_workers


def _walk(volume, include_metadata=True, known_paths=None, known_dirs=None,
          workers=1):
    """生成 (path, name, parent, is_dir, size, mtime), 跳过系统目录。

    快速增量刷新只需判断路径的新增/删除/改名，无需对几十万个
    已有文件逐一 stat；新文件在比对后再单独读取大小和时间。
    """
    roots = _scan_roots(volume)
    if workers <= 1:
        stack = list(roots)
        while stack:
            entries, children = _scan_directory(
                stack.pop(), include_metadata, known_paths, known_dirs)
            stack.extend(children)
            yield from entries
        return

    # ExFAT 打开每个目录的延迟很高。有界线程池只并发目录枚举，
    # 数据库写入仍在主线程串行，兼顾速度与索引安全。
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
        pending = {
            pool.submit(_scan_directory, root, include_metadata,
                        known_paths, known_dirs) for root in roots
        }
        while pending:
            done, pending = concurrent.futures.wait(
                pending, return_when=concurrent.futures.FIRST_COMPLETED)
            for future in done:
                entries, children = future.result()
                for child in children:
                    pending.add(pool.submit(
                        _scan_directory, child, include_metadata,
                        known_paths, known_dirs))
                yield from entries


def _lock_file(db_path):
    return os.path.join(os.path.dirname(db_path), ".building.lock")


def _progress_file(db_path):
    return os.path.join(os.path.dirname(db_path), "refresh-progress.json")


def _fsevents_helper():
    return os.path.join(os.path.dirname(os.path.abspath(__file__)),
                        "fsevents_helper")


def _device_mount_root(path):
    """返回路径所在设备的挂载根；FSEvents 相对路径以设备根为基准。"""
    current = os.path.realpath(path)
    try:
        device = os.stat(current).st_dev
        while True:
            parent = os.path.dirname(current)
            if parent == current or os.stat(parent).st_dev != device:
                return current
            current = parent
    except OSError:
        return os.path.realpath(path)


def _current_event_id(start=0, volume=None):
    """读取外接卷当前的 FSEvents 序号；不支持时返回 0。

    start 可传最近已知位点：FSEvents 会从该位点开始回放，比从最早
    历史开始(可达数十万事件)快一个数量级。
    """
    event_volume = volume or VOLUME
    helper = _fsevents_helper()
    if not (os.path.isfile(helper) and os.access(helper, os.X_OK)):
        return 0
    try:
        args = [helper, "current", event_volume]
        if start:
            args.append(str(max(1, int(start))))
        result = subprocess.run(
            args, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
            timeout=90, check=True)
        for chunk in result.stdout.split(b"\0"):
            if chunk.strip():
                return int(chunk.strip())
        return 0
    except (OSError, ValueError, subprocess.SubprocessError):
        return 0


def _changed_paths_since(since, volume=None):
    """从 macOS 原生卷日志取变化路径，日志不完整时返回 None。"""
    helper = _fsevents_helper()
    event_volume = volume or VOLUME
    if not (since and os.path.isfile(helper) and os.access(helper, os.X_OK)):
        return None
    try:
        result = subprocess.run(
            [helper, "changes", event_volume, str(since)],
            stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
            timeout=90, check=True)
    except (OSError, subprocess.SubprocessError):
        return None
    latest = since
    events = {}
    stream_complete = False
    # EventIdsWrapped/RootChanged/Mount/Unmount 强制全盘扫描。
    # MustScanSubDirs/UserDropped/KernelDropped(0x01|0x02|0x04)表示系统/内核
    # 曾丢弃部分事件，同批中未报告的变化可能被遗漏。这些标志一旦出现即
    # 判定本次日志不完整(见 kFSEventStreamEventFlagUserDropped)，触发重扫
    # 受影响范围或全盘；只有重扫成功后调用方才推进游标。
    unsafe_flags = 0x08 | 0x20 | 0x40 | 0x80
    for chunk in result.stdout.split(b"\0"):
        if not chunk:
            continue
        if chunk.startswith(b"LATEST\t"):
            try:
                latest = max(latest, int(chunk.split(b"\t", 1)[1]))
            except ValueError:
                pass
            continue
        if chunk.startswith(b"STATUS\t"):
            # helper 空闲收敛(读到事件流尾部)才算完整；回放超时则不可信。
            stream_complete = chunk.split(b"\t", 1)[1].strip() == b"DONE"
            continue
        try:
            event_id_s, flags_s, relative_b = chunk.split(b"\t", 2)
            event_id, flags = int(event_id_s), int(flags_s)
        except ValueError:
            continue
        relative = relative_b.decode("utf-8", "replace")
        latest = max(latest, event_id)
        if event_id <= since or flags & 0x10:  # HistoryDone
            continue
        if flags & (0x01 | 0x02 | 0x04):
            # 丢失事件：本批可能漏报变化，判定为不完整，触发重扫。
            return None
        if flags & unsafe_flags or not relative:
            return None
        relative = relative.lstrip("/")
        parts = relative.split("/")
        if (not relative or parts[0] in SKIP_DIRS or
                any(part.startswith(SKIP_NAME_PREFIXES) for part in parts) or
                any(part in SKIP_NAMES for part in parts)):
            continue
        path = os.path.join(_device_mount_root(event_volume), relative)
        if _is_protected_path(path):
            continue
        scope = os.path.realpath(event_volume).rstrip(os.sep) or os.sep
        if path != scope and not path.startswith(scope.rstrip(os.sep) + os.sep):
            continue
        if SELF_BUNDLE_ROOT and (path == SELF_BUNDLE_ROOT or
                path.startswith(SELF_BUNDLE_ROOT + os.sep)):
            continue
        events[path] = events.get(path, 0) | flags
        if len(events) > 100000:
            return None
    # 完整性防线：helper 空闲收敛(STATUS=DONE)才说明历史已全部回放。
    # 注意设备相对事件流的 ID 不能与 FSEventsGetCurrentEventId 的全局 ID
    # 比较——空闲外置卷自身 ID 可能落后全局 ID 数百万，旧逻辑会因此把
    # 正常的“零变更”误判为截断，导致每次刷新都退化成全盘扫描。
    # 若 helper 达到轮询上限仍有事件(STATUS=TIMEOUT，常见于历史事件过多)，
    # 直接推进基线会永久跳过未读事件，必须回退全盘扫描。
    if not stream_complete:
        return None
    return events, latest


def _apply_event_refresh(conn, baseline, short_ready, report_progress,
                         quiet=False):
    """只对账 FSEvents 标记的路径，成功返回总数，失败回退全盘扫描。"""
    report_progress("读取变更日志", force=True)
    if isinstance(baseline, dict):
        events, latest = {}, {}
        current_roots = _scan_roots(VOLUME)
        if set(baseline) != set(current_roots):
            return None
        # 每个卷的 FSEvents 历史回放互不依赖，并行读取可把多卷等待时间
        # 从总和压到最慢单卷的耗时。
        with concurrent.futures.ThreadPoolExecutor(
                max_workers=len(current_roots)) as pool:
            futures = {
                root: pool.submit(
                    _changed_paths_since, baseline[root], volume=root)
                for root in current_roots
            }
            root_changes = {root: future.result()
                            for root, future in futures.items()}
        for root, changes in root_changes.items():
            if changes is None:
                return None
            root_events, latest[root] = changes
            for path, flags in root_events.items():
                events[path] = events.get(path, 0) | flags
        event_meta = ("fsevent_roots", json.dumps(
            latest, ensure_ascii=False, separators=(",", ":")))
    else:
        changes = _changed_paths_since(baseline)
        if changes is None:
            return None
        events, latest = changes
        event_meta = ("fsevent_id", str(latest))
    report_progress("分析变更", 0, max(1, len(events)), force=True)
    added = changed = deleted = 0
    records = {}
    removed = {}

    # 目录从废纸篓恢复到原位置时，FSEvents 会报告“目录改名”，但原路径
    # 及其全部子项通常仍在索引中。先批量确认这些目录，避免仅为更新时间
    # 重新扫描几十万文件；真正改到新路径的目录仍会完整扫描。
    indexed_dirs = set()
    event_paths = list(events)
    for start in range(0, len(event_paths), 800):
        chunk = event_paths[start:start + 800]
        marks = ",".join("?" for _ in chunk)
        indexed_dirs.update(row[0] for row in conn.execute(
            f"SELECT path FROM files WHERE is_dir=1 AND path IN ({marks})",
            chunk))

    def delete_aux(ids):
        if not ids:
            return
        for start in range(0, len(ids), 800):
            chunk = ids[start:start + 800]
            marks = ",".join("?" for _ in chunk)
            conn.execute(
                f"DELETE FROM short_grams WHERE file_rowid IN ({marks})", chunk)
            if FTS_OK:
                conn.execute(
                    f"DELETE FROM files_fts WHERE rowid IN ({marks})", chunk)
                conn.execute(
                    f"DELETE FROM path_fts WHERE rowid IN ({marks})", chunk)
            conn.execute(f"DELETE FROM fname WHERE rowid IN ({marks})", chunk)
            conn.execute(f"DELETE FROM fpbase WHERE rowid IN ({marks})", chunk)

    def rows_at_or_below(path):
        prefix = path.rstrip(os.sep) + os.sep
        return list(conn.execute(
            "SELECT rowid,is_dir FROM files "
            "WHERE path=? OR (path>=? AND path<?)",
            (path, prefix, prefix + "\U0010ffff")))

    def collect(path):
        try:
            st = os.stat(path, follow_symlinks=False)
        except OSError:
            return
        is_dir = int(os.path.isdir(path))
        name = os.path.basename(path.rstrip(os.sep))
        parent = os.path.dirname(path.rstrip(os.sep))
        size = None if is_dir else st.st_size
        records[path] = (path, name, parent, is_dir, size, st.st_mtime)

    # 先在文件系统上对账，再合并写入；避免对每个事件都做一次
    # FTS/短词索引的随机删除，在外接盘上可快数十倍。
    for index, (path, flags) in enumerate(events.items(), 1):
        if os.path.lexists(path):
            collect(path)
            if (os.path.isdir(path) and flags & (0x100 | 0x800) and
                    path not in indexed_dirs):
                discovered = 0
                for row in _walk(path, include_metadata=True,
                                 workers=SCAN_WORKERS):
                    collect(row[0])
                    discovered += 1
                    if discovered % 1000 == 0:
                        report_progress(
                            f"扫描变更目录（{discovered:,} 项）",
                            index - 1, max(1, len(events)))
        else:
            for rowid, is_dir in rows_at_or_below(path):
                removed[rowid] = is_dir
        report_progress("分析变更", index, max(1, len(events)))

    existing = {}
    record_paths = list(records)
    for start in range(0, len(record_paths), 800):
        chunk = record_paths[start:start + 800]
        marks = ",".join("?" for _ in chunk)
        for path, rowid, is_dir in conn.execute(
                f"SELECT path,rowid,is_dir FROM files WHERE path IN ({marks})",
                chunk):
            existing[path] = (rowid, is_dir)

    max_rowid = conn.execute(
        "SELECT COALESCE(MAX(rowid),0) FROM files").fetchone()[0]
    file_rows, fname_rows, fpbase_rows, fts_rows, path_fts_rows, short_rows = [], [], [], [], [], []
    replaced_ids = []
    added_files = type_delta = 0
    for path, (raw_path, name, parent, is_dir, size, mtime) in records.items():
        previous = existing.get(path)
        if previous:
            rowid, old_is_dir = previous
            replaced_ids.append(rowid)
            changed += 1
            type_delta += old_is_dir - is_dir
        else:
            max_rowid += 1
            rowid = max_rowid
            added += 1
            added_files += int(not is_dir)
        ext = name.rsplit(".", 1)[-1].lower() if "." in name else ""
        file_rows.append((rowid, raw_path, name, parent, is_dir, size, mtime))
        fname_rows.append((rowid, name, ext, is_dir))
        fpbase_rows.append((rowid, os.path.basename(parent), ext, is_dir))
        fts_rows.append((rowid, name))
        path_fts_rows.append((rowid, raw_path))
        if short_ready:
            short_rows.extend(
                (gram, rowid) for gram in _cjk_short_grams(name))

    deleted_ids = list(removed)
    # 若同一 rowid 已被现存路径复用，以现存路径为准。
    deleted_ids = [rowid for rowid in deleted_ids if rowid not in replaced_ids]
    deleted = len(deleted_ids)
    deleted_files = sum(not removed[rowid] for rowid in deleted_ids)

    report_progress("写入高速索引", len(events), max(1, len(events)),
                    added, changed, deleted, force=True)
    conn.execute("BEGIN")
    try:
        affected_ids = list(dict.fromkeys(replaced_ids + deleted_ids))
        delete_aux(affected_ids)
        for start in range(0, len(deleted_ids), 800):
            chunk = deleted_ids[start:start + 800]
            marks = ",".join("?" for _ in chunk)
            conn.execute(f"DELETE FROM files WHERE rowid IN ({marks})", chunk)
        conn.executemany(
            "INSERT OR REPLACE INTO files"
            "(rowid,path,name,parent,is_dir,size,mtime) VALUES(?,?,?,?,?,?,?)",
            file_rows)
        conn.executemany(
            "INSERT INTO fname(rowid,name,ext,is_dir) VALUES(?,?,?,?)", fname_rows)
        conn.executemany(
            "INSERT INTO fpbase(rowid,pbase,ext,is_dir) VALUES(?,?,?,?)",
            fpbase_rows)
        if FTS_OK and fts_rows:
            conn.executemany(
                "INSERT INTO files_fts(rowid,name) VALUES(?,?)", fts_rows)
            conn.executemany(
                "INSERT INTO path_fts(rowid,path) VALUES(?,?)", path_fts_rows)
        if short_ready and short_rows:
            conn.executemany(
                "INSERT OR IGNORE INTO short_grams(gram,file_rowid) VALUES(?,?)",
                short_rows)
        old_total = int(conn.execute(
            "SELECT value FROM meta WHERE key='total'").fetchone()[0])
        old_files = int(conn.execute(
            "SELECT value FROM meta WHERE key='files'").fetchone()[0])
        n_total = old_total + added - deleted
        n_files = old_files + added_files - deleted_files + type_delta
        conn.executemany(
            "INSERT OR REPLACE INTO meta(key,value) VALUES(?,?)",
            [("total", str(n_total)), ("files", str(n_files)),
             ("dirs", str(n_total - n_files)),
             ("updated", time.strftime("%Y-%m-%d %H:%M", time.localtime())),
             ("revision", str(time.time_ns())),
             event_meta,
             ("volume_root", os.path.abspath(VOLUME).rstrip(os.sep) or os.sep)])
        conn.commit()
    except Exception:
        conn.rollback()
        return None
    maintain_fts(conn, optimize=False)
    page_count = conn.execute("PRAGMA page_count").fetchone()[0]
    free_pages = conn.execute("PRAGMA freelist_count").fetchone()[0]
    if (deleted >= 50000 and page_count and
            free_pages * 4 >= page_count):
        report_progress("压缩索引", n_total, n_total,
                        added, changed, deleted, force=True)
        conn.execute("VACUUM")
    report_progress("校验索引", n_total, n_total,
                    added, changed, deleted, force=True)
    if not quiet:
        print(f"快速刷新完成: 新增 {added:,}, 更新 {changed:,}, "
              f"删除 {deleted:,}, 变更事件 {len(events):,}")
    report_progress("完成", n_total, n_total, added, changed, deleted, force=True)
    return n_total


def _acquire_build_lock(lock):
    """原子获取构建锁，防止两个刷新进程同时写索引。"""
    for _ in range(2):
        try:
            fd = os.open(lock, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        except FileExistsError:
            try:
                with open(lock, encoding="utf-8") as handle:
                    pid = int(handle.read().strip() or "0")
                if pid <= 0:
                    raise OSError("invalid lock pid")
                os.kill(pid, 0)
                raise RuntimeError("索引正在刷新中")
            except (OSError, ValueError):
                try:
                    os.remove(lock)
                except OSError:
                    pass
                continue
        else:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                handle.write(str(os.getpid()))
            return
    raise RuntimeError("无法获取索引刷新锁")


def build_short_index(db_path, quiet=False):
    """从 fname 表构建中文1–2字专用倒排索引。"""
    t0 = time.time()
    lock = _lock_file(db_path)
    _acquire_build_lock(lock)
    conn = _connect(db_path)
    create_schema(conn)
    conn.execute("DELETE FROM meta WHERE key='short_grams_ready'")
    conn.execute("DELETE FROM short_grams")
    conn.execute("DROP INDEX IF EXISTS idx_short_grams_rowid")
    conn.commit()
    processed = gram_count = 0
    cur = conn.execute("SELECT rowid, name FROM fname")
    while True:
        source = cur.fetchmany(2000)
        if not source:
            break
        rows = []
        for rid, name in source:
            rows.extend((gram, rid) for gram in _cjk_short_grams(name))
        if rows:
            conn.execute("BEGIN")
            conn.executemany(
                "INSERT OR IGNORE INTO short_grams(gram,file_rowid) VALUES(?,?)", rows)
            conn.commit()
            gram_count += len(rows)
        processed += len(source)
        if not quiet and processed % 20000 == 0:
            print(f"\r  短词索引 {processed:,} 项 | {time.time()-t0:.0f}s   ",
                  end="", flush=True)
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_short_grams_rowid ON short_grams(file_rowid)")
    conn.execute(
        "INSERT OR REPLACE INTO meta(key,value) VALUES('short_grams_ready','1')")
    conn.commit()
    try:
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    except sqlite3.Error:
        pass
    conn.close()
    try:
        os.remove(lock)
    except OSError:
        pass
    if not quiet:
        print(f"\r  短词索引完成: {processed:,} 项, {gram_count:,} 键 | "
              f"{time.time()-t0:.1f}s        ")
    return processed


def build_fts_index(db_path, quiet=False):
    """重建 trigram FTS，分批提交以免外接盘生成超大 WAL。"""
    t0 = time.time()
    lock = _lock_file(db_path)
    _acquire_build_lock(lock)
    conn = _connect(db_path)
    create_schema(conn)
    conn.execute("INSERT OR REPLACE INTO meta(key,value) VALUES('fts_ready','0')")
    conn.execute("DROP TABLE IF EXISTS files_fts")
    conn.execute(
        "CREATE VIRTUAL TABLE files_fts USING fts5(name, tokenize='trigram')")
    conn.execute("DROP TABLE IF EXISTS path_fts")
    conn.execute(
        "CREATE VIRTUAL TABLE path_fts USING fts5(path, tokenize='trigram')")
    conn.commit()
    total = 0
    cursor = conn.execute("SELECT rowid,name FROM fname ORDER BY rowid")
    path_cursor = conn.execute("SELECT rowid,path FROM files ORDER BY rowid")
    while True:
        rows = cursor.fetchmany(5000)
        path_rows = path_cursor.fetchmany(5000)
        if not rows and not path_rows:
            break
        conn.execute("BEGIN")
        if rows:
            conn.executemany(
                "INSERT INTO files_fts(rowid,name) VALUES(?,?)", rows)
        if path_rows:
            conn.executemany(
                "INSERT INTO path_fts(rowid,path) VALUES(?,?)", path_rows)
        conn.commit()
        total += len(rows)
        if not quiet:
            print(f"\r  FTS 重建 {total:,} 项 | {time.time()-t0:.0f}s   ",
                  end="", flush=True)
    conn.execute("INSERT OR REPLACE INTO meta(key,value) VALUES('fts_ready','1')")
    conn.commit()
    try:
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    except sqlite3.Error:
        pass
    conn.close()
    try:
        os.remove(lock)
    except OSError:
        pass
    if not quiet:
        print(f"\r  FTS 重建完成: {total:,} 项 | {time.time()-t0:.1f}s        ")
    return total


def build_index(db_path, quiet=False, incremental=False):
    t0 = time.time()
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    lock = _lock_file(db_path)
    _acquire_build_lock(lock)
    progress_path = _progress_file(db_path)
    last_progress = [0.0]

    def report_progress(stage, scanned=0, estimated=0, added=0, changed=0,
                        deleted=0, force=False):
        if not incremental:
            return
        now = time.time()
        if not force and now - last_progress[0] < 0.2:
            return
        last_progress[0] = now
        percent = (100 if stage == "完成" else
                   0 if not estimated else min(99, int(scanned * 100 / estimated)))
        data = {
            "stage": stage, "scanned": scanned, "estimated": estimated,
            "percent": percent, "added": added, "changed": changed,
            "deleted": deleted, "elapsed": round(now - t0, 1),
        }
        tmp = progress_path + ".tmp"
        try:
            with open(tmp, "w", encoding="utf-8") as handle:
                json.dump(data, handle, ensure_ascii=False)
            os.replace(tmp, progress_path)
        except OSError:
            pass
    conn = _connect(db_path)
    create_schema(conn)
    short_ready = _short_index_ready(conn)
    event_floor = 0
    baseline = 0

    if incremental:
        if sys.platform == "darwin" and os.path.realpath(VOLUME) == os.sep:
            baseline_row = conn.execute(
                "SELECT value FROM meta WHERE key='fsevent_roots'").fetchone()
            try:
                baseline = {str(k): int(v) for k, v in
                            json.loads(baseline_row[0]).items()} if baseline_row else {}
            except (TypeError, ValueError, json.JSONDecodeError):
                baseline = {}
        else:
            baseline_row = conn.execute(
                "SELECT value FROM meta WHERE key='fsevent_id'").fetchone()
            try:
                baseline = int(baseline_row[0]) if baseline_row else 0
            except (TypeError, ValueError):
                baseline = 0
        if baseline and not _force_full_incremental(conn):
            fast_total = _apply_event_refresh(
                conn, baseline, short_ready, report_progress, quiet)
            if fast_total is not None:
                _persist_uncovered(conn)
                try:
                    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
                except sqlite3.Error:
                    pass
                conn.close()
                try:
                    os.remove(lock)
                except OSError:
                    pass
                return fast_total

    # 没有可用日志基线时才需要全盘扫描，并记住开始时的
    # 卷事件位点。扫描期间的变化会在下一次快速刷新中对账。
    # 从已知基线附近开始读，避免重放全部历史事件。
    if sys.platform == "darwin" and os.path.realpath(VOLUME) == os.sep:
        root_baselines = baseline if isinstance(baseline, dict) else {}
        event_floor = {
            root: _current_event_id(
                start=max(0, int(root_baselines.get(root, 0)) - 100000), volume=root)
            for root in _scan_roots(VOLUME)
        }
        if not all(event_floor.values()):
            event_floor = {}
    else:
        event_floor = _current_event_id(start=max(0, baseline - 100000))

    if incremental:
        old = {}
        try:
            estimate_row = conn.execute(
                "SELECT value FROM meta WHERE key='total'").fetchone()
            load_estimate = int(estimate_row[0]) if estimate_row else 0
        except (sqlite3.Error, TypeError, ValueError):
            load_estimate = 0
        report_progress("读取旧索引", estimated=load_estimate, force=True)
        try:
            for loaded, row in enumerate(
                    conn.execute(
                        "SELECT path, rowid FROM files "
                        "INDEXED BY sqlite_autoindex_files_1"), 1):
                old[row[0]] = row[1]
                if loaded % 10000 == 0:
                    report_progress("读取旧索引", loaded, load_estimate)
        except sqlite3.OperationalError:
            old = {}
        changed = added = deleted = 0
        original_total = len(old)
        report_progress("读取目录结构", original_total,
                        original_total, force=True)
        old_dirs = {
            row[0] for row in conn.execute(
                "SELECT path FROM files INDEXED BY idx_dir WHERE is_dir=1")
        }
        # 新条目从现有最大 rowid 之后分配。从 1 重用会在
        # INSERT OR REPLACE 时静默覆盖完全无关的旧文件。
        rid = conn.execute("SELECT COALESCE(MAX(rowid), 0) FROM files").fetchone()[0]
    else:
        # 全量构建先顺序写事实和紧凑表，扫描结束后再批量建立索引；
        # 避免每插入一行都随机更新多个 B-tree/FTS segment。
        drop_query_indexes(conn)
        conn.execute("DELETE FROM files")
        conn.execute("DELETE FROM fname")
        conn.execute("DELETE FROM fpbase")
        conn.execute("DELETE FROM short_grams")
        conn.execute("DELETE FROM meta WHERE key='short_grams_ready'")
        short_ready = False
        try:
            conn.execute("DELETE FROM files_fts")
            conn.execute("DELETE FROM path_fts")
        except sqlite3.OperationalError:
            pass
        old, old_dirs, changed = {}, set(), 0

    total = 0
    if not incremental:
        rid = 0
    rows = []
    fname_rows = []
    fpbase_rows = []
    fts_rows = []
    path_fts_rows = []
    short_rows = []
    delete_ids = []
    last_flush = time.time()
    flush_target = 50000 if not incremental else 5000

    def flush():
        nonlocal rows, fname_rows, fpbase_rows, fts_rows, path_fts_rows, short_rows, delete_ids
        if not rows and not delete_ids:
            return
        conn.execute("BEGIN")
        try:
            for start in range(0, len(delete_ids), 800):
                chunk = delete_ids[start:start + 800]
                marks = ",".join("?" for _ in chunk)
                conn.execute(
                    f"DELETE FROM short_grams WHERE file_rowid IN ({marks})", chunk)
                if FTS_OK:
                    conn.execute(
                        f"DELETE FROM files_fts WHERE rowid IN ({marks})", chunk)
                    conn.execute(
                        f"DELETE FROM path_fts WHERE rowid IN ({marks})", chunk)
                conn.execute(f"DELETE FROM fname WHERE rowid IN ({marks})", chunk)
                conn.execute(f"DELETE FROM fpbase WHERE rowid IN ({marks})", chunk)
                conn.execute(f"DELETE FROM files WHERE rowid IN ({marks})", chunk)
            conn.executemany(
                "INSERT OR REPLACE INTO files(rowid,path,name,parent,is_dir,size,mtime) "
                "VALUES(?,?,?,?,?,?,?)", rows)
            conn.executemany(
                "INSERT OR REPLACE INTO fname(rowid,name,ext,is_dir) VALUES(?,?,?,?)",
                fname_rows)
            conn.executemany(
                "INSERT OR REPLACE INTO fpbase(rowid,pbase,ext,is_dir) VALUES(?,?,?,?)",
                fpbase_rows)
            if FTS_OK and incremental and fts_rows:
                conn.executemany(
                    "INSERT INTO files_fts(rowid,name) VALUES(?,?)",
                    fts_rows)
                conn.executemany(
                    "INSERT INTO path_fts(rowid,path) VALUES(?,?)",
                    path_fts_rows)
            if short_ready and short_rows:
                conn.executemany(
                    "INSERT OR IGNORE INTO short_grams(gram,file_rowid) VALUES(?,?)",
                    short_rows)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        rows, fname_rows, fpbase_rows = [], [], []
        fts_rows, path_fts_rows, short_rows, delete_ids = [], [], [], []

    n_files = n_dirs = 0
    if incremental:
        report_progress("扫描磁盘", estimated=original_total, force=True)
    scan_workers = benchmark_scan_workers(VOLUME)
    if not quiet:
        print(f"目录扫描并发度: {scan_workers}")
    for path, name, parent, is_dir, size, mtime in _walk(
            VOLUME, include_metadata=not incremental,
            known_paths=old if incremental else None,
            known_dirs=old_dirs if incremental else None,
            workers=scan_workers):
        if is_dir:
            n_dirs += 1
        else:
            n_files += 1
        total += 1
        if incremental:
            # pop 后 old 仅保留已删除路径，省掉同规模 seen 集合。
            previous = old.pop(path, None)
            if previous:
                report_progress("扫描磁盘", total, original_total,
                                added, changed)
                continue
            rid += 1
            rid_for_row = rid
            added += 1
            if not is_dir:
                try:
                    stat = os.stat(path, follow_symlinks=False)
                    size, mtime = stat.st_size, stat.st_mtime
                except OSError:
                    size = mtime = None
        else:
            rid += 1
            rid_for_row = rid
        rows.append((rid_for_row, path, name, parent, is_dir, size, mtime))
        ext = name.rsplit(".", 1)[-1].lower() if "." in name else ""
        fname_rows.append((rid_for_row, name, ext, is_dir))
        fpbase_rows.append((rid_for_row, os.path.basename(parent), ext, is_dir))
        if FTS_OK and incremental:
            fts_rows.append((rid_for_row, name))
            path_fts_rows.append((rid_for_row, path))
        if short_ready:
            short_rows.extend((gram, rid_for_row) for gram in _cjk_short_grams(name))

        if len(rows) >= flush_target:
            flush()
            if not quiet:
                el = time.time() - t0
                rate = total / el if el > 0 else 0
                print(f"\r  扫描中 {total:,} 项 | 文件 {n_files:,} 目录 {n_dirs:,} "
                      f"| {rate:.0f} 项/秒 | 用时 {el:.0f}s   ", end="", flush=True)
            # 防长时间无输出
            if time.time() - last_flush > 5:
                last_flush = time.time()
        if incremental:
            report_progress("扫描磁盘", total, original_total,
                            added, changed)

    flush()

    if not incremental:
        create_query_indexes(conn)

    if incremental:
        # 磁盘瞬时断开/读取失败时不能把大部分索引误删。
        if original_total and total < original_total // 2:
            conn.close()
            raise RuntimeError(
                f"扫描仅发现 {total:,}/{original_total:,} 项，已中止以保护原索引")
        failed = _failed_subtree_snapshot()
        protected = 0
        gone = len(old)
        report_progress("应用变更", total, original_total,
                        added, changed, gone, force=True)
        for path, previous in old.items():
            # 失败子树下的旧记录禁止删除：这些目录扫描不完整，不应视为已删除。
            if _under_failed_subtree(path, failed):
                protected += 1
                continue
            delete_ids.append(previous)
            if len(delete_ids) >= 5000:
                flush()
        flush()
        gone -= protected
        if not quiet:
            print(f"\r  增量更新完成: 新增 {added:,} 项, 更新 {changed:,} 项, "
                  f"删除 {gone:,} 项 (受保护 {protected:,}) | 当前共 {total:,} 项        ")
    else:
        if not quiet:
            print(f"\r  索引完成: {total:,} 项 (文件 {n_files:,} 目录 {n_dirs:,}) "
                  f"| 用时 {time.time()-t0:.1f}s | 库 {human_size(os.path.getsize(db_path))}")

    if incremental:
        report_progress("校验索引", total, original_total,
                        added, changed, gone, force=True)

    # FTS 对账: 主表与 FTS 行数不一致时重建 FTS (兜底异常中断)
    n_total_db = conn.execute("SELECT COUNT(*) FROM files").fetchone()[0]
    if FTS_OK:
        n_fts = conn.execute("SELECT COUNT(*) FROM files_fts").fetchone()[0]
        n_pfts = conn.execute("SELECT COUNT(*) FROM path_fts").fetchone()[0]
        if n_total_db != n_fts or n_total_db != n_pfts:
            if not quiet:
                print(f"  FTS 索引不同步({n_total_db} vs {n_fts}/{n_pfts}), 重建中...")
            conn.execute("DELETE FROM files_fts")
            conn.execute(
                "INSERT INTO files_fts(rowid, name) "
                "SELECT rowid, name FROM files")
            conn.execute("DELETE FROM path_fts")
            conn.execute(
                "INSERT INTO path_fts(rowid, path) "
                "SELECT rowid, path FROM files")
            conn.commit()
        conn.execute(
            "INSERT OR REPLACE INTO meta(key,value) VALUES('fts_ready','1')")
        conn.commit()
        maintain_fts(conn, optimize=not incremental)
    # fname 对账
    n_fname = conn.execute("SELECT COUNT(*) FROM fname").fetchone()[0]
    if n_total_db != n_fname:
        if not quiet:
            print(f"  fname 索引不同步({n_total_db} vs {n_fname}), 重建中...")
        conn.execute("DELETE FROM fname")
        conn.execute(
            "INSERT INTO fname(rowid, name, ext, is_dir) "
            "SELECT rowid, name, "
            "CASE WHEN instr(name,'.')>0 THEN lower(substr(name, instr(name,'.')+1)) "
            "ELSE '' END, is_dir FROM files")
        conn.commit()
    # fpbase 对账 (parent 的 basename + ext)
    n_fpbase = conn.execute("SELECT COUNT(*) FROM fpbase").fetchone()[0]
    if n_total_db != n_fpbase:
        if not quiet:
            print(f"  fpbase 索引不同步({n_total_db} vs {n_fpbase}), 重建中...")
        conn.execute("DELETE FROM fpbase")
        frows = conn.execute("SELECT rowid, parent, name, is_dir FROM files").fetchall()
        conn.executemany(
            "INSERT OR REPLACE INTO fpbase(rowid, pbase, ext, is_dir) VALUES(?,?,?,?)",
            [(r, os.path.basename(p), (n.rsplit(".", 1)[-1].lower() if "." in n else ""), d)
             for r, p, n, d in frows])
        conn.commit()

    # 统计信息写入 meta (stats 毫秒读取, 免全表 COUNT)
    n_files_db = conn.execute("SELECT COUNT(*) FROM files WHERE is_dir=0").fetchone()[0]
    meta_rows = [
        ("total", str(n_total_db)), ("files", str(n_files_db)),
        ("dirs", str(n_total_db - n_files_db)),
        ("updated", time.strftime("%Y-%m-%d %H:%M", time.localtime())),
        ("revision", str(time.time_ns())),
        ("volume_root", os.path.abspath(VOLUME).rstrip(os.sep) or os.sep),
        ("uncovered", json.dumps(_uncovered_snapshot(), ensure_ascii=False)),
        ("user_dirs_indexed", "1")]
    if event_floor:
        if isinstance(event_floor, dict):
            meta_rows.append(("fsevent_roots", json.dumps(
                event_floor, ensure_ascii=False, separators=(",", ":"))))
        else:
            meta_rows.append(("fsevent_id", str(event_floor)))
    conn.executemany(
        "INSERT OR REPLACE INTO meta(key,value) VALUES(?,?)",
        meta_rows)
    conn.commit()
    if not incremental:
        conn.execute("ANALYZE")
    conn.execute("PRAGMA optimize")

    if incremental and gone >= 50000:
        page_count = conn.execute("PRAGMA page_count").fetchone()[0]
        free_pages = conn.execute("PRAGMA freelist_count").fetchone()[0]
        if page_count and free_pages * 4 >= page_count:
            report_progress("压缩索引", n_total_db, n_total_db,
                            added, changed, gone, force=True)
            conn.execute("VACUUM")

    # 收敛 WAL: 避免 App 下次启动 replay 大 WAL
    try:
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    except Exception:
        pass
    conn.close()
    if incremental:
        report_progress("完成", n_total_db, n_total_db,
                        added, changed, gone, force=True)
    try:
        os.remove(lock)
    except OSError:
        pass
    return total


def rebuild_index_atomic(db_path, quiet=False):
    """用可丢弃临时库极速全量构建，完成后原子替换当前索引。"""
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    temporary = db_path + ".v4-building"
    for suffix in ("", "-wal", "-shm", "-journal"):
        try:
            os.remove(temporary + suffix)
        except OSError:
            pass
    previous_turbo = os.environ.get("LYCSEARCH_TURBO")
    previous_rebuild = os.environ.get("LYCSEARCH_REBUILD")
    os.environ["LYCSEARCH_TURBO"] = "1"
    os.environ["LYCSEARCH_REBUILD"] = "1"
    try:
        total = build_index(temporary, quiet=quiet, incremental=False)
        with open(temporary, "rb") as handle:
            os.fsync(handle.fileno())
        os.replace(temporary, db_path)
        for suffix in ("-wal", "-shm"):
            try:
                os.remove(db_path + suffix)
            except OSError:
                pass
        return total
    finally:
        if previous_turbo is None:
            os.environ.pop("LYCSEARCH_TURBO", None)
        else:
            os.environ["LYCSEARCH_TURBO"] = previous_turbo
        if previous_rebuild is None:
            os.environ.pop("LYCSEARCH_REBUILD", None)
        else:
            os.environ["LYCSEARCH_REBUILD"] = previous_rebuild
        for suffix in ("", "-wal", "-shm", "-journal"):
            try:
                os.remove(temporary + suffix)
            except OSError:
                pass


def refresh_index_atomic(db_path, quiet=False):
    """增量刷新在临时库上完成，校验通过后再原子替换正式索引。

    直接在正式库上就地刷新时，一旦中途出现权限拒绝、磁盘拔出或进程
    中断，已写入的部分结果就会污染在线索引。此处先把正式库在线备份
    到可丢弃的临时库，在其上执行增量刷新，成功后 os.replace 替换；
    任何失败都会丢弃临时库，旧索引保持不变。
    """
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    temporary = db_path + ".v4-incremental"
    for suffix in ("", "-wal", "-shm", "-journal"):
        try:
            os.remove(temporary + suffix)
        except OSError:
            pass
    try:
        # 一致性快照：SQLite 在线备份，避免直接在正式库上就地修改。
        src = _connect(db_path)
        try:
            dst = _connect(temporary)
            src.backup(dst)
            dst.close()
        finally:
            src.close()
        # 固定临时路径天然串行化：build_index 内部会加临时库锁，
        # 并发刷新会在临时库锁上冲突并报“索引正在刷新中”。
        total = build_index(temporary, quiet=quiet, incremental=True)
        # 真正启用中文单/双字倒排索引: 临时库完成基础构建后、替换正式库前
        # 全量重建 short_grams 并置 short_grams_ready=1。增量路径对
        # short_grams 的维护以该标志为前提, 因此必须在此建立一次;
        # 否则旧库(无标志/0行)增量永远不会建起该索引。
        build_short_index(temporary, quiet=True)
        with open(temporary, "rb") as handle:
            os.fsync(handle.fileno())
        os.replace(temporary, db_path)
        for suffix in ("-wal", "-shm"):
            try:
                os.remove(db_path + suffix)
            except OSError:
                pass
        return total
    finally:
        for suffix in ("", "-wal", "-shm", "-journal"):
            try:
                os.remove(temporary + suffix)
            except OSError:
                pass


# ---- 卷挂载过滤: 索引保留离线卷历史记录, 搜索时按已挂载卷过滤候选 ----
_VOL_MOUNT_CACHE = {}

def _vol_mounted(name):
    if name not in _VOL_MOUNT_CACHE:
        p = "/Volumes/" + name
        try:
            mounted = os.path.ismount(p)
            if mounted:
                _VOL_MOUNT_CACHE[name] = True
            else:
                # 软链指向根的系统卷(如 /Volumes/Macintosh HD -> /)视为在线;
                # 未挂载的移动卷/普通残留目录视为离线。
                _VOL_MOUNT_CACHE[name] = (
                    os.path.realpath(p) == os.path.realpath("/"))
        except OSError:
            _VOL_MOUNT_CACHE[name] = False
    return _VOL_MOUNT_CACHE[name]

def _path_is_mounted(path):
    """系统卷或已挂载卷返回 True；未挂载的 /Volumes/* 返回 False。"""
    if not path.startswith("/Volumes/"):
        return True
    name = path[len("/Volumes/"):].split("/", 1)[0]
    return _vol_mounted(name)

def _offline_volume_names():
    """当前 /Volumes 下未挂载的卷名(索引中的离线卷历史)。空=无离线卷。"""
    if not os.path.isdir("/Volumes"):
        return []
    return [n for n in os.listdir("/Volumes") if not _vol_mounted(n)]

def _mounted_rids(conn, rids):
    """返回位于已挂载卷上的 rowid 子集(排除离线卷候选)。"""
    if not rids:
        return set()
    keep = set()
    ids = list(set(rids))
    for start in range(0, len(ids), 800):
        chunk = ids[start:start + 800]
        marks = ",".join("?" for _ in chunk)
        for rid, path in conn.execute(
                f"SELECT rowid, path FROM files WHERE rowid IN ({marks})", chunk):
            if _path_is_mounted(path):
                keep.add(rid)
    return keep


# ---------------------------------------------------------------- 搜索层

def _search_full(conn, db_path, kws, file_only, dir_only, type_exts,
                 limit, sort_by, return_total, count_only, case_sensitive,
                 path_prefix, own_conn, t0):
    """完整路径搜索：文件名 ∪ 完整路径两路召回后按 rowid UNION 去重。

    与旧实现(只匹配一级父目录名 fpbase)不同：>=3 字符词对 path_fts
    (完整路径 trigram) 与 files_fts (文件名 trigram) 各查一次并合并；
    少于 3 字符/通配符/大小写敏感词走明确回退查询(父目录名 fpbase +
    文件名 fname 的 LIKE)。总数取去重集合大小，准确而非样本估算。
    """
    def _fts_eligible(kw):
        has_wild = ("*" in kw) or ("?" in kw)
        # FTS5 MATCH 把 - : ^ " ( ) / 等当查询语法，含它们的词必须回退，
        # 否则解析报错(如 eclipse-workspace 被当成 NOT 运算)。
        # 仅对纯字母/数字/下划线词走 FTS MATCH；含 . - : ^ " ( ) / 空格等
        # 查询语法字符的词必须回退，否则 FTS5 trigram 解析会报 syntax error。
        safe = bool(kw) and all(ch.isalnum() or ch == "_" for ch in kw)
        return bool(FTS_OK and not case_sensitive and not has_wild and safe
                    and len(kw) >= 3)

    def _path_fallback(esc, coll, kw):
        # 路径回退: 短词走 fpbase(一级父目录名, 紧凑表); 含特殊字符的长词
        # 走 files.path LIKE(完整路径), 保证覆盖且不触发 FTS 语法解析。
        if len(kw) >= 3:
            return {r[0] for r in conn.execute(
                f"SELECT rowid FROM files WHERE path LIKE ?{coll} ESCAPE '\\'",
                [f"%{esc}%"]).fetchall()}
        return {r[0] for r in conn.execute(
            f"SELECT rowid FROM fpbase WHERE pbase LIKE ?{coll} ESCAPE '\\'",
            [f"%{esc}%"]).fetchall()}

    path_sets, name_sets = [], []
    for kw in kws:
        if not kw:
            continue
        esc = kw.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
        coll = (" COLLATE NOCASE" if (not case_sensitive
                                      and re.search(r"[A-Za-z]", kw)) else "")
        if _fts_eligible(kw):
            rows = conn.execute(
                "SELECT rowid FROM path_fts WHERE path_fts MATCH ?", [kw]).fetchall()
            path_sets.append({r[0] for r in rows})
        else:
            path_sets.append(_path_fallback(esc, coll, kw))
        if _fts_eligible(kw):
            rows = conn.execute(
                "SELECT rowid FROM files_fts WHERE files_fts MATCH ?", [kw]).fetchall()
            name_sets.append({r[0] for r in rows})
        else:
            rows = conn.execute(
                f"SELECT rowid FROM fname WHERE name LIKE ?{coll} ESCAPE '\\'",
                [f"%{esc}%"]).fetchall()
            name_sets.append({r[0] for r in rows})

    path_ids = set.intersection(*path_sets) if path_sets else set()
    name_ids = set.intersection(*name_sets) if name_sets else set()
    ids = path_ids | name_ids

    if file_only or dir_only or type_exts:
        allowed = set()
        id_list = list(ids)
        exts = type_exts
        for start in range(0, len(id_list), 800):
            chunk = id_list[start:start + 800]
            marks = ",".join("?" for _ in chunk)
            for rid, is_dir, ext in conn.execute(
                    f"SELECT rowid, is_dir, ext FROM fname WHERE rowid IN ({marks})",
                    chunk):
                if file_only and is_dir:
                    continue
                if dir_only and not is_dir:
                    continue
                if exts and ext not in exts:
                    continue
                allowed.add(rid)
        ids = allowed

    if path_prefix:
        prefix = os.path.abspath(path_prefix).rstrip(os.sep) + os.sep
        allowed = set()
        id_list = list(ids)
        for start in range(0, len(id_list), 800):
            chunk = id_list[start:start + 800]
            marks = ",".join("?" for _ in chunk)
            for rid, path in conn.execute(
                    f"SELECT rowid, path FROM files WHERE rowid IN ({marks})", chunk):
                if path == path_prefix or path.startswith(prefix):
                    allowed.add(rid)
        ids = allowed

    # 卷过滤: 排除离线卷候选, 使 total=在线卷候选数(精确到卷)。
    if _offline_volume_names():
        ids = _mounted_rids(conn, ids)

    total = len(ids)
    if count_only:
        if own_conn:
            conn.close()
        return total, time.time() - t0, ""
    if not ids:
        if own_conn:
            conn.close()
        return [], time.time() - t0, (0 if return_total else "")

    details = {}
    id_list = list(ids)
    for start in range(0, len(id_list), 800):
        chunk = id_list[start:start + 800]
        marks = ",".join("?" for _ in chunk)
        for row in conn.execute(
                f"SELECT rowid, path, is_dir, size, mtime FROM files "
                f"WHERE rowid IN ({marks})", chunk):
            details[row[0]] = row

    def _score(name, is_path):
        s = 0.0
        nm = normalize_search_key(name)
        stem = normalize_search_key(os.path.splitext(name)[0])
        for kw in kws:
            if not kw or "*" in kw or "?" in kw:
                continue
            kk = normalize_search_key(kw)
            if not kk:
                continue
            if is_path:
                if nm == kk:
                    s += 3200
                elif nm.startswith(kk):
                    s += 2400
                elif kk in nm:
                    s += 1400 - min(800, nm.find(kk) * 8)
                continue
            if nm == kk:
                s += 12000
            elif stem == kk:
                s += 11000
            elif nm.startswith(kk):
                s += 9000
            elif stem.startswith(kk):
                s += 8500
            else:
                pos = nm.find(kk)
                if pos >= 0:
                    boundary = pos == 0 or not nm[pos - 1].isalnum()
                    s += (7600 if boundary else 6000) - min(1800, pos * 12)
            s -= min(500, len(nm) * .5)
        return s

    if sort_by == "size":
        keyf = lambda rid: (-(details[rid][3] or 0), details[rid][1])
    elif sort_by == "time":
        keyf = lambda rid: (-(details[rid][4] or 0), details[rid][1])
    elif sort_by == "path":
        keyf = lambda rid: details[rid][1]
    elif sort_by == "name":
        keyf = lambda rid: details[rid][1].lower()
    else:
        keyf = lambda rid: _score(
            os.path.basename(details[rid][1]),
            rid in path_ids and rid not in name_ids)

    rid_list = list(details.keys())
    # 保持全部候选参与排序；用有界 Top-K 避免排序前截断导致漏掉最大记录。
    if limit:
        top = heapq.nsmallest(max(0, limit), rid_list, key=keyf)
    else:
        top = sorted(rid_list, key=keyf)
    result = [details[r][1:] for r in top if r in details]
    if own_conn:
        conn.close()
    marker = total if return_total else len(top) < total
    return result, time.time() - t0, marker


def _kw_conditions(kws, full, case_sensitive, conn, pbase_only=False):
    """为每个关键词生成 (conds, params, fts_used, like_used, full_used)。
    - 默认: >=3字符走 trigram FTS(毫秒); 1-2字符/通配符走 fname 紧凑表 LIKE
    - full/pbase_only: 走 fpbase 紧凑表(9MB), 匹配父目录名
    - 含 ASCII 字母的词加 NOCASE, 纯中文/数字用二进制 LIKE(更快)
    """
    conds, params = [], []
    fts_used = False
    like_used = False
    full_used = False
    for kw in kws:
        if not kw:
            continue
        has_wild = ("*" in kw) or ("?" in kw)
        coll = (" COLLATE NOCASE" if (not case_sensitive
                                      and re.search(r"[A-Za-z]", kw)) else "")
        # 大小写语义：默认不敏感；--case 由 search() 顶层在自建连接上
        # 用 PRAGMA case_sensitive_like=ON 实现（LIKE 运算符会把操作数
        # 转回 TEXT，CAST BLOB 技巧对 LIKE 无效，不能用于连接池共享连接）。
        like_name = f"fname.name LIKE ?{coll} ESCAPE '\\'"
        like_pbase = f"fpbase.pbase LIKE ?{coll} ESCAPE '\\'"
        # 通配符 *.ext 形式 -> ext 等值(走索引, 毫秒级)
        m_ext = re.fullmatch(r"\*\.[A-Za-z0-9]{1,10}", kw) if has_wild else None
        if full or pbase_only:
            if m_ext:
                conds.append("fpbase.ext = ?")
                params.append(m_ext.group(0)[2:].lower())
            elif has_wild:
                conds.append(like_pbase)
                params.append(_glob_to_like(kw))
            else:
                esc = kw.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
                conds.append(like_pbase)
                params.append(f"%{esc}%")
            full_used = True
        elif m_ext:
            conds.append("fname.ext = ?")
            params.append(m_ext.group(0)[2:].lower())
            like_used = True
        elif has_wild:
            conds.append(like_name)
            params.append(_glob_to_like(kw))
            like_used = True
        elif FTS_OK and not case_sensitive and len(kw) >= 3 and "\"" not in kw:
            conds.append("files_fts MATCH ?")
            params.append('"' + kw.replace('"', '""') + '"')
            fts_used = True
        else:
            esc = kw.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
            conds.append(like_name)
            params.append(f"%{esc}%")
            like_used = True
    return conds, params, fts_used, like_used, full_used


def search(db_path, kws, file_only=False, dir_only=False, full=False,
           type_filter=None, limit=30, count_only=False, case_sensitive=False,
           sort_by=None, conn=None, pbase_only=False, return_total=False,
           path_prefix=None):
    """两阶段搜索(实测避免 SQLite 跨表回表慢 30s):
    阶段1: 紧凑表(fname/fpbase/FTS)单表查询拿 (rowid, 匹配列) —— 毫秒~秒级
    阶段2: Python 合并/排序/分页 -> files 点查详情(命中集小, 快)
    """
    if not os.path.exists(db_path):
        return None, "索引不存在, 请先运行: python3 lycsearch.py build", ""

    t0 = time.time()
    type_exts = type_extensions(type_filter)
    offline_vols = _offline_volume_names()
    own_conn = conn is None
    if own_conn:
        conn = _connect_ro(db_path)
        if case_sensitive:
            # 仅在自建短连接上翻转（用完即关，绝不影响连接池共享连接）。
            conn.execute("PRAGMA case_sensitive_like=ON")

    # GUI 打开现有索引时也必须启用 trigram FTS。
    _detect_fts(conn)

    short_terms = list(dict.fromkeys(kws))
    short_used = bool(
        short_terms and not full and not pbase_only and not case_sensitive and
        not offline_vols and
        _short_index_ready(conn) and
        all(1 <= len(term) <= 2 and all(_is_cjk(ch) for ch in term)
            for term in short_terms)
    )
    if short_used:
        # 中文短词的高并发快路径：匹配、过滤、计数、排序和详情一次下推
        # 给 SQLite C 层，避免 Python 拉取并排序成千上万个中间结果。
        marks = ",".join("?" for _ in short_terms)
        cte = (
            f"WITH matched AS (SELECT file_rowid FROM short_grams "
            f"WHERE gram IN ({marks}) GROUP BY file_rowid HAVING COUNT(*)=?) ")
        base_params = short_terms + [len(short_terms)]
        filters = []
        filter_params = []
        if file_only:
            filters.append("n.is_dir=0")
        if dir_only:
            filters.append("n.is_dir=1")
        if type_exts:
            extensions = sorted(type_exts)
            filters.append("n.ext IN (" + ",".join("?" for _ in extensions) + ")")
            filter_params.extend(extensions)
        if path_prefix:
            prefix = os.path.abspath(path_prefix).rstrip(os.sep) + os.sep
            filters.append("(f.path=? OR (f.path>=? AND f.path<?))")
            filter_params.extend([path_prefix, prefix, prefix + "\U0010ffff"])
        where = (" WHERE " + " AND ".join(filters)) if filters else ""
        joins = ("FROM matched m JOIN fname n ON n.rowid=m.file_rowid "
                 "JOIN files f ON f.rowid=m.file_rowid")
        total = conn.execute(
            cte + "SELECT COUNT(*) " + joins + where,
            base_params + filter_params).fetchone()[0]
        if count_only:
            if own_conn:
                conn.close()
            return total, time.time() - t0, ""
        if sort_by == "size":
            order = " ORDER BY COALESCE(f.size,0) DESC, n.name COLLATE NOCASE"
            order_params = []
        elif sort_by == "time":
            order = " ORDER BY COALESCE(f.mtime,0) DESC, n.name COLLATE NOCASE"
            order_params = []
        elif sort_by == "path":
            order = " ORDER BY f.path COLLATE NOCASE"
            order_params = []
        elif sort_by == "name":
            order = " ORDER BY n.name COLLATE NOCASE"
            order_params = []
        else:
            rank_parts = ["CASE WHEN instr(n.name,?)=1 THEN 0 ELSE 1 END"
                          for _ in short_terms]
            order = " ORDER BY " + "+".join(rank_parts) + ", n.name COLLATE NOCASE"
            order_params = short_terms
        limit_sql = "" if limit == 0 else " LIMIT ?"
        limit_params = [] if limit == 0 else [max(0, limit)]
        rows = conn.execute(
            cte + "SELECT f.path,f.is_dir,f.size,f.mtime " + joins + where +
            order + limit_sql,
            base_params + filter_params + order_params + limit_params).fetchall()
        if own_conn:
            conn.close()
        marker = total if return_total else len(rows) < total
        return rows, time.time() - t0, marker
    if full and not pbase_only:
        # 完整路径搜索: 文件名 ∪ 完整路径, UNION rowid 去重, 返回准确总数。
        return _search_full(
            conn, db_path, kws, file_only, dir_only, type_exts,
            limit, sort_by, return_total, count_only, case_sensitive,
            path_prefix, own_conn, t0)
    conds, params, fts_used, like_used, full_used = _kw_conditions(
        kws, full, case_sensitive, conn, pbase_only=pbase_only)
    if not conds:
        if own_conn:
            conn.close()
        return (0 if count_only else []), 0.0, (0 if return_total else "")

    # ---- 阶段1: 紧凑表单表查询(无跨表 JOIN, 不回表主库) ----
    if full_used and not fts_used:
        # 父目录名分支: fpbase 表(rowid, pbase) + 类型/范围过滤
        where = " AND ".join(conds)
        if file_only:
            where += " AND is_dir = 0"
        if dir_only:
            where += " AND is_dir = 1"
        if type_exts:
            pat = "','".join(sorted(type_exts))
            where += f" AND ext IN ('{pat}')"
        rows = conn.execute(
            f"SELECT rowid, pbase FROM fpbase WHERE {where}", params).fetchall()
        hits = [(r[0], r[1], 1) for r in rows]  # (rowid, match_name, is_pbase)
    elif fts_used:
        # 多个长词合并为一个 FTS AND 查询；旧实现只使用第一词。
        pairs = list(zip(conds, params))
        fts_terms = [p for c, p in pairs if c == "files_fts MATCH ?"]
        like_pairs = [(c, p) for c, p in pairs if c != "files_fts MATCH ?"]
        rows = conn.execute(
            "SELECT rowid, name FROM files_fts WHERE files_fts MATCH ?",
            [" AND ".join(fts_terms)]).fetchall()
        hits = [(r[0], r[1], 0) for r in rows]
        if like_pairs:
            # 多词: 其余词走 fname, 与 FTS 结果求交集
            where = " AND ".join(c for c, _ in like_pairs)
            like_params = [p for _, p in like_pairs]
            if file_only:
                where += " AND is_dir = 0"
            if dir_only:
                where += " AND is_dir = 1"
            if type_exts:
                ext_marks = ",".join("?" for _ in type_exts)
                where += f" AND ext IN ({ext_marks})"
                like_params.extend(sorted(type_exts))
            like_rows = conn.execute(
                f"SELECT rowid, name FROM fname WHERE {where}", like_params).fetchall()
            like_map = {r[0]: (r[1], 0) for r in like_rows}
            hits = [(rid, like_map[rid][0], 0)
                    for rid, _, _ in hits if rid in like_map]
        elif file_only or dir_only or type_exts:
            # FTS 表只含文件名，范围/类型需按 rowid 回 fname 紧凑表过滤。
            allowed = set()
            ids = [h[0] for h in hits]
            exts = type_exts
            for start in range(0, len(ids), 800):
                chunk = ids[start:start + 800]
                marks = ",".join("?" for _ in chunk)
                for rid, is_dir, ext in conn.execute(
                        f"SELECT rowid, is_dir, ext FROM fname WHERE rowid IN ({marks})",
                        chunk):
                    if file_only and is_dir:
                        continue
                    if dir_only and not is_dir:
                        continue
                    if exts and ext not in exts:
                        continue
                    allowed.add(rid)
            hits = [h for h in hits if h[0] in allowed]
    else:
        # 默认: fname 表(rowid, name) + 类型/范围过滤
        where = " AND ".join(conds)
        if file_only:
            where += " AND is_dir = 0"
        if dir_only:
            where += " AND is_dir = 1"
        if type_exts:
            pat = "','".join(sorted(type_exts))
            where += f" AND ext IN ('{pat}')"
        rows = conn.execute(
            f"SELECT rowid, name FROM fname WHERE {where}", params).fetchall()
        hits = [(r[0], r[1], 0) for r in rows]

    if path_prefix and hits:
        prefix = os.path.abspath(path_prefix).rstrip(os.sep) + os.sep
        allowed = set()
        ids = [item[0] for item in hits]
        for start in range(0, len(ids), 800):
            chunk = ids[start:start + 800]
            marks = ",".join("?" for _ in chunk)
            for rid, path in conn.execute(
                    f"SELECT rowid,path FROM files WHERE rowid IN ({marks})",
                    chunk):
                if path == path_prefix or path.startswith(prefix):
                    allowed.add(rid)
        hits = [item for item in hits if item[0] in allowed]

    # 卷过滤: 排除离线卷候选, 使 total=在线卷候选数(精确到卷)。
    if offline_vols and hits:
        hits = [h for h in hits
                if h[0] in _mounted_rids(conn, [h[0] for h in hits])]

    if not hits:
        if own_conn:
            conn.close()
        return ((0 if count_only else []), time.time() - t0,
                (0 if return_total else ""))

    # ---- 阶段2: Python 合并/过滤/排序/分页 -> files 详情 ----
    if count_only:
        if own_conn:
            conn.close()
        return len(hits), time.time() - t0, ""

    def take(items):
        return items if limit == 0 else items[:max(0, limit)]

    def fetch_details(rowids):
        details = {}
        for start in range(0, len(rowids), 800):
            chunk = rowids[start:start + 800]
            marks = ",".join("?" for _ in chunk)
            for row in conn.execute(
                    f"SELECT rowid, path, is_dir, size, mtime FROM files "
                    f"WHERE rowid IN ({marks})", chunk):
                details[row[0]] = row
        return details

    # 匹配度排序(前缀 > 子串 > 父目录), 或指定字段
    if sort_by and sort_by in ("name", "size", "time", "path"):
        rid_list = [h[0] for h in hits]
        def _file_rows(rids):
            # 按批读取详情，避免一次拉全量进内存。
            for start in range(0, len(rids), 800):
                chunk = rids[start:start + 800]
                marks = ",".join("?" for _ in chunk)
                for row in conn.execute(
                        f"SELECT rowid, path, is_dir, size, mtime FROM files "
                        f"WHERE rowid IN ({marks})", chunk):
                    yield row
        def _sort_key(row):
            if sort_by == "size":
                return (-(row[3] or 0), row[1])
            if sort_by == "time":
                return (-(row[4] or 0), row[1])
            if sort_by == "path":
                return row[1]
            return row[1].lower()
        # 保持全部候选参与排序；详情分批读取；用有界 Top-K 取前 limit 条，
        # 避免排序前截断导致漏掉真正最大/最新/最靠前的记录。
        if limit:
            ordered_rows = heapq.nsmallest(
                max(0, limit), _file_rows(rid_list), key=_sort_key)
        else:
            ordered_rows = sorted(_file_rows(rid_list), key=_sort_key)
        result = [row[1:] for row in ordered_rows]
        if own_conn:
            conn.close()
        marker = len(hits) if return_total else len(ordered_rows) < len(hits)
        return result, time.time() - t0, marker

    # 默认匹配度排序：FTS/短词索引只负责召回，最终顺序由文件搜索
    # 专用特征决定，不使用文档全文检索的 BM25 作为最终相关度。
    def _score(name, is_pbase):
        s = 0.0
        nm = normalize_search_key(name)
        stem = normalize_search_key(os.path.splitext(name)[0])
        for kw in kws:
            if not kw or "*" in kw or "?" in kw:
                continue
            kk = normalize_search_key(kw)
            if not kk:
                continue
            if is_pbase:
                if nm == kk:
                    s += 3200
                elif nm.startswith(kk):
                    s += 2400
                elif kk in nm:
                    s += 1400 - min(800, nm.find(kk) * 8)
                continue
            if nm == kk:
                s += 12000
            elif stem == kk:
                s += 11000
            elif nm.startswith(kk):
                s += 9000
            elif stem.startswith(kk):
                s += 8500
            else:
                pos = nm.find(kk)
                if pos >= 0:
                    boundary = pos == 0 or not nm[pos - 1].isalnum()
                    s += (7600 if boundary else 6000) - min(1800, pos * 12)
            # 更短、更浅的结果在同类命中中自然靠前。
            s -= min(500, len(nm) * .5)
        return s

    scored = [(_score(n, ip), r, n) for r, n, ip in hits]
    scored.sort(key=lambda x: (-x[0], x[2].lower()))
    top = take(scored)
    ordered_ids = [r for _, r, _ in top]
    det = fetch_details(ordered_ids)
    # SQL IN 不保证顺序；按评分后的 rowid 明确组装结果。
    result = [det[r][1:] for r in ordered_ids if r in det]
    if own_conn:
        conn.close()
    marker = len(scored) if return_total else len(top) < len(scored)
    return result, time.time() - t0, marker


def _fmt_row(path, is_dir, size, mtime):
    kind = "\033[1;34m[D]\033[0m" if is_dir else "\033[0m[F]"
    sz = human_size(size)
    mt = time.strftime("%Y-%m-%d %H:%M", time.localtime(mtime)) if mtime else "-"
    return f"{kind} {sz:>8}  {mt}  {path}"


def print_help():
    print(__doc__)


def interactive(db_path):
    """连续模糊搜索 REPL。"""
    if not os.path.exists(db_path):
        print("索引不存在, 先运行: python3 lycsearch.py build")
        return
    conn = _connect_ro(db_path)
    n = conn.execute("SELECT COUNT(*) FROM files").fetchone()[0]
    age = time.time() - os.path.getmtime(db_path)
    conn.close()
    age_s = "刚刚" if age < 60 else f"{age/60:.0f} 分钟前" if age < 3600 else f"{age/3600:.1f} 小时前"
    print(f"{os.path.basename(VOLUME.rstrip(os.sep)) or VOLUME} 模糊搜索  "
          f"索引 {n:,} 项 | 更新于 {age_s} | 输入关键词回车搜索, 输入 q 退出")
    while True:
        try:
            kw = input("搜索> ").strip()
        except (EOFError, KeyboardInterrupt):
            print()
            return
        if not kw:
            return
        if kw in ("q", "quit", "exit"):
            return
        parts = kw.split()
        rows, elapsed, truncated = search(db_path, parts, limit=30)
        if rows is None:
            print(elapsed)
            return
        if not rows:
            print(f"  无结果 ({elapsed*1000:.0f} ms)")
            continue
        for i, (path, is_dir, size, mtime) in enumerate(rows, 1):
            print(f"  {i:>3}. {_fmt_row(path, is_dir, size, mtime)}")
        note = f"  仅显示前 {len(rows)} 条" if truncated else ""
        print(f"  ── 共 {len(rows)} 条{note}, 耗时 {elapsed*1000:.0f} ms")


# ---------------------------------------------------------------- 入口

def main():
    if len(sys.argv) == 1:
        interactive(DEFAULT_DB)
        return

    cmd = sys.argv[1]
    if cmd in ("-h", "--help", "help"):
        print_help()
        return

    if cmd == "build":
        quiet = "-q" in sys.argv or "--quiet" in sys.argv
        print(f"开始全量扫描 {VOLUME} ...")
        t0 = time.time()
        n = rebuild_index_atomic(DEFAULT_DB, quiet=quiet)
        print(f"完成: {n:,} 项, 用时 {time.time()-t0:.1f}s")
        print("现在可以直接搜索了, 例如: python3 lycsearch.py 三亚")
        return

    if cmd == "refresh":
        print(f"增量更新 {VOLUME} ...")
        t0 = time.time()
        n = refresh_index_atomic(DEFAULT_DB, quiet=False)
        print(f"完成, 用时 {time.time()-t0:.1f}s")
        return

    if cmd == "shortindex":
        print("正在构建中文一至两字快速索引 ...")
        build_short_index(DEFAULT_DB, quiet=False)
        return

    if cmd == "ftsindex":
        print("正在重建模糊搜索索引 ...")
        build_fts_index(DEFAULT_DB, quiet=False)
        return

    if cmd == "stats":
        if not os.path.exists(DEFAULT_DB):
            print("索引不存在")
            return
        conn = _connect(DEFAULT_DB)
        n = conn.execute("SELECT COUNT(*) FROM files").fetchone()[0]
        nf = conn.execute("SELECT COUNT(*) FROM files WHERE is_dir=0").fetchone()[0]
        nd = n - nf
        total_size = conn.execute(
            "SELECT SUM(size) FROM files WHERE is_dir=0").fetchone()[0]
        conn.close()
        print(f"索引文件 : {DEFAULT_DB}")
        print(f"条目总数 : {n:,}  (文件 {nf:,} / 目录 {nd:,})")
        print(f"文件总大小: {human_size(total_size)}")
        print(f"索引更新 : {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(os.path.getmtime(DEFAULT_DB)))}")
        return

    # 否则: 模糊搜索
    parser = argparse.ArgumentParser(add_help=False, usage="python3 lycsearch.py <关键词...> [选项]")
    parser.add_argument("kws", nargs="+")
    parser.add_argument("-f", "--file", action="store_true")
    parser.add_argument("-d", "--dir", action="store_true")
    parser.add_argument("--full", action="store_true")
    parser.add_argument("-t", "--type", choices=list(TYPE_EXTS.keys()))
    parser.add_argument("--limit", type=int, default=30)
    parser.add_argument("-c", "--count", action="store_true")
    parser.add_argument("--case", action="store_true")
    parser.add_argument("--sort", choices=["name", "size", "time", "path"])
    parser.add_argument("--db", default=DEFAULT_DB)
    try:
        args = parser.parse_args(sys.argv[1:])
    except SystemExit:
        print_help()
        return

    rows, elapsed, truncated = search(
        args.db, args.kws, file_only=args.file, dir_only=args.dir,
        full=args.full, type_filter=args.type, limit=args.limit,
        count_only=args.count, case_sensitive=args.case, sort_by=args.sort)

    if isinstance(rows, int):  # count 模式
        print(f"命中 {rows:,} 条  (耗时 {elapsed*1000:.0f} ms)")
        return
    if rows is None:
        print(elapsed)
        return
    if not rows:
        print(f"无结果 (耗时 {elapsed*1000:.0f} ms)")
        return
    for i, (path, is_dir, size, mtime) in enumerate(rows, 1):
        print(f"{i:>4}. {_fmt_row(path, is_dir, size, mtime)}")
    note = f"仅显示前 {len(rows)} 条, 可用 --limit 0 全部显示" if truncated else f"共 {len(rows)} 条"
    print(f"── {note}, 耗时 {elapsed*1000:.0f} ms")


if __name__ == "__main__":
    main()
