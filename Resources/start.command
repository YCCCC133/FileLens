#!/bin/bash
DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$DIR"
# 优先使用 App 自带的 Intel + Apple Silicon 通用 Python，macOS 14+ 无需安装开发工具。
PY=""
BUNDLED=0
OS_MAJOR=$(/usr/bin/sw_vers -productVersion 2>/dev/null | /usr/bin/cut -d. -f1)
case "$OS_MAJOR" in ''|*[!0-9]*) OS_MAJOR=0;; esac
if [ "$OS_MAJOR" -ge 14 ] && [ -x "$DIR/python/bin/python3" ]; then
    PY="$DIR/python/bin/python3"
    BUNDLED=1
fi
if [ ! -x "$PY" ]; then
    for CANDIDATE in /usr/bin/python3 /opt/homebrew/bin/python3 /usr/local/bin/python3; do
        if [ -x "$CANDIDATE" ]; then PY="$CANDIDATE"; break; fi
    done
fi
if [ ! -x "$PY" ]; then
    /usr/bin/osascript -e 'display alert "文件搜索器无法启动" message "未找到 Python 3 运行时，请重新复制完整的 App。" as critical'
    exit 1
fi
# 压缩运行库需要明确指定自身位置，且不读取当前电脑的用户 Python 包。
if [ "$BUNDLED" -eq 1 ]; then
    export PYTHONHOME="$DIR/python"
    export PYTHONNOUSERSITE=1
fi
# 由 Terminal 授予移动卷访问上下文，随后立即退出命令窗口。
nohup "$PY" -u "$DIR/lycapp.py" >>/tmp/lyc-filesearch.log 2>&1 </dev/null &
exit 0
