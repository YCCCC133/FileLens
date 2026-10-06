# FileLens · 文件搜索器

AppKit 和 WebKit 原生窗口配合 Python 本地服务，通过 SQLite 索引快速搜索文件。

## 功能

- 文件名、路径、多关键词及文件类型过滤
- SQLite FTS 索引和增量更新
- FSEvents 监听文件变化
- 本地服务随机令牌验证

## 从源码构建

要求 macOS 14 或更新版本，以及 Xcode Command Line Tools。

```bash
./build.sh --compile-only
```

这会编译 Swift 程序到 `build/`。运行完整应用还需要以下依赖：

Python 3 标准库，无第三方 Python 包。App 优先使用 `Resources/python/bin/python3`，否则使用 `/usr/bin/python3`；构建前请确认系统 Python 可用，或提供相同布局的独立 Python 运行时。

`Sources/` 存放原生界面源码，`Resources/` 存放应用资源和后端脚本。准备好依赖后运行 `./build.sh` 生成应用包。构建脚本执行本地 ad-hoc 签名；正式发布需另行签名和公证。

如已有对应应用的完整依赖，可以指定其资源目录重新打包：

```bash
APP_RESOURCES="/path/to/Application.app/Contents/Resources" ./build.sh
```

此方式复用第三方运行时，重新编译本仓库的原生程序；构建结果在 `build/`，不提交到源码仓库。脚本不会打包已有的文件索引或用户转换结果。

## 命令行使用

```bash
LYCSEARCH_VOLUME="/path/to/folder" LYCSEARCH_DB="/tmp/file-search.db" python3 Resources/lycsearch.py build
LYCSEARCH_VOLUME="/path/to/folder" LYCSEARCH_DB="/tmp/file-search.db" python3 Resources/lycsearch.py "关键词"
```

索引由用户运行时生成，仓库不包含任何本机文件清单。直接运行源码时建议用这两个环境变量指定扫描目录和数据库位置。

## 许可证与第三方组件

本项目原创界面、脚本和构建文件使用 [MIT License](LICENSE)。第三方源码、图标和运行时遵循其原有许可，根目录许可不覆盖第三方材料。

原生界面使用 Apple AppKit/WebKit；搜索后端使用 Python 和 SQLite。运行时另行分发时应保留其许可证。
