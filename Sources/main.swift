import AppKit
import WebKit

private let appVersion = "4.0"

/// WKWebView 会吞掉鼠标事件，单靠 isMovableByWindowBackground 无法拖窗口。
/// 在顶部工具栏的空白区域直接交给 AppKit 执行原生拖动。
final class DraggableWebView: WKWebView {
    override func mouseDown(with event: NSEvent) {
        let point = convert(event.locationInWindow, from: nil)
        let inToolbar = point.y >= bounds.height - 52
        let afterTrafficLights = point.x >= 78
        let beforeControls = point.x <= bounds.width - 330
        if inToolbar && afterTrafficLights && beforeControls {
            window?.performDrag(with: event)
            return
        }
        super.mouseDown(with: event)
    }
}

/// 放在 WebView 之上的真正 AppKit 拖动层。WKWebView 内部有多层子视图，
/// 因此只重写 WKWebView.mouseDown 在部分 macOS 版本上仍收不到事件。
final class WindowDragHandle: NSView {
    override func mouseDown(with event: NSEvent) {
        window?.performDrag(with: event)
    }
}

final class AppDelegate: NSObject, NSApplicationDelegate, WKNavigationDelegate, WKScriptMessageHandler, NSWindowDelegate {
    private var window: NSWindow!
    private var webView: WKWebView!
    private var backend: Process?
    private var pollTimer: Timer?
    private var attempts = 0
    private var serviceURL: URL?

    func applicationDidFinishLaunching(_ notification: Notification) {
        buildWindow()
        startBackend()
        pollForService()
    }

    func applicationShouldTerminateAfterLastWindowClosed(_ sender: NSApplication) -> Bool { true }

    func applicationWillTerminate(_ notification: Notification) {
        pollTimer?.invalidate()
        if let base = serviceURL {
            var request = URLRequest(url: base.appendingPathComponent("api/quit"))
            request.httpMethod = "POST"
            URLSession.shared.dataTask(with: request).resume()
        }
        if let process = backend, process.isRunning { process.terminate() }
    }

    private func buildWindow() {
        let config = WKWebViewConfiguration()
        config.preferences.setValue(true, forKey: "developerExtrasEnabled")
        let controller = WKUserContentController()
        controller.add(self, name: "nativeApp")
        controller.addUserScript(WKUserScript(source: """
            document.addEventListener('click', function(event) {
              const button = event.target.closest('#btnQuit');
              if (button) {
                event.preventDefault(); event.stopImmediatePropagation();
                window.webkit.messageHandlers.nativeApp.postMessage('quit');
              }
            }, true);
            """, injectionTime: .atDocumentEnd, forMainFrameOnly: true))
        config.userContentController = controller

        webView = DraggableWebView(frame: .zero, configuration: config)
        webView.navigationDelegate = self
        webView.setValue(false, forKey: "drawsBackground")
        webView.wantsLayer = true
        webView.layer?.backgroundColor = NSColor.clear.cgColor
        webView.translatesAutoresizingMaskIntoConstraints = false

        let root = NSView()
        root.wantsLayer = true
        root.layer?.backgroundColor = NSColor.clear.cgColor

        let backdrop: NSView
        if #available(macOS 26.0, *) {
            let glass = NSGlassEffectView()
            glass.style = .clear
            glass.cornerRadius = 0
            glass.tintColor = NSColor.windowBackgroundColor.withAlphaComponent(0.72)
            backdrop = glass
        } else {
            let material = NSVisualEffectView()
            material.material = .underWindowBackground
            material.blendingMode = .behindWindow
            material.state = .active
            backdrop = material
        }
        backdrop.translatesAutoresizingMaskIntoConstraints = false
        root.addSubview(backdrop)
        root.addSubview(webView)
        let dragHandle = WindowDragHandle()
        dragHandle.translatesAutoresizingMaskIntoConstraints = false
        root.addSubview(dragHandle)
        NSLayoutConstraint.activate([
            backdrop.leadingAnchor.constraint(equalTo: root.leadingAnchor),
            backdrop.trailingAnchor.constraint(equalTo: root.trailingAnchor),
            backdrop.topAnchor.constraint(equalTo: root.topAnchor),
            backdrop.bottomAnchor.constraint(equalTo: root.bottomAnchor),
            webView.leadingAnchor.constraint(equalTo: root.leadingAnchor),
            webView.trailingAnchor.constraint(equalTo: root.trailingAnchor),
            webView.topAnchor.constraint(equalTo: root.topAnchor),
            webView.bottomAnchor.constraint(equalTo: root.bottomAnchor),
            dragHandle.topAnchor.constraint(equalTo: root.topAnchor),
            dragHandle.heightAnchor.constraint(equalToConstant: 52),
            dragHandle.leadingAnchor.constraint(equalTo: root.leadingAnchor, constant: 220),
            dragHandle.trailingAnchor.constraint(equalTo: root.trailingAnchor, constant: -330)
        ])

        window = NSWindow(
            contentRect: NSRect(x: 0, y: 0, width: 1120, height: 760),
            styleMask: [.titled, .closable, .miniaturizable, .resizable, .fullSizeContentView],
            backing: .buffered,
            defer: false
        )
        window.delegate = self
        window.title = "文件搜索器"
        window.titleVisibility = .hidden
        window.titlebarAppearsTransparent = true
        window.isOpaque = false
        window.backgroundColor = .clear
        window.hasShadow = true
        window.isMovableByWindowBackground = false
        window.minSize = NSSize(width: 760, height: 520)
        window.contentView = root
        window.center()
        window.makeKeyAndOrderFront(nil)
        NSApp.activate(ignoringOtherApps: true)

        showLoadingPage()
    }

    private func showLoadingPage() {
        let html = """
        <!doctype html><meta name="color-scheme" content="light dark">
        <style>
        html,body{height:100%;margin:0;background:transparent;color:CanvasText;font-family:-apple-system,"SF Pro Text",sans-serif}
        body{display:grid;place-items:center}.loading{text-align:center;font-size:13px;opacity:.66}
        .ring{width:22px;height:22px;margin:0 auto 13px;border:2px solid color-mix(in srgb,CanvasText 15%,transparent);border-top-color:#0a84ff;border-radius:50%;animation:r .75s linear infinite}@keyframes r{to{transform:rotate(1turn)}}
        @media(prefers-reduced-motion:reduce){.ring{animation:none}}
        </style><div class="loading"><div class="ring"></div>正在连接高速索引…</div>
        """
        webView.loadHTMLString(html, baseURL: nil)
    }

    private func startBackend() {
        guard let resources = Bundle.main.resourceURL else { return }
        let script = resources.appendingPathComponent("lycapp.py")
        let bundledPython = resources.appendingPathComponent("python/bin/python3")
        let executable = FileManager.default.isExecutableFile(atPath: bundledPython.path)
            ? bundledPython : URL(fileURLWithPath: "/usr/bin/python3")
        let process = Process()
        process.executableURL = executable
        process.arguments = [script.path]
        process.currentDirectoryURL = resources
        var environment = ProcessInfo.processInfo.environment
        environment["LYCSEARCH_NO_BROWSER"] = "1"
        environment["PYTHONNOUSERSITE"] = "1"
        if executable == bundledPython {
            environment["PYTHONHOME"] = resources.appendingPathComponent("python").path
        }
        process.environment = environment
        let log = URL(fileURLWithPath: NSTemporaryDirectory()).appendingPathComponent("lyc-filesearch-native.log")
        FileManager.default.createFile(atPath: log.path, contents: nil)
        if let handle = try? FileHandle(forWritingTo: log) {
            process.standardOutput = handle
            process.standardError = handle
        }
        do {
            try process.run()
            backend = process
        } catch {
            showError("无法启动索引服务：\(error.localizedDescription)")
        }
    }

    private func pollForService() {
        pollTimer?.invalidate()
        attempts = 0
        pollTimer = Timer.scheduledTimer(withTimeInterval: 0.18, repeats: true) { [weak self] _ in
            self?.probePorts()
        }
        pollTimer?.fire()
    }

    private func probePorts() {
        attempts += 1
        if attempts > 110 {
            pollTimer?.invalidate()
            showError("索引服务启动超时，请重新打开 App。")
            return
        }
        for port in 8765...8784 {
            guard let url = URL(string: "http://127.0.0.1:\(port)/api/stats") else { continue }
            var request = URLRequest(url: url, timeoutInterval: 0.15)
            request.cachePolicy = .reloadIgnoringLocalCacheData
            URLSession.shared.dataTask(with: request) { [weak self] data, response, _ in
                guard let self, self.serviceURL == nil, let data,
                      let object = try? JSONSerialization.jsonObject(with: data) as? [String: Any] else { return }
                let version = (object["app_version"] as? String) ?? (object["version"] as? String)
                guard version == nil || version == appVersion else { return }
                DispatchQueue.main.async {
                    guard self.serviceURL == nil else { return }
                    self.serviceURL = URL(string: "http://127.0.0.1:\(port)/")!
                    self.pollTimer?.invalidate()
                    self.webView.load(URLRequest(url: self.serviceURL!))
                }
            }.resume()
        }
    }

    private func showError(_ message: String) {
        let safe = message.replacingOccurrences(of: "&", with: "&amp;").replacingOccurrences(of: "<", with: "&lt;")
        webView.loadHTMLString("<meta name='color-scheme' content='light dark'><body style='background:transparent;color:CanvasText;font:13px -apple-system;display:grid;place-items:center;height:100vh;margin:0'><div>\(safe)</div></body>", baseURL: nil)
    }

    func userContentController(_ userContentController: WKUserContentController, didReceive message: WKScriptMessage) {
        if (message.body as? String) == "quit" { NSApp.terminate(nil) }
        else if (message.body as? String) == "requestFolderAccess" { requestFolderAccess() }
    }

    /// 通过 AppKit NSOpenPanel 让用户选择要纳入索引的文件夹。
    /// 用户点“打开”后，TCC 将访问权授予本 App（responsible process），
    /// 其后端 Python 子进程即可扫描这些目录；随后自动触发一次刷新。
    private func requestFolderAccess() {
        let panel = NSOpenPanel()
        panel.canChooseFiles = false
        panel.canChooseDirectories = true
        panel.allowsMultipleSelection = true
        panel.prompt = "授权索引"
        panel.message = "选择要纳入索引的文件夹（桌面、文稿、下载、影片、音乐、图片等）。授权后会自动刷新并重建覆盖。"
        if panel.runModal() == .OK {
            // 触达所选目录即完成 TCC 授权登记；后续子进程扫描将继承该权限。
            for url in panel.urls {
                _ = FileManager.default.isReadableFile(atPath: url.path)
            }
            triggerBackendRefresh()
        }
    }

    private func triggerBackendRefresh() {
        guard let base = serviceURL else { return }
        var request = URLRequest(url: base.appendingPathComponent("api/refresh"))
        request.httpMethod = "POST"
        URLSession.shared.dataTask(with: request).resume()
    }
}

let application = NSApplication.shared
let delegate = AppDelegate()
application.setActivationPolicy(.regular)
application.delegate = delegate
application.run()
