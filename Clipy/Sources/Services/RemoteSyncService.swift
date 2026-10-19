//
//  RemoteSyncService.swift
//
//  Clipy
//  GitHub: https://github.com/clipy
//  HP: https://clipy-app.com
//
//  Local-network clipboard sync so a phone (e.g. Android) can read the
//  history and send text to this Mac through a browser.
//

import Cocoa
import Combine
import Dependencies
import Foundation
import Network
import Sharing

final class RemoteSyncService {

    // MARK: - Properties
    private var listener: NWListener?
    private var cancellables: Set<AnyCancellable> = []
    private let queue = DispatchQueue(label: "com.clipy-app.Clipy.RemoteSync")

    private let maxRequestSize = 2 * 1024 * 1024
    private let maxTextLength = 1_000_000
    private let historyLimit = 50

    @Dependency(\.pasteboardHistoryRepository)
    private var pasteboardHistoryRepository
    @Dependency(\.pasteService)
    private var pasteService
    @Dependency(\.mainQueue)
    private var mainQueue

    @Shared(.remoteSyncEnabled)
    private var isEnabled: Bool
    @Shared(.remoteSyncPort)
    private var port: Int
    @Shared(.remoteSyncToken)
    private var token: String

    // MARK: - Lifecycle
    func start() {
        if token.isEmpty {
            $token.withLock { $0 = Self.makeToken() }
        }

        $isEnabled.changes(includingInitialValue: true)
            .combineLatest($port.changes(includingInitialValue: true))
            .receive(on: mainQueue)
            .sink { [weak self] isEnabled, port in
                self?.restart(isEnabled: isEnabled, port: port)
            }
            .store(in: &cancellables)
    }

    func stop() {
        listener?.cancel()
        listener = nil
    }

    private func restart(isEnabled: Bool, port: Int) {
        stop()
        guard isEnabled, let nwPort = NWEndpoint.Port(rawValue: UInt16(clamping: port)) else { return }

        do {
            let listener = try NWListener(using: .tcp, on: nwPort)
            listener.newConnectionHandler = { [weak self] connection in
                self?.accept(connection)
            }
            listener.stateUpdateHandler = { [weak listener] state in
                if case .failed = state { listener?.cancel() }
            }
            listener.start(queue: queue)
            self.listener = listener
        } catch {
            reportIssue(error)
        }
    }

    // MARK: - Helpers
    static func makeToken() -> String {
        var bytes = [UInt8](repeating: 0, count: 16)
        _ = SecRandomCopyBytes(kSecRandomDefault, bytes.count, &bytes)
        return bytes.map { String(format: "%02x", $0) }.joined()
    }

    static func localIPv4Address() -> String? {
        var head: UnsafeMutablePointer<ifaddrs>?
        guard getifaddrs(&head) == 0, let first = head else { return nil }
        defer { freeifaddrs(head) }

        for pointer in sequence(first: first, next: { $0.pointee.ifa_next }) {
            let interface = pointer.pointee
            guard let address = interface.ifa_addr, address.pointee.sa_family == UInt8(AF_INET) else { continue }
            let name = String(cString: interface.ifa_name)
            guard name.hasPrefix("en") else { continue }
            var host = [CChar](repeating: 0, count: Int(NI_MAXHOST))
            getnameinfo(address, socklen_t(address.pointee.sa_len), &host, socklen_t(host.count), nil, 0, NI_NUMERICHOST)
            return String(cString: host)
        }
        return nil
    }

    static func pairingURL(port: Int, token: String) -> String? {
        guard let address = localIPv4Address() else { return nil }
        return "http://\(address):\(port)/#t=\(token)"
    }

    private func isAuthorized(_ request: HTTPRequest) -> Bool {
        let provided = Array((request.headers["x-clipy-token"] ?? "").utf8)
        let expected = Array(token.utf8)
        guard !expected.isEmpty, provided.count == expected.count else { return false }
        return zip(provided, expected).reduce(0) { $0 | ($1.0 ^ $1.1) } == 0
    }
}

// MARK: - HTTP
private extension RemoteSyncService {
    struct HTTPRequest {
        let method: String
        let path: String
        let headers: [String: String]
        let body: Data
    }

    struct HTTPResponse {
        var status: Int
        var contentType: String
        var body: Data

        static func json(_ object: Any, status: Int = 200) -> HTTPResponse {
            let data = (try? JSONSerialization.data(withJSONObject: object)) ?? Data("{}".utf8)
            return HTTPResponse(status: status, contentType: "application/json; charset=utf-8", body: data)
        }

        static func error(_ status: Int, _ message: String) -> HTTPResponse {
            .json(["error": message], status: status)
        }
    }

    func accept(_ connection: NWConnection) {
        connection.start(queue: queue)
        queue.asyncAfter(deadline: .now() + 15) { connection.cancel() }
        receive(on: connection, buffer: Data())
    }

    func receive(on connection: NWConnection, buffer: Data) {
        connection.receive(minimumIncompleteLength: 1, maximumLength: 65536) { [weak self] data, _, isComplete, error in
            guard let self else { return }
            var buffer = buffer
            if let data { buffer.append(data) }

            if buffer.count > maxRequestSize {
                send(.error(413, "payload too large"), on: connection)
                return
            }
            if let request = parse(buffer) {
                route(request) { [weak self] response in
                    self?.send(response, on: connection)
                }
            } else if error != nil || isComplete {
                connection.cancel()
            } else {
                receive(on: connection, buffer: buffer)
            }
        }
    }

    /// Returns nil until the whole request (headers + Content-Length body) has arrived.
    func parse(_ buffer: Data) -> HTTPRequest? {
        guard let separator = buffer.range(of: Data("\r\n\r\n".utf8)) else { return nil }
        let headerText = String(decoding: buffer[buffer.startIndex..<separator.lowerBound], as: UTF8.self)
        var lines = headerText.components(separatedBy: "\r\n")
        let requestLine = lines.removeFirst().split(separator: " ")
        guard requestLine.count >= 2 else { return nil }

        var headers: [String: String] = [:]
        for line in lines {
            guard let colon = line.firstIndex(of: ":") else { continue }
            let name = line[..<colon].lowercased()
            let value = line[line.index(after: colon)...].trimmingCharacters(in: .whitespaces)
            headers[name] = value
        }

        let contentLength = Int(headers["content-length"] ?? "0") ?? 0
        let body = buffer[separator.upperBound...]
        guard body.count >= contentLength else { return nil }

        return HTTPRequest(
            method: String(requestLine[0]),
            path: String(requestLine[1].split(separator: "?").first ?? "/"),
            headers: headers,
            body: Data(body.prefix(contentLength))
        )
    }

    func send(_ response: HTTPResponse, on connection: NWConnection) {
        let reason = [200: "OK", 204: "No Content", 400: "Bad Request", 401: "Unauthorized", 404: "Not Found", 413: "Payload Too Large"]
        var head = "HTTP/1.1 \(response.status) \(reason[response.status] ?? "OK")\r\n"
        head += "Content-Type: \(response.contentType)\r\n"
        head += "Content-Length: \(response.body.count)\r\n"
        head += "Cache-Control: no-store\r\n"
        head += "Connection: close\r\n\r\n"
        connection.send(content: Data(head.utf8) + response.body, completion: .contentProcessed { _ in
            connection.cancel()
        })
    }
}

// MARK: - Routing
private extension RemoteSyncService {
    func route(_ request: HTTPRequest, respond: @escaping (HTTPResponse) -> Void) {
        switch (request.method, request.path) {
        case ("GET", "/"):
            respond(HTTPResponse(status: 200, contentType: "text/html; charset=utf-8", body: Data(Self.page.utf8)))
        case ("GET", "/favicon.ico"):
            respond(HTTPResponse(status: 204, contentType: "text/plain", body: Data()))
        case (_, let path) where path.hasPrefix("/api/"):
            guard isAuthorized(request) else {
                respond(.error(401, "unauthorized"))
                return
            }
            // Pasteboard and database access happen on the main thread, like the rest of Clipy.
            DispatchQueue.main.async { [self] in
                respond(handleAPI(request))
            }
        default:
            respond(.error(404, "not found"))
        }
    }

    func handleAPI(_ request: HTTPRequest) -> HTTPResponse {
        switch (request.method, request.path) {
        case ("GET", "/api/history"):
            let items: [[String: Any]] = pasteboardHistoryRepository
                .fetchHistoryDetails(sortsByCreatedAt: false, includesThumbnailAsset: false, limit: historyLimit)
                .compactMap { detail in
                    guard
                        let text = pasteboardHistoryRepository.fetchContent(id: detail.history.id)?.stringValue,
                        !text.isEmpty
                    else { return nil }
                    return [
                        "id": detail.history.id.rawValue,
                        "text": String(text.prefix(maxTextLength)),
                        "updatedAt": detail.history.updateAt
                    ]
                }
            return .json(["device": Host.current().localizedName ?? "Mac", "items": items])

        case ("POST", "/api/copy"):
            guard
                let object = try? JSONSerialization.jsonObject(with: request.body) as? [String: Any],
                let id = object["id"] as? String,
                let text = pasteboardHistoryRepository.fetchContent(id: PasteboardHistory.ID(rawValue: id))?.stringValue
            else { return .error(400, "invalid id") }
            pasteService.copyToPasteboard(with: text)
            return .json(["ok": true])

        case ("POST", "/api/send"):
            guard
                let object = try? JSONSerialization.jsonObject(with: request.body) as? [String: Any],
                let text = object["text"] as? String,
                !text.isEmpty, text.count <= maxTextLength
            else { return .error(400, "invalid text") }
            pasteService.copyToPasteboard(with: text)
            return .json(["ok": true])

        default:
            return .error(404, "not found")
        }
    }
}

// MARK: - Web UI
private extension RemoteSyncService {
    static let page = #"""
    <!doctype html>
    <html lang="en"><head><meta charset="utf-8">
    <meta name="viewport" content="width=device-width,initial-scale=1">
    <meta name="color-scheme" content="light dark">
    <title>ClipyDroid</title>
    <style>
    :root{--bg:#f5f5f7;--card:#fff;--fg:#1d1d1f;--mute:#6e6e73;--acc:#2563EB;--line:#e5e5ea}
    @media(prefers-color-scheme:dark){:root{--bg:#000;--card:#1c1c1e;--fg:#f5f5f7;--mute:#98989d;--line:#2c2c2e}}
    *{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--fg);font:16px/1.4 system-ui,sans-serif}
    main{max-width:640px;margin:0 auto;padding:16px}h1{font-size:20px;margin:0 0 4px}
    .sub{color:var(--mute);font-size:13px;margin-bottom:16px}
    textarea{width:100%;min-height:96px;padding:12px;border-radius:12px;border:1px solid var(--line);background:var(--card);color:var(--fg);font:inherit;resize:vertical}
    button{border:0;border-radius:10px;background:var(--acc);color:#fff;font:inherit;font-weight:600;padding:10px 16px}
    .row{display:flex;gap:8px;margin:8px 0 20px}.row button.alt{background:var(--card);color:var(--fg);border:1px solid var(--line)}
    .item{background:var(--card);border-radius:12px;padding:12px;margin-bottom:8px;white-space:pre-wrap;word-break:break-word;max-height:7.5em;overflow:hidden}
    .item:active{opacity:.6}#msg{position:fixed;left:0;right:0;bottom:16px;text-align:center;color:var(--mute);font-size:14px}
    </style></head><body><main>
    <h1>ClipyDroid</h1><div class="sub" id="dev">Connecting…</div>
    <textarea id="t" placeholder="Type or paste text to send to your Mac"></textarea>
    <div class="row"><button id="send">Send to Mac</button><button class="alt" id="paste">Paste</button></div>
    <div id="list"></div><div id="msg"></div>
    <script>
    const $=id=>document.getElementById(id);
    let token=localStorage.getItem('clipyToken')||'';
    const m=location.hash.match(/t=([0-9a-f]+)/);
    if(m){token=m[1];localStorage.setItem('clipyToken',token);history.replaceState(null,'',location.pathname)}
    const note=t=>{$('msg').textContent=t;setTimeout(()=>{if($('msg').textContent===t)$('msg').textContent=''},2000)};
    async function api(path,body){
      const r=await fetch(path,{method:body?'POST':'GET',headers:{'X-Clipy-Token':token,'Content-Type':'application/json'},body:body?JSON.stringify(body):undefined});
      if(r.status===401)throw new Error('Not paired — scan the QR code in ClipyDroid settings again');
      if(!r.ok)throw new Error('Request failed');return r.json();
    }
    function copyLocal(text){
      if(navigator.clipboard&&window.isSecureContext)return navigator.clipboard.writeText(text);
      const a=document.createElement('textarea');a.value=text;a.style.position='fixed';a.style.opacity=0;
      document.body.appendChild(a);a.select();document.execCommand('copy');a.remove();return Promise.resolve();
    }
    let last='';
    async function refresh(){
      try{
        const d=await api('/api/history');
        $('dev').textContent='Connected to '+d.device;
        const sig=JSON.stringify(d.items.map(i=>i.id+i.updatedAt));
        if(sig===last)return;last=sig;
        const list=$('list');list.textContent='';
        for(const i of d.items){
          const e=document.createElement('div');e.className='item';e.textContent=i.text;
          e.onclick=()=>copyLocal(i.text).then(()=>note('Copied on this phone'));
          list.appendChild(e);
        }
      }catch(e){$('dev').textContent=e.message}
    }
    $('send').onclick=async()=>{
      const text=$('t').value;if(!text)return;
      try{await api('/api/send',{text});$('t').value='';note('Sent to Mac');refresh()}catch(e){note(e.message)}
    };
    $('paste').onclick=async()=>{
      try{$('t').value=await navigator.clipboard.readText()}catch(e){note('Long-press the box and paste')}
    };
    refresh();setInterval(refresh,2000);
    </script></main></body></html>
    """#
}

// MARK: - DependencyKey
extension DependencyValues {
    var remoteSyncService: RemoteSyncService {
        get { self[RemoteSyncServiceKey.self] }
        set { self[RemoteSyncServiceKey.self] = newValue }
    }

    private enum RemoteSyncServiceKey: DependencyKey {
        static let liveValue = RemoteSyncService()
    }
}
