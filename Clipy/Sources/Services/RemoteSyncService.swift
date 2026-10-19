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

    private let maxRequestSize = 30 * 1024 * 1024
    /// Bodies larger than this are only accepted once the pairing token checks out.
    private let maxUnauthenticatedSize = 64 * 1024
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

    private func isAuthorized(_ headers: [String: String]) -> Bool {
        let provided = Array((headers["x-clipy-token"] ?? "").utf8)
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
        let query: [String: String]
        let headers: [String: String]
        let body: Data
    }

    struct RequestHead {
        let method: String
        let target: String
        let headers: [String: String]
        let bodyStart: Data.Index
    }

    /// Accumulates one request without copying the whole buffer on every chunk.
    final class RequestBuffer {
        var data = Data()
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
        // Long enough for a phone to upload a photo over Wi-Fi.
        queue.asyncAfter(deadline: .now() + 60) { connection.cancel() }
        receive(on: connection, buffer: RequestBuffer())
    }

    func receive(on connection: NWConnection, buffer: RequestBuffer) {
        connection.receive(minimumIncompleteLength: 1, maximumLength: 1 << 20) { [weak self] data, _, isComplete, error in
            guard let self else { return }
            if let data { buffer.data.append(data) }

            if buffer.data.count > maxRequestSize {
                send(.error(413, "payload too large"), on: connection)
                return
            }
            if buffer.data.count > maxUnauthenticatedSize,
               let head = parseHead(buffer.data), !isAuthorized(head.headers) {
                send(.error(401, "unauthorized"), on: connection)
                return
            }
            if let request = parse(buffer.data) {
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

    /// Returns nil until the request line and all headers have arrived.
    func parseHead(_ buffer: Data) -> RequestHead? {
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
        return RequestHead(
            method: String(requestLine[0]),
            target: String(requestLine[1]),
            headers: headers,
            bodyStart: separator.upperBound
        )
    }

    /// Returns nil until the whole request (headers + Content-Length body) has arrived.
    func parse(_ buffer: Data) -> HTTPRequest? {
        guard let head = parseHead(buffer) else { return nil }
        let contentLength = Int(head.headers["content-length"] ?? "0") ?? 0
        let body = buffer[head.bodyStart...]
        guard body.count >= contentLength else { return nil }

        let components = URLComponents(string: head.target)
        let query = (components?.queryItems ?? []).reduce(into: [String: String]()) { $0[$1.name] = $1.value }
        return HTTPRequest(
            method: head.method,
            path: components?.path ?? "/",
            query: query,
            headers: head.headers,
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
            guard isAuthorized(request.headers) else {
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
                .compactMap { historyItem(for: $0.history) }
            return .json(["device": Host.current().localizedName ?? "Mac", "items": items])

        case ("GET", "/api/image"):
            guard
                let id = request.query["id"],
                let data = pasteboardHistoryRepository.fetchContent(id: PasteboardHistory.ID(rawValue: id))?.imageData,
                let image = Self.transferableImage(data)
            else { return .error(404, "no image") }
            return HTTPResponse(status: 200, contentType: image.contentType, body: image.data)

        case ("POST", "/api/send-image"):
            guard let representation = Self.bitmap(from: request.body),
                  let png = representation.representation(using: .png, properties: [:])
            else { return .error(400, "invalid image") }
            let pasteboard = NSPasteboard.general
            pasteboard.clearContents()
            pasteboard.setData(png, forType: .png)
            if let tiff = representation.tiffRepresentation {
                pasteboard.setData(tiff, forType: .tiff)
            }
            return .json(["ok": true])

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

// MARK: - Images
private extension RemoteSyncService {
    static let imageTypes: Set<NSPasteboard.PasteboardType> = [.png, .tiff, .deprecatedTIFF]

    /// Classifies a history entry from its stored types so polling never loads image blobs needlessly.
    func historyItem(for history: PasteboardHistory) -> [String: Any]? {
        var item: [String: Any] = ["id": history.id.rawValue, "updatedAt": history.updateAt]
        let types = history.pasteboardTypes
        let hasImage = types.contains { Self.imageTypes.contains($0) }
        let primaryIsImage = types.first.map { Self.imageTypes.contains($0) } ?? false

        if !primaryIsImage,
           types.contains(.string) || types.contains(.deprecatedString),
           let text = pasteboardHistoryRepository.fetchContent(id: history.id)?.stringValue,
           !text.isEmpty {
            item["kind"] = "text"
            item["text"] = String(text.prefix(maxTextLength))
        } else if hasImage {
            item["kind"] = "image"
        } else {
            return nil
        }
        return item
    }

    /// PNG and JPEG go out as-is; anything else (TIFF, HEIC, …) is converted to PNG for the phone.
    static func transferableImage(_ data: Data) -> (data: Data, contentType: String)? {
        if data.starts(with: [0x89, 0x50, 0x4E, 0x47]) { return (data, "image/png") }
        if data.starts(with: [0xFF, 0xD8, 0xFF]) { return (data, "image/jpeg") }
        guard let png = bitmap(from: data)?.representation(using: .png, properties: [:]) else { return nil }
        return (png, "image/png")
    }

    static func bitmap(from data: Data) -> NSBitmapImageRep? {
        guard !data.isEmpty else { return nil }
        if let representation = NSBitmapImageRep(data: data) { return representation }
        return NSImage(data: data)?.tiffRepresentation.flatMap(NSBitmapImageRep.init(data:))
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
    .item:active{opacity:.6}.item.pic{max-height:none;padding:8px}.item.pic img{display:block;max-width:100%;max-height:260px;border-radius:8px;margin:0 auto}
    .hint{color:var(--mute);font-size:12px;text-align:center;margin-top:4px}#msg{position:fixed;left:0;right:0;bottom:16px;text-align:center;color:var(--mute);font-size:14px}
    </style></head><body><main>
    <h1>ClipyDroid</h1><div class="sub" id="dev">Connecting…</div>
    <textarea id="t" placeholder="Type or paste text to send to your Mac"></textarea>
    <div class="row"><button id="send">Send to Mac</button><button class="alt" id="paste">Paste</button><button class="alt" id="pick">Send image</button>
    <input type="file" id="file" accept="image/*" hidden></div>
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
    const images=new Map();
    function imageURL(id){
      if(!images.has(id))images.set(id,fetch('/api/image?id='+encodeURIComponent(id),{headers:{'X-Clipy-Token':token}})
        .then(r=>{if(!r.ok)throw new Error();return r.blob()}).then(b=>URL.createObjectURL(b)));
      return images.get(id);
    }
    async function refresh(){
      try{
        const d=await api('/api/history');
        $('dev').textContent='Connected to '+d.device;
        const sig=JSON.stringify(d.items.map(i=>i.id+i.updatedAt));
        if(sig===last)return;last=sig;
        const list=$('list');list.textContent='';
        for(const i of d.items){
          const e=document.createElement('div');
          if(i.kind==='image'){
            e.className='item pic';
            const img=document.createElement('img');img.alt='Image from Mac';
            imageURL(i.id).then(u=>img.src=u).catch(()=>{e.textContent='Image unavailable'});
            const h=document.createElement('div');h.className='hint';h.textContent='Long-press to save or copy';
            e.append(img,h);
          }else{
            e.className='item';e.textContent=i.text;
            e.onclick=()=>copyLocal(i.text).then(()=>note('Copied on this phone'));
          }
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
    $('pick').onclick=()=>$('file').click();
    $('file').onchange=async()=>{
      const f=$('file').files[0];if(!f)return;note('Sending image…');
      try{
        const r=await fetch('/api/send-image',{method:'POST',headers:{'X-Clipy-Token':token,'Content-Type':f.type||'application/octet-stream'},body:f});
        if(!r.ok)throw new Error('Upload failed');note('Image sent to Mac');refresh();
      }catch(e){note(e.message)}
      $('file').value='';
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
