import CoreImage.CIFilterBuiltins
import Settings
import Sharing
import SwiftUI

struct RemoteSyncSettingsView: View {
    @Shared(.remoteSyncEnabled)
    private var isEnabled: Bool
    @Shared(.remoteSyncPort)
    private var port: Int
    @Shared(.remoteSyncToken)
    private var token: String

    private var pairingURL: String? {
        RemoteSyncService.pairingURL(port: port, token: token)
    }

    var body: some View {
        VStack(alignment: .leading, spacing: 12) {
            Toggle("Allow phones on this Wi-Fi to connect", isOn: Binding($isEnabled))
            Text("Scan the QR code with your Android phone. It must be on the same network as this Mac. Traffic is not encrypted, so only enable this on networks you trust.")
                .foregroundStyle(.secondary)
                .fixedSize(horizontal: false, vertical: true)

            HStack {
                Text("Port")
                TextField("Port", value: Binding($port), format: .number.grouping(.never))
                    .frame(width: 70)
                Button("New pairing code") {
                    $token.withLock { $0 = RemoteSyncService.makeToken() }
                }
            }

            if isEnabled, let pairingURL {
                HStack(alignment: .top, spacing: 16) {
                    if let image = Self.qrImage(for: pairingURL) {
                        Image(nsImage: image)
                            .interpolation(.none)
                            .resizable()
                            .frame(width: 160, height: 160)
                            .accessibilityLabel(Text("Pairing QR code"))
                    }
                    Text(pairingURL)
                        .textSelection(.enabled)
                        .font(.system(.caption, design: .monospaced))
                        .frame(width: 280, alignment: .leading)
                }
            } else if isEnabled {
                Text("No local network address found.")
                    .foregroundStyle(.secondary)
            }
        }
        .padding(20)
        .frame(width: 520, alignment: .leading)
    }

    private static func qrImage(for string: String) -> NSImage? {
        let filter = CIFilter.qrCodeGenerator()
        filter.message = Data(string.utf8)
        guard let output = filter.outputImage?.transformed(by: CGAffineTransform(scaleX: 8, y: 8)) else { return nil }
        let representation = NSCIImageRep(ciImage: output)
        let image = NSImage(size: representation.size)
        image.addRepresentation(representation)
        return image
    }
}
